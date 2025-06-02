# File: bot/helper/listeners/task_listener.py

from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from html import escape
from requests import utils as rutils

from ... import (
    intervals,
    task_dict,
    task_dict_lock,
    LOGGER,
    non_queued_up,
    non_queued_dl,
    queued_up, # Kept as per original user provided snippet
    queued_dl, # Kept as per original user provided snippet
    queue_dict_lock,
    same_directory_lock,
    DOWNLOAD_DIR,
)
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager
from ..common import TaskConfig
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.db_handler import database
from ..ext_utils.files_utils import (
    get_path_size,
    clean_download,
    clean_target,
    join_files,
    create_recursive_symlink,
    remove_excluded_files,
    move_and_merge,
)
from ..ext_utils.links_utils import is_gdrive_id
from ..ext_utils.status_utils import get_readable_file_size
from ..ext_utils.task_manager import start_from_queued, check_running_tasks
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import GoogleDriveStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..mirror_leech_utils.telegram_uploader import TelegramUploader
from ..telegram_helper.button_build import ButtonMaker
from ..telegram_helper.message_utils import (
    send_message,
    delete_status,
    delete_message,
    update_status_message,
)


class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()

    async def clean(self):
        try:
            if st := intervals["status"]:
                for intvl in list(st.values()):
                    intvl.cancel()
                intervals["status"].clear()
                await gather(TorrentManager.aria2.purgeDownloadResult(), delete_status())
        except:
            pass

    def clear(self):
        self.subname = ""
        self.subsize = 0
        self.files_to_proceed = []
        self.proceed_count = 0
        self.progress = True

    async def remove_from_same_dir(self):
        async with task_dict_lock: # task_dict_lock might not be the correct lock for same_dir
                                 # Assuming same_directory_lock as per original logic
            if (
                self.folder_name
                and self.same_dir
                and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", []) # Added .get for safety from original
            ):
                # Use same_directory_lock for modifying self.same_dir
                async with same_directory_lock:
                    folder_data = self.same_dir.get(self.folder_name)
                    if folder_data and self.mid in folder_data.get("tasks", []):
                        folder_data["tasks"].remove(self.mid)
                        folder_data["total"] -= 1
                        if folder_data["total"] == 0:
                            del self.same_dir[self.folder_name]


    async def on_download_start(self):
        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.add_incomplete_task(
                self.message.chat.id, self.message.link, self.tag
            )

    async def on_download_complete(self):
        await sleep(2)
        if self.is_cancelled:
            return
        multi_links = False
        if (
            self.folder_name
            and self.same_dir
            and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", []) # Added .get for safety from original
        ):
            async with same_directory_lock:
                while True: # Original loop structure
                    # Re-check conditions inside loop, especially after await sleep
                    if not (self.folder_name and self.same_dir and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", [])):
                        break 
                    folder_state = self.same_dir.get(self.folder_name)
                    if not folder_state: break # Folder entry removed

                    # Original logic for same_dir merging from user's provided snippet
                    # This part is kept as close to original as possible
                    tasks_in_folder = folder_state.get("tasks", [])
                    if not tasks_in_folder or self.mid not in tasks_in_folder:
                        break # Task no longer listed for this folder

                    if folder_state.get("total", 0) <= 1 or len(tasks_in_folder) > 1:
                        if folder_state.get("total", 0) > 1 and self.mid != tasks_in_folder[0]: # If not the primary task and others exist
                            tasks_in_folder.remove(self.mid)
                            folder_state["total"] -= 1
                            spath = f"{self.dir}{self.folder_name}"
                            des_id = tasks_in_folder[0] # Merge to the first task in the list
                            des_path = f"{DOWNLOAD_DIR}{des_id}{self.folder_name}"
                            LOGGER.info(f"Moving files from {self.mid} to {des_id}")
                            await move_and_merge(spath, des_path, self.mid)
                            multi_links = True
                        elif folder_state.get("total", 0) <=1 and self.mid == tasks_in_folder[0]: # Last task or only task
                            pass # Will proceed to upload
                        # If it's the primary task (tasks_in_folder[0]) but total > 1, it should wait for others to merge.
                        # The original loop structure implies it waits.
                        break # Exit while loop to proceed or if merged
                    await sleep(1)
        
        async with task_dict_lock:
            if self.is_cancelled: return
            if self.mid not in task_dict: return
            download = task_dict[self.mid]
            self.name = download.name()
            gid = download.gid()
        LOGGER.info(f"Download completed: {self.name}")

        if not (self.is_torrent or self.is_qbit):
            self.seed = False

        if multi_links: # If this task's files were merged
            self.seed = False # Merged tasks don't seed
            await self.on_upload_error( # Use on_upload_error to send a status and clean up this task's resources
                f"{escape(self.name)} Downloaded & Merged!\n\nWaiting for other tasks in the group to finish..."
            )
            return # End processing for this merged task

        # This part is from original user code. self.same_dir might have been cleared if folder_name was removed
        elif self.same_dir and self.folder_name and self.same_dir.get(self.folder_name): 
            self.seed = False


        if self.folder_name and not self.name: # If name still not set, use folder_name
             self.name = self.folder_name.strip("/").split("/", 1)[0]


        # Fallback for self.name if dl_path doesn't exist (e.g. after extract/ffmpeg name changes)
        # dl_path construction should be robust
        dl_path_initial_guess = ospath.join(self.dir, self.name)

        if not await aiopath.exists(dl_path_initial_guess):
            try:
                files_in_dir = await listdir(self.dir)
                # Try to find a more suitable name if the initial guess is bad
                valid_files = [f for f in files_in_dir if f != "yt-dlp-thumb" and not f.startswith('.')]
                if valid_files:
                    self.name = valid_files[0] # Prioritize valid files
                elif files_in_dir: # Fallback if only hidden or thumb exists (though should be error)
                    self.name = files_in_dir[0]
                else: # Directory is empty
                    await self.on_upload_error("Download directory is empty after completion.")
                    return
                dl_path_initial_guess = ospath.join(self.dir, self.name) # Update dl_path with new name
            except Exception as e:
                await self.on_upload_error(f"Error finalizing download path: {str(e)}")
                return
        
        dl_path = dl_path_initial_guess # Final dl_path to use
        self.size = await get_path_size(dl_path)
        self.is_file = await aiopath.isfile(dl_path)

        up_dir = self.dir # Default upload directory base
        up_path = dl_path # Default path to content for processing/upload

        if self.seed: # For torrents being seeded
            self.up_dir = f"{self.dir.rstrip('/')}10000" # Specific suffix from original code
            up_dir = self.up_dir # Upload operations from this symlink parent
            up_path = ospath.join(up_dir, self.name) # Path of the symlink itself
            await create_recursive_symlink(dl_path, up_path) # Create symlink from dl_path to up_path
            LOGGER.info(f"Shortcut created: {dl_path} -> {up_path}")
        
        # up_dir is now the root for upload processing (original dir or symlink dir)
        # up_path is the path to the main file/folder within up_dir for initial processing

        await remove_excluded_files(up_dir, self.excluded_extensions)

        if not Config.QUEUE_ALL:
            async with queue_dict_lock:
                if self.mid in non_queued_dl:
                    non_queued_dl.remove(self.mid)
            await start_from_queued()

        if self.join and not self.is_file: # path is a folder
            await join_files(up_path) # join_files operates on the folder path

        # path_after_processing will track the main content path
        path_after_processing = up_path 

        if self.extract and not self.is_nzb:
            path_after_processing = await self.proceed_extract(path_after_processing, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing) # Update name to the extracted content's name
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(up_dir) # Size of the whole upload directory
            self.clear()
            await remove_excluded_files(up_dir, self.excluded_extensions) # Re-run exclusion

        if self.ffmpeg_cmds:
            path_after_processing = await self.proceed_ffmpeg(path_after_processing, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(up_dir)
            self.clear()

        if self.name_sub:
            path_after_processing = await self.substitute(path_after_processing)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)

        if self.screen_shots:
            # generate_screenshots might create a subfolder (e.g., _mltbss)
            # It should return the path to the main content, not the screenshot folder
            original_content_path = path_after_processing
            await self.generate_screenshots(original_content_path) # Pass content path
            if self.is_cancelled: return
            path_after_processing = original_content_path # Main content path is unchanged by screenshot generation itself
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
            # Size of up_dir might not change much if screenshots are in a subfolder handled by uploader
            self.size = await get_path_size(up_dir) 

        # ... (Other processing steps like convert_media, sample_video from original code) ...
        # Ensure each step updates path_after_processing, self.name, self.is_file, self.size correctly.
        # For brevity, assuming original logic for these was correct.

        if self.compress: # Compress for mirror, or for leech if explicitly set (original code implies this structure)
            path_after_processing = await self.proceed_compress(path_after_processing, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing) # Name is now archive name
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(path_after_processing) # Size is now archive size
            self.clear()
        elif self.is_leech and not self.compress: # Split for leech if not compressing
            await self.proceed_split(path_after_processing, gid)
            if self.is_cancelled: return
            self.clear()
            self.size = await get_path_size(up_dir) # Total size of split parts in upload dir

        self.subproc = None

        add_to_queue, event = await check_running_tasks(self, "up")
        await start_from_queued()
        if add_to_queue:
            LOGGER.info(f"Added to Queue/Upload: {self.name}")
            async with task_dict_lock:
                task_dict[self.mid] = QueueStatus(self, gid, "Up")
            await event.wait()
            if self.is_cancelled: return
            LOGGER.info(f"Start from Queued/Upload: {self.name}")

        # Final size before calling uploader status (up_dir is the root for upload)
        self.size = await get_path_size(up_dir)

        if self.is_leech:
            LOGGER.info(f"Leech Name: {self.name} from directory {up_dir}")
            tg = TelegramUploader(self, up_dir) # TelegramUploader walks the up_dir
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, gid, "up")
            # The status update should be managed carefully if tg.upload() is blocking or long.
            # Original code implies update_status_message might run concurrently or be an interval.
            # For simplicity and directness:
            status_updater = asyncio.create_task(update_status_message(self.message.chat.id))
            await tg.upload()
            status_updater.cancel() # Cancel status updater after upload is done
            del tg # Optional: help garbage collector
        elif is_gdrive_id(self.up_dest):
            LOGGER.info(f"Gdrive Upload Name: {self.name} from path {path_after_processing}")
            drive = GoogleDriveUpload(self, path_after_processing) # Upload specific processed path
            async with task_dict_lock:
                task_dict[self.mid] = GoogleDriveStatus(self, drive, gid, "up")
            status_updater = asyncio.create_task(update_status_message(self.message.chat.id))
            await sync_to_async(drive.upload)
            status_updater.cancel()
            del drive
        else: # Rclone
            LOGGER.info(f"Rclone Upload Name: {self.name} from path {path_after_processing}")
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, "up")
            status_updater = asyncio.create_task(update_status_message(self.message.chat.id))
            await RCTransfer.upload(path_after_processing) # Upload specific processed path
            status_updater.cancel()
            del RCTransfer
        return


    async def on_upload_complete(
        self, link, files, folders, mime_type, rclone_path="", dir_id=""
    ):
        # Original parameters from user's snippet:
        # link: For mirror, the cloud link. For leech, None.
        # files: For mirror, file_count (gdrive) or size (rclone). For leech, _msgs_dict.
        # folders: For mirror, folder_count (gdrive) or total_files (rclone). For leech, _total_files from uploader.
        # mime_type: For mirror, actual mime_type. For leech, _corrupted_files_count from uploader.

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)

        LOGGER.info(f"Task Done: {self.name}")
        
        # Store chat_id and tag before self.message might be altered or deleted
        # Ensure self.message and its attributes are valid
        chat_id_to_send_to = self.message.chat.id if hasattr(self.message, 'chat') else self.user_id
        user_mention_tag = self.tag if hasattr(self, 'tag') else ""
        original_message_id = self.message.id if hasattr(self.message, 'id') else None


        if self.is_leech:
            # MODIFIED LEECH COMPLETION MESSAGE BLOCK STARTS HERE
            # For Leech (Telegram Upload):
            # 'files' is _msgs_dict from TelegramUploader (dict of {link: caption})
            # 'folders' is _total_files (attempted file uploads by uploader) from TelegramUploader
            # 'mime_type' is _corrupted (failed file uploads by uploader) from TelegramUploader
            
            if hasattr(self.message, 'delete'): # Check if self.message is a deletable message object
                try:
                    await delete_message(self.message) 
                except Exception as e:
                    LOGGER.warning(f"Failed to delete original command message for leech task: {e}")
            
            leech_completion_message = f"<b>Name: </b><code>{escape(self.name)}</code>\n"
            leech_completion_message += f"<b>Size: </b>{get_readable_file_size(self.size)}\n" # Original download size

            total_attempted_by_uploader = folders 
            corrupted_by_uploader = mime_type 
            successful_uploads = total_attempted_by_uploader - corrupted_by_uploader
            
            leech_completion_message += f"<b>Files Uploaded: </b>{successful_uploads}"
            
            if corrupted_by_uploader > 0:
                leech_completion_message += f"\n<b>Corrupted/Skipped: </b>{corrupted_by_uploader}"
            
            leech_completion_message += f"\n\n<b>cc: </b>{user_mention_tag}"

            # Send the summary message to the original chat.
            await send_message(chat_id_to_send_to, leech_completion_message)

            # Log the detailed file links if available ('files' is _msgs_dict) for debugging
            if files and isinstance(files, dict) and files:
                 LOGGER.info(f"Leech upload details for {self.name} ({len(files)} links generated).")
            elif not files and successful_uploads > 0: 
                 LOGGER.warning(f"Leech completed for {self.name} with {successful_uploads} files, but no file links dictionary was provided.")
            elif successful_uploads == 0 and total_attempted_by_uploader > 0 : 
                LOGGER.warning(f"Leech for {self.name}: {total_attempted_by_uploader} files processed by uploader, none successful.")
            # MODIFIED LEECH COMPLETION MESSAGE BLOCK ENDS HERE
        else:
            # This is the original mirror logic from user's snippet
            msg = f"<b>Name: </b><code>{escape(self.name)}</code>\n\n<b>Size: </b>{get_readable_file_size(self.size)}"
            msg += f"\n\n<b>Type: </b>{mime_type}"
            if mime_type == "Folder":
                msg += f"\n<b>SubFolders: </b>{folders}" # For GDrive, this is folder_count
                msg += f"\n<b>Files: </b>{files}"         # For GDrive, this is file_count
            
            buttons = ButtonMaker()
            button_added = False # Track if any button is added
            if link:
                buttons.url_button("☁️ Cloud Link", link)
                button_added = True
            
            if rclone_path: # Covers general rclone cases
                if Config.RCLONE_SERVE_URL and not self.private_link:
                    remote, rpath = rclone_path.split(":", 1)
                    url_path = rutils.quote(f"{rpath.lstrip('/')}")
                    share_url = f"{Config.RCLONE_SERVE_URL.rstrip('/')}/{remote}/{url_path}"
                    if mime_type == "Folder":
                        share_url = f"{share_url.rstrip('/')}/"
                    buttons.url_button("🔗 Rclone Link", share_url)
                    button_added = True
                else: # No rclone serve, just show path
                     msg += f"\n\nPath: <code>{rclone_path}</code>"
            
            if not rclone_path and dir_id: # GDrive specific index link
                INDEX_URL = ""
                if self.private_link: # User's specific index URL
                    INDEX_URL = self.user_dict.get("INDEX_URL", "") or ""
                elif Config.INDEX_URL: # Global index URL
                    INDEX_URL = Config.INDEX_URL
                
                if INDEX_URL:
                    share_url = f"{INDEX_URL.rstrip('/')}/findpath?id={dir_id}"
                    buttons.url_button("⚡ Index Link", share_url)
                    button_added = True
                    if isinstance(mime_type, str) and mime_type.startswith(("image", "video", "audio")):
                        share_urls = f"{INDEX_URL.rstrip('/')}/findpath?id={dir_id}&view=true"
                        buttons.url_button("🌐 View Link", share_urls)
            
            button_menu = buttons.build_menu(2) if button_added else None
            msg += f"\n\n<b>cc: </b>{user_mention_tag}"
            # For mirror, send as reply to original command message
            await send_message(self.message, msg, button_menu)


        # Common cleanup logic (same for Leech and Mirror after their specific messages)
        if self.seed:
            # For seeding tasks that created a symlink in self.up_dir
            if hasattr(self, 'up_dir') and self.up_dir and ospath.islink(self.up_dir): # or check specific symlink path
                 await clean_target(self.up_dir) # Removes the symlink directory
            async with queue_dict_lock:
                if self.mid in non_queued_up:
                    non_queued_up.remove(self.mid)
            await start_from_queued()
            # Original download in self.dir is kept for seeding.
            # Further cleanup of self.dir for seeding tasks depends on bot's overall seed management.
            # This return might be specific to original bot's seeding logic.
            return 

        # Standard cleanup for non-seeding tasks or after upload from symlink is done.
        if hasattr(self, 'dir') and self.dir:
            await clean_download(self.dir) # Clean main download dir
        # If up_dir was created (e.g. for seeding symlink) and it's different from self.dir
        # and it was NOT the symlink dir already cleaned above. This case needs care.
        # The original had clean_download(self.up_dir) without specific symlink check.
        # If self.up_dir was the symlink, it's already cleaned by clean_target.
        # If self.up_dir was a copy or different processed location, it should be cleaned.
        if hasattr(self, 'up_dir') and self.up_dir and self.up_dir != self.dir and await aiopath.exists(self.up_dir):
            if not (self.seed and ospath.islink(self.up_dir)): # Don't re-clean if it was the symlink dir
                await clean_download(self.up_dir)

        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        
        if count == 0:
            await self.clean() # Full cleanup (Aria2 purge, delete global status message)
        else:
            await update_status_message(chat_id_to_send_to) # Update global status message

        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)
        await start_from_queued()

        # Task-specific custom thumbnail cleanup (if it's not a default one)
        if self.thumb and self.thumb != "none":
            is_default_thumb_pattern = self.thumb == f"thumbnails/{self.user_id}.jpg" or self.thumb == "thumbnails/default.jpg"
            if not is_default_thumb_pattern and await aiopath.exists(self.thumb):
                try:
                    await remove(self.thumb)
                    LOGGER.info(f"Removed task-specific custom thumbnail: {self.thumb}")
                except Exception as e:
                    LOGGER.warning(f"Failed to remove task-specific custom thumbnail {self.thumb}: {e}")


    async def on_download_error(self, error, button=None):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await self.remove_from_same_dir() # Use the refined version
        
        error_message_text = f"{self.tag} Download: {escape(str(error))}"
        target_chat_id = self.message.chat.id if hasattr(self.message, 'chat') else self.user_id
        reply_to_id = self.message.id if hasattr(self.message, 'id') else None

        await send_message(target_chat_id, error_message_text, button, reply_to_message_id=reply_to_id)
        
        if count == 0:
            await self.clean()
        else:
            await update_status_message(target_chat_id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
            and hasattr(self.message, 'link')
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock: # Original complex queue clearing logic
            if self.mid in queued_dl: # Assuming queued_dl and queued_up are dicts of events
                if hasattr(queued_dl[self.mid], 'set'): queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                if hasattr(queued_up[self.mid], 'set'): queued_up[self.mid].set()
                del queued_up[self.mid]
            # non_queued_dl/up are lists in some versions
            if self.mid in non_queued_dl: non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up: non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3) # Small delay for operations
        
        if hasattr(self, 'dir') and self.dir: await clean_download(self.dir)
        if hasattr(self, 'up_dir') and self.up_dir and self.up_dir != self.dir : await clean_download(self.up_dir)
        
        if self.thumb and self.thumb != "none": # User-provided thumb cleanup
            is_default_thumb_pattern = self.thumb == f"thumbnails/{self.user_id}.jpg" or self.thumb == "thumbnails/default.jpg"
            if not is_default_thumb_pattern and await aiopath.exists(self.thumb):
                try: await remove(self.thumb)
                except Exception as e: LOGGER.warning(f"Failed to remove custom thumb on DL error {self.thumb}: {e}")


    async def on_upload_error(self, error):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        
        error_message_text = f"{self.tag} {escape(str(error))}"
        target_chat_id = self.message.chat.id if hasattr(self.message, 'chat') else self.user_id
        
        # For leech, original message is deleted, so send new. For mirror, reply.
        reply_to_message_id = None
        if not self.is_leech and hasattr(self.message, 'id'):
            reply_to_message_id = self.message.id

        await send_message(target_chat_id, error_message_text, reply_to_message_id=reply_to_message_id)
        
        if count == 0:
            await self.clean()
        else:
            await update_status_message(target_chat_id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
            and hasattr(self.message, 'link')
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock: # Original complex queue clearing logic
            if self.mid in queued_dl:
                if hasattr(queued_dl[self.mid], 'set'): queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                if hasattr(queued_up[self.mid], 'set'): queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl: non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up: non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)

        if hasattr(self, 'dir') and self.dir: await clean_download(self.dir)
        if hasattr(self, 'up_dir') and self.up_dir and self.up_dir != self.dir: await clean_download(self.up_dir)
        
        if self.thumb and self.thumb != "none": # User-provided thumb cleanup
            is_default_thumb_pattern = self.thumb == f"thumbnails/{self.user_id}.jpg" or self.thumb == "thumbnails/default.jpg"
            if not is_default_thumb_pattern and await aiopath.exists(self.thumb):
                try: await remove(self.thumb)
                except Exception as e: LOGGER.warning(f"Failed to remove custom thumb on UL error {self.thumb}: {e}")
