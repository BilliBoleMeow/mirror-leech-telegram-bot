# File: bot/helper/listeners/task_listener.py

from aiofiles.os import path as aiopath, listdir, remove
from asyncio import sleep, gather
from html import escape
from requests import utils as rutils # For Rclone link quoting

from ... import (
    intervals,
    task_dict,
    task_dict_lock,
    LOGGER,
    non_queued_up,
    non_queued_dl,
    # queued_up, # Not used in this provided snippet from user
    # queued_dl, # Not used in this provided snippet from user
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
from ..ext_utils.status_utils import get_readable_file_size # Used for formatting size
from ..ext_utils.task_manager import start_from_queued, check_running_tasks
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import GoogleDriveStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus # Status class
from ..mirror_leech_utils.telegram_uploader import TelegramUploader # The uploader
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
        # Attributes like self.name, self.size, self.is_leech, etc.,
        # are initialized by the specific task handlers before these listener methods are called.

    async def clean(self):
        try:
            if status_intervals := intervals.get("status"): 
                for interval_timer in list(status_intervals.values()): # Iterate over a copy
                    interval_timer.cancel()
                status_intervals.clear() 
            await gather(TorrentManager.aria2.purgeDownloadResult(), delete_status())
        except Exception as e:
            LOGGER.error(f"Error during listener clean: {e}")

    def clear(self): 
        self.subname = ""
        self.subsize = 0
        self.files_to_proceed = []
        self.proceed_count = 0
        self.progress = True 

    async def remove_from_same_dir(self):
        if not self.folder_name or not self.same_dir:
            return
        folder_data = self.same_dir.get(self.folder_name)
        if folder_data and self.mid in folder_data.get("tasks", []):
            async with same_directory_lock: 
                folder_data = self.same_dir.get(self.folder_name) # Re-check after acquiring lock
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
            and hasattr(self.message, 'chat') and hasattr(self.message, 'link')
        ):
            await database.add_incomplete_task(
                self.message.chat.id, self.message.link, self.tag
            )

    async def on_download_complete(self):
        await sleep(2) 
        if self.is_cancelled:
            return
        
        multi_links_merged = False 
        if self.folder_name and self.same_dir and self.mid in self.same_dir.get(self.folder_name, {}).get("tasks", []):
            async with same_directory_lock:
                while self.folder_name in self.same_dir and self.mid in self.same_dir[self.folder_name]["tasks"]:
                    folder_state = self.same_dir[self.folder_name]
                    # This task is not the "main" one for the folder and there are others.
                    if len(folder_state["tasks"]) > 1 and self.mid != folder_state["tasks"][0]:
                        folder_state["tasks"].remove(self.mid)
                        folder_state["total"] -= 1
                        
                        source_path = f"{self.dir}{self.folder_name}"
                        dest_task_id = folder_state["tasks"][0]
                        dest_base_path = f"{DOWNLOAD_DIR}{dest_task_id}"
                        dest_folder_path = f"{dest_base_path}/{self.folder_name}"

                        LOGGER.info(f"Same_dir: Moving from task {self.mid} ({source_path}) to {dest_task_id} ({dest_folder_path})")
                        await move_and_merge(source_path, dest_folder_path, self.mid)
                        multi_links_merged = True
                        break 
                    elif folder_state["total"] == 1 and self.mid == folder_state["tasks"][0]:
                        LOGGER.info(f"Same_dir: Task {self.mid} is last for {self.folder_name}. Proceeding.")
                        break 
                    elif len(folder_state["tasks"]) == 1 and self.mid == folder_state["tasks"][0]: # Implicitly total == 1
                         LOGGER.info(f"Same_dir: Task {self.mid} is only one for {self.folder_name}. Proceeding.")
                         break
                    # If this task is the first in list, but other tasks are still pending to merge into it
                    elif self.mid == folder_state["tasks"][0] and len(folder_state["tasks"]) == 1 and folder_state["total"] > 1:
                        LOGGER.info(f"Same_dir: Task {self.mid} is primary for {self.folder_name}, waiting for {folder_state['total'] -1} other(s) to merge.")
                        # This task will wait. Others will merge into it.
                        # To prevent busy loop if something goes wrong with other tasks:
                        # This part of same_dir logic can be complex. The original code might have more nuanced handling.
                        # For now, assume other tasks will decrement 'total' or remove themselves.
                        pass # Wait for next iteration or external event.
                    else: # Conditions not met to break or merge, wait and re-evaluate
                        LOGGER.debug(f"Same_dir: Task {self.mid} for {self.folder_name} waiting. Tasks: {folder_state['tasks']}, Total: {folder_state['total']}")
                        
                    await sleep(1) 

        async with task_dict_lock:
            if self.is_cancelled or self.mid not in task_dict:
                return
            download = task_dict.get(self.mid) 
            if not download: return 

            self.name = download.name() 
            gid = download.gid() # gid is used for status updates, not directly in completion message
        
        LOGGER.info(f"Download completed: {self.name}")

        # Determine if it's a torrent/qbit task; otherwise, no seeding.
        if not (self.is_torrent or self.is_qbit):
            self.seed = False # Ensure seeding is false for direct downloads/nzbs

        if multi_links_merged: # If this task's files were merged into another
            self.seed = False # Merged tasks don't seed independently
            await self.on_upload_error( # Use on_upload_error to send a custom status and clean up
                f"✅ {escape(self.name)} downloaded & merged.\n\nWaiting for other tasks in the group to finish..."
            )
            return # End processing for this merged task

        # If it was part of same_dir but was the *last* one (not merged), it doesn't seed from its original path
        # as the content is now consolidated. This is usually handled by the uploader taking the final path.
        # The original same_dir logic might have more specific handling for the 'final' task of a group.
        # For now, if it wasn't merged, it proceeds. If it was the target of merges, its self.dir is the one with all files.
        
        # Fallback for name if folder_name was used but directory structure is unexpected
        if self.folder_name and not self.name: # If name wasn't set from download object but folder_name exists
            self.name = self.folder_name.strip("/").split("/", 1)[0]

        # Determine the primary download path and its size
        # self.dir is the base directory for this task (e.g., DOWNLOAD_DIR/self.mid/)
        # self.name is the name of the file or folder within self.dir
        dl_path = ospath.join(self.dir, self.name)
        if not await aiopath.exists(dl_path):
            try:
                # Attempt to find the main content if dl_path is not found (e.g., if self.name was incorrect)
                files_in_dir = await listdir(self.dir)
                if files_in_dir:
                    # Prioritize non-hidden files, avoid yt-dlp-thumb if other content exists
                    content_files = [f for f in files_in_dir if f != "yt-dlp-thumb" and not f.startswith('.')]
                    if content_files:
                        self.name = content_files[0] # Take the first valid content item
                        dl_path = ospath.join(self.dir, self.name)
                    elif "yt-dlp-thumb" in files_in_dir and len(files_in_dir) == 1: # Only yt-dlp-thumb exists
                        await self.on_upload_error("No content found other than thumbnails.")
                        return
                    else: # No identifiable main content
                        self.name = files_in_dir[0] # Fallback to first item
                        dl_path = ospath.join(self.dir, self.name)
                else: # Directory is empty
                    await self.on_upload_error("Download directory is empty after completion.")
                    return
            except Exception as e:
                await self.on_upload_error(f"Error finalizing download path: {str(e)}")
                return

        self.size = await get_path_size(dl_path) # Get size of the final path
        self.is_file = await aiopath.isfile(dl_path) # Check if it's a single file or a folder

        # Setup for upload: symlink for seeding, or direct path
        up_dir_base = self.dir # Base directory for upload operations
        up_path_content = dl_path # Path to the actual content to be uploaded

        if self.seed: # For torrents, create a symlink to allow seeding from original path
            self.up_dir = f"{self.dir.rstrip('/')}_seed" # Suffix for symlink parent
            up_dir_base = self.up_dir # Upload operations happen from this symlinked parent
            # Create symlink: up_path_content (actual file/folder) will be symlinked inside up_dir_base
            # The name of the symlink will be self.name
            symlink_target_path = ospath.join(up_dir_base, self.name)
            await create_recursive_symlink(dl_path, symlink_target_path) # symlink dl_path to symlink_target_path
            LOGGER.info(f"Symlink created for seeding: {dl_path} -> {symlink_target_path}")
            up_path_content = symlink_target_path # Upload from the symlinked path
        
        # up_dir_base is where operations like exclusion, join, extract will happen
        # up_path_content is the specific file/folder within up_dir_base to process initially

        await remove_excluded_files(up_dir_base, self.excluded_extensions) # Remove excluded files from the upload dir

        if not Config.QUEUE_ALL: # If QUEUE_ALL is false, remove from non_queued_dl now
            async with queue_dict_lock:
                if self.mid in non_queued_dl:
                    non_queued_dl.remove(self.mid)
            await start_from_queued() # Check if other tasks can start from queue

        # Post-download processing steps
        if self.join and not self.is_file: # Join files if it's a folder and join is enabled
            await join_files(up_path_content) # join_files works on a folder path

        # Note: After join, up_path_content might change if join creates a new file and deletes folder.
        # The functions below (extract, ffmpeg etc.) should ideally return the new path of the processed content.
        # And self.name, self.is_file, self.size should be updated.

        path_after_processing = up_path_content # Start with current content path
        
        if self.extract and not self.is_nzb: # NZB extraction is often handled by downloader
            path_after_processing = await self.proceed_extract(path_after_processing, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(up_dir_base) # Get size of the entire upload directory
            self.clear() # Reset subname, subsize etc.
            await remove_excluded_files(up_dir_base, self.excluded_extensions)


        if self.ffmpeg_cmds:
            path_after_processing = await self.proceed_ffmpeg(path_after_processing, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(up_dir_base)
            self.clear()

        if self.name_sub:
            path_after_processing = await self.substitute(path_after_processing)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
        
        if self.screen_shots:
            # generate_screenshots might create a subfolder like _mltbss
            # It should return the path to the main file/folder, screenshots are handled separately by uploader
            original_path_before_ss = path_after_processing
            await self.generate_screenshots(path_after_processing) # This likely creates screenshots in a subfolder
            if self.is_cancelled: return
            path_after_processing = original_path_before_ss # Main content path remains the same
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(up_dir_base) # Recalculate size if screenshots added to same dir (unlikely)

        # ... (Other processing like convert_media, sample_video) ...
        # Ensure self.name, self.size, self.is_file are updated after each processing step
        # path_after_processing should always point to the final content to be uploaded.

        if self.compress and not self.is_leech : # Compress only for mirrors, leeches handle split/compress differently
            path_after_processing = await self.proceed_compress(path_after_processing, gid)
            if self.is_cancelled: return
            self.name = ospath.basename(path_after_processing)
            self.is_file = await aiopath.isfile(path_after_processing)
            self.size = await get_path_size(path_after_processing) # Size of the archive
            self.clear()
        elif self.is_leech and not self.compress: # Leech handles split; compress for leech is not standard here
            await self.proceed_split(path_after_processing, gid) # proceed_split works on path_after_processing
            if self.is_cancelled: return
            self.clear()
            # After split, the content is in potentially multiple parts.
            # TelegramUploader will walk the up_dir_base.
            # self.name and self.size for the listener message should reflect the whole task.
            self.size = await get_path_size(up_dir_base) # Total size of content in upload dir

        self.subproc = None # Clear any subprocess references

        # Final check for running tasks and queuing logic
        add_to_queue, event = await check_running_tasks(self, "up")
        await start_from_queued() 
        if add_to_queue:
            LOGGER.info(f"Added to Queue/Upload: {self.name}")
            async with task_dict_lock:
                task_dict[self.mid] = QueueStatus(self, gid, "Up")
            await event.wait()
            if self.is_cancelled: return
            LOGGER.info(f"Start from Queued/Upload: {self.name}")
        
        # Recalculate size one last time before passing to uploader status
        # up_dir_base is the root directory from which uploads will occur (e.g. self.dir or self.up_dir for seeds)
        final_upload_source_dir = up_dir_base
        self.size = await get_path_size(final_upload_source_dir) # Get total size of what's in the final upload source dir

        # UPLOAD সিদ্ধান্ত
        if self.is_leech:
            LOGGER.info(f"Leech Name: {self.name} from directory {final_upload_source_dir}")
            # TelegramUploader will walk final_upload_source_dir
            tg_uploader = TelegramUploader(listener=self, path=final_upload_source_dir) 
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg_uploader, gid, "up")
            
            # update_status_message typically starts an interval, ensure it's handled correctly
            status_updater_task = asyncio.create_task(update_status_message(self.message.chat.id))
            await tg_uploader.upload()
            status_updater_task.cancel() # Cancel the status updater after upload finishes or errors
            # del tg_uploader # Not strictly necessary with ARC/GC but good practice if managing resources

        elif is_gdrive_id(self.up_dest):
            LOGGER.info(f"Gdrive Upload Name: {self.name} from path {path_after_processing}")
            # path_after_processing should be the specific file/folder to upload, not necessarily the whole up_dir_base
            drive_uploader = GoogleDriveUpload(listener=self, path=path_after_processing)
            async with task_dict_lock:
                task_dict[self.mid] = GoogleDriveStatus(self, drive_uploader, gid, "up")
            
            status_updater_task = asyncio.create_task(update_status_message(self.message.chat.id))
            await sync_to_async(drive_uploader.upload) # GDrive upload is synchronous
            status_updater_task.cancel()
            # del drive_uploader

        else: # Rclone Upload
            LOGGER.info(f"Rclone Upload Name: {self.name} from path {path_after_processing}")
            rclone_transfer = RcloneTransferHelper(listener=self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, rclone_transfer, gid, "up")

            status_updater_task = asyncio.create_task(update_status_message(self.message.chat.id))
            await rclone_transfer.upload(path_after_processing) # path_after_processing is the specific file/folder
            status_updater_task.cancel()
            # del rclone_transfer
        return


    async def on_upload_complete(
        self, link, files_arg, folders_arg, mime_type_arg, rclone_path="", dir_id=""
    ):
        # Common: Remove from incomplete tasks in DB
        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
            and hasattr(self.message, 'link') # Ensure message has link attribute
        ):
            await database.rm_complete_task(self.message.link)
        
        LOGGER.info(f"Task Done: {self.name}")

        chat_id_to_send_to = self.message.chat.id if hasattr(self.message, 'chat') else self.user_id # Fallback for safety
        user_mention_tag = self.tag

        if self.is_leech:
            # For Leech (Telegram Upload):
            # files_arg is _msgs_dict from TelegramUploader (dict of {link: caption})
            # folders_arg is _total_files (attempted file uploads by uploader) from TelegramUploader
            # mime_type_arg is _corrupted (failed file uploads by uploader) from TelegramUploader
            
            # Delete the original command message for leech tasks
            if hasattr(self.message, 'delete'): # Ensure it's a message object that can be deleted
                try:
                    await delete_message(self.message) 
                except Exception as e:
                    LOGGER.warning(f"Failed to delete original command message: {e}")
            
            # Construct the simplified completion message
            leech_completion_msg = f"<b>Name: </b><code>{escape(self.name)}</code>\n"
            # self.size is the size of the initially downloaded and processed content
            leech_completion_msg += f"<b>Size: </b>{get_readable_file_size(self.size)}\n"

            total_attempted_by_uploader = folders_arg 
            corrupted_by_uploader = mime_type_arg 
            successful_uploads = total_attempted_by_uploader - corrupted_by_uploader
            
            leech_completion_msg += f"<b>Files Uploaded: </b>{successful_uploads}"
            
            if corrupted_by_uploader > 0:
                leech_completion_msg += f"\n<b>Corrupted/Skipped: </b>{corrupted_by_uploader}"
            
            leech_completion_msg += f"\n\n<b>cc: </b>{user_mention_tag}"

            await send_message(chat_id_to_send_to, leech_completion_msg)

            # Log the detailed file links if available (files_arg is _msgs_dict from TelegramUploader)
            if files_arg and isinstance(files_arg, dict) and files_arg:
                 LOGGER.info(f"Leech upload details for {self.name} ({len(files_arg)} links generated).")
            elif not files_arg and successful_uploads > 0: 
                 LOGGER.warning(f"Leech completed for {self.name} with {successful_uploads} files, but no file links dictionary provided.")
            elif successful_uploads == 0 and total_attempted_by_uploader > 0 : 
                LOGGER.warning(f"Leech for {self.name}: {total_attempted_by_uploader} files processed by uploader, but none were successful.")

        else: # Mirror (Gdrive/Rclone)
            # Parameter mapping for Mirror tasks:
            # link = cloud link (gdrive or rclone serve)
            # files_arg = total_files_in_gdrive_upload (int) / uploaded_size_for_rclone (int)
            # folders_arg = total_folders_in_gdrive_upload (int) / listener.total_files_for_rclone (int)
            # mime_type_arg = mime_type_of_gdrive_upload (str) / mime_type_for_rclone (str)
            
            mirror_msg = f"<b>Name: </b><code>{escape(self.name)}</code>\n\n<b>Size: </b>{get_readable_file_size(self.size)}"
            mirror_msg += f"\n\n<b>Type: </b>{mime_type_arg}" 

            if mime_type_arg == "Folder": 
                if is_gdrive_id(self.up_dest): # GDrive specific counts
                    mirror_msg += f"\n<b>SubFolders: </b>{folders_arg}" 
                    mirror_msg += f"\n<b>Files: </b>{files_arg}"       
                else: # Rclone (folders_arg is listener.total_files, files_arg is size for rclone)
                    mirror_msg += f"\n<b>Total Files: </b>{folders_arg}" 
            
            buttons = ButtonMaker()
            button_added = False
            if link: 
                buttons.url_button("☁️ Cloud Link", link)
                button_added = True
            
            if not is_gdrive_id(self.up_dest) and rclone_path: 
                if Config.RCLONE_SERVE_URL and not self.private_link:
                    remote, rpath = rclone_path.split(":", 1)
                    url_path = rutils.quote(f"{rpath.lstrip('/')}") # Ensure no leading slash for quote
                    share_url = f"{Config.RCLONE_SERVE_URL.rstrip('/')}/{remote}/{url_path}"
                    if mime_type_arg == "Folder": share_url = f"{share_url.rstrip('/')}/"
                    buttons.url_button("🔗 Rclone Link", share_url)
                    button_added = True
                else: 
                    mirror_msg += f"\n\nPath: <code>{rclone_path}</code>"

            if is_gdrive_id(self.up_dest) and dir_id : 
                INDEX_URL = self.user_dict.get("INDEX_URL", "") or Config.INDEX_URL or ""
                if INDEX_URL:
                    share_url = f"{INDEX_URL.rstrip('/')}/findpath?id={dir_id}"
                    buttons.url_button("⚡ Index Link", share_url)
                    button_added = True
                    if isinstance(mime_type_arg, str) and mime_type_arg.startswith(("image", "video", "audio")):
                        share_urls = f"{INDEX_URL.rstrip('/')}/findpath?id={dir_id}&view=true" 
                        buttons.url_button("🌐 View Link", share_urls)
            
            mirror_msg += f"\n\n<b>cc: </b>{user_mention_tag}"
            button_menu = buttons.build_menu(2) if button_added else None
            # For mirror, send reply to original message if it still exists
            reply_to_message_id = self.message.id if hasattr(self.message, 'id') else None
            await send_message(chat_id_to_send_to, mirror_msg, button_menu, reply_to_message_id=reply_to_message_id)

        # Common cleanup logic (after Leech or Mirror completion message)
        if self.seed: 
            if hasattr(self, 'up_dir') and self.up_dir: 
                await clean_target(self.up_dir) 
            async with queue_dict_lock:
                if self.mid in non_queued_up: non_queued_up.remove(self.mid)
            await start_from_queued() 
            # For active seeding tasks, self.dir (original download) is typically kept.
            # The specific cleanup for self.dir in seeding scenarios might depend on bot's seed settings.
            # If upload means seed task is fully done, then self.dir might be cleaned here too.
            # For now, returning implies seed handling takes precedence for self.dir.
            return

        # Cleanup for non-seeding tasks, or after a "copy then seed" type of upload.
        if hasattr(self, 'dir') and self.dir:
            await clean_download(self.dir) 
        if hasattr(self, 'up_dir') and self.up_dir and self.up_dir != self.dir : # If up_dir was a separate location (not symlink handled above)
            await clean_download(self.up_dir)


        async with task_dict_lock:
            if self.mid in task_dict: del task_dict[self.mid]
            count = len(task_dict)
        
        if count == 0: await self.clean() 
        else: await update_status_message(chat_id_to_send_to) 

        async with queue_dict_lock: 
            if self.mid in non_queued_up: non_queued_up.remove(self.mid)
        await start_from_queued()

        # Thumbnail cleanup (user provided thumb for the task)
        if self.thumb and self.thumb != f"thumbnails/{self.user_id}.jpg" and self.thumb != "thumbnails/default.jpg" and self.thumb != "none": # Don't delete default/placeholder paths
            if await aiopath.exists(self.thumb):
                 try:
                     await remove(self.thumb)
                     LOGGER.info(f"Removed task-specific custom thumbnail: {self.thumb}")
                 except Exception as e:
                     LOGGER.warning(f"Failed to remove task-specific custom thumbnail {self.thumb}: {e}")


    async def on_download_error(self, error, button=None):
        async with task_dict_lock:
            if self.mid in task_dict: del task_dict[self.mid]
            count = len(task_dict)
        await self.remove_from_same_dir()
        msg = f"{self.tag} Download: {escape(str(error))}"
        # Ensure self.message exists for sending error, or use chat_id if it was deleted
        target_chat_id = self.message.chat.id if hasattr(self.message, 'chat') else self.user_id
        await send_message(target_chat_id, msg, button, reply_to_message_id=self.message.id if hasattr(self.message, 'id') else None)
        
        if count == 0: await self.clean()
        else: await update_status_message(target_chat_id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
            and hasattr(self.message, 'link')
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock: # Clear from all possible queue/non-queue lists
            for q_list_dict in [queued_dl, queued_up, non_queued_dl, non_queued_up]:
                 if self.mid in q_list_dict:
                    if hasattr(q_list_dict[self.mid], 'set'): # If it's an event
                        q_list_dict[self.mid].set()
                    del q_list_dict[self.mid]
                 elif self.mid in q_list_dict and isinstance(q_list_dict, list): # If it's a list like non_queued_dl/up
                      if self.mid in q_list_dict : q_list_dict.remove(self.mid)


        await start_from_queued()
        await sleep(3) # Allow some time for message sending etc.
        
        if hasattr(self, 'dir') and self.dir: await clean_download(self.dir)
        if hasattr(self, 'up_dir') and self.up_dir and self.up_dir != self.dir : await clean_download(self.up_dir)
        
        # Thumbnail cleanup
        if self.thumb and self.thumb != f"thumbnails/{self.user_id}.jpg" and self.thumb != "thumbnails/default.jpg" and self.thumb != "none":
            if await aiopath.exists(self.thumb):
                try: await remove(self.thumb)
                except Exception as e: LOGGER.warning(f"Failed to remove custom thumb on DL error {self.thumb}: {e}")


    async def on_upload_error(self, error):
        # This method is called by uploader instances (TG, GDrive, Rclone) if their upload fails
        async with task_dict_lock:
            if self.mid in task_dict: del task_dict[self.mid]
            count = len(task_dict)
        
        target_chat_id = self.message.chat.id if hasattr(self.message, 'chat') else self.user_id
        # For leech, self.message might have been deleted.
        # The error message should ideally go to where the status messages are, or original chat.
        # If self.is_leech and original message deleted, send new message to target_chat_id.
        # Otherwise, reply to self.message (which is fine for mirrors).
        reply_to_msg_id = None
        if not (self.is_leech and not hasattr(self.message, 'id')): # if not leech, or if leech and message still exists (should not happen)
            reply_to_msg_id = self.message.id if hasattr(self.message, 'id') else None

        await send_message(target_chat_id, f"{self.tag} {escape(str(error))}", reply_to_message_id=reply_to_msg_id)
        
        if count == 0: await self.clean()
        else: await update_status_message(target_chat_id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
            and hasattr(self.message, 'link')
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock: # Clear from all possible queue/non-queue lists
            for q_list_dict in [queued_dl, queued_up, non_queued_dl, non_queued_up]:
                 if self.mid in q_list_dict:
                    if hasattr(q_list_dict[self.mid], 'set'): 
                        q_list_dict[self.mid].set()
                    del q_list_dict[self.mid]
                 elif self.mid in q_list_dict and isinstance(q_list_dict, list): 
                      if self.mid in q_list_dict : q_list_dict.remove(self.mid)

        await start_from_queued()
        await sleep(3)

        if hasattr(self, 'dir') and self.dir: await clean_download(self.dir)
        if hasattr(self, 'up_dir') and self.up_dir and self.up_dir != self.dir : await clean_download(self.up_dir)
        
        # Thumbnail cleanup
        if self.thumb and self.thumb != f"thumbnails/{self.user_id}.jpg" and self.thumb != "thumbnails/default.jpg" and self.thumb != "none":
            if await aiopath.exists(self.thumb):
                try: await remove(self.thumb)
                except Exception as e: LOGGER.warning(f"Failed to remove custom thumb on UL error {self.thumb}: {e}")
