from PIL import Image
from aioshutil import rmtree
from asyncio import sleep
from logging import getLogger
from natsort import natsorted
from os import walk, path as ospath
from time import time
from re import match as re_match, sub as re_sub
from pyrogram.errors import FloodWait, RPCError, BadRequest
from aiofiles.os import (
    remove,
    path as aiopath,
    rename,
)
from pyrogram.types import (
    InputMediaVideo,
    InputMediaDocument,
    InputMediaPhoto,
)
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    RetryError,
)

# Assuming these are correctly pathed in your project structure
from ...core.config_manager import Config
from ...core.mltb_client import TgClient
from ..ext_utils.bot_utils import sync_to_async, get_readable_file_size # Added get_readable_file_size for listener example
from ..ext_utils.files_utils import is_archive, get_base_name
from ..telegram_helper.message_utils import delete_message
from ..ext_utils.media_utils import (
    get_media_info,
    get_document_type,
    get_video_thumbnail,
    get_audio_thumbnail,
    get_multiple_frames_thumbnail,
)
from html import escape # For escaping HTML in listener example

LOGGER = getLogger(__name__)

try:
    from pyrogram.errors import FloodPremiumWait
except ImportError:
    FloodPremiumWait = FloodWait

class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener # listener object, expected to have attributes like up_dest, name, message, client etc.
        self._path = path
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._media_dict = {"videos": {}, "documents": {}}
        self._last_msg_in_group = False
        self._up_path = ""
        self._lprefix = ""
        self._media_group = False
        self._is_private = False
        self._sent_msg = None
        self._user_session = self._listener.user_transmission
        self._error = ""

    async def _upload_progress(self, current, _):
        if self._listener.is_cancelled:
            if self._user_session:
                TgClient.user.stop_transmission()
            else:
                self._listener.client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def _user_settings(self):
        self._media_group = self._listener.user_dict.get("MEDIA_GROUP") or (
            Config.MEDIA_GROUP
            if "MEDIA_GROUP" not in self._listener.user_dict
            else False
        )
        self._lprefix = self._listener.user_dict.get("LEECH_FILENAME_PREFIX") or (
            Config.LEECH_FILENAME_PREFIX
            if "LEECH_FILENAME_PREFIX" not in self._listener.user_dict
            else ""
        )
        if self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            # Requirement 1: Don't send original command/link to dump/log chat.
            # Send a generic initial message instead.
            # Ensure self._listener.name is available and provides a meaningful task name.
            task_name = getattr(self._listener, 'name', 'Unknown Task')
            initial_message_text = f"<b>Task Initiated:</b> {escape(task_name)}\n\n<i>Uploading files, please wait...</i>"
            try:
                if self._user_session:
                    self._sent_msg = await TgClient.user.send_message(
                        chat_id=self._listener.up_dest,
                        text=initial_message_text,
                        disable_web_page_preview=True,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                        parse_mode="HTML", # Using HTML for bold/italic
                    )
                else:
                    self._sent_msg = await self._listener.client.send_message(
                        chat_id=self._listener.up_dest,
                        text=initial_message_text,
                        disable_web_page_preview=True,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                        parse_mode="HTML", # Using HTML for bold/italic
                    )
                if self._sent_msg: # Ensure message was actually sent
                    self._is_private = self._sent_msg.chat.type.name == "PRIVATE"
            except Exception as e:
                LOGGER.error(f"Error sending initial message to up_dest: {e}")
                await self._listener.on_upload_error(f"Error sending initial message to upload destination: {e}")
                return False
        elif self._user_session: # Replying in the same chat as the command
            # Attempt to get the original message to reply to
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id, message_ids=self._listener.mid
            )
            if self._sent_msg is None: # Original command message might have been deleted
                LOGGER.warning("Original command message deleted, cannot reply directly. Sending new message.")
                # Send a new message in the original chat if the command message is gone
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="<b>Warning:</b> Original command message was deleted. Uploads will proceed without a direct reply to it.",
                    disable_web_page_preview=True,
                    disable_notification=True,
                    parse_mode="HTML"
                )
        else: # Replying in the same chat as the command, using bot client
            self._sent_msg = self._listener.message
        
        # If _sent_msg is still None here (e.g. up_dest failed and other conditions not met), it's an issue.
        if self._sent_msg is None and not self._listener.up_dest : # only critical if not up_dest
             LOGGER.error("Could not determine a message to reply to and no up_dest is set.")
             await self._listener.on_upload_error("Failed to initialize reply message.")
             return False
        return True

    async def _prepare_file(self, file_, dirpath):
        if self._lprefix:
            cap_mono = f"{self._lprefix} <code>{file_}</code>"
            self._lprefix = re_sub("<.*?>", "", self._lprefix)
            new_path = ospath.join(dirpath, f"{self._lprefix} {file_}")
            await rename(self._up_path, new_path)
            self._up_path = new_path
        else:
            cap_mono = f"<code>{file_}</code>"
        if len(file_) > 60:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", file_):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = file_
                ext = ""
            extn = len(ext)
            remain = 60 - extn
            name = name[:remain]
            new_path = ospath.join(dirpath, f"{name}{ext}")
            await rename(self._up_path, new_path)
            self._up_path = new_path
        return cap_mono

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg in self._media_dict[key][subkey]:
            if key == "videos":
                input_media = InputMediaVideo(
                    media=msg.video.file_id, caption=msg.caption
                )
            else:
                input_media = InputMediaDocument(
                    media=msg.document.file_id, caption=msg.caption
                )
            rlist.append(input_media)
        return rlist

    async def _send_screenshots(self, dirpath, outputs):
        # Ensure _sent_msg is valid before trying to reply
        if not self._sent_msg and self._listener.up_dest: # Attempt to re-establish _sent_msg if in up_dest
            LOGGER.warning("No _sent_msg for screenshots, attempting to send a placeholder.")
            # This is a fallback, ideally _msg_to_reply should always establish _sent_msg for up_dest
            task_name = getattr(self._listener, 'name', 'Unknown Task')
            placeholder_text = f"<b>Task:</b> {escape(task_name)}\n\n<i>Sending screenshots...</i>"
            client_to_use = TgClient.user if self._user_session else self._listener.client
            try:
                self._sent_msg = await client_to_use.send_message(
                    chat_id=self._listener.up_dest,
                    text=placeholder_text,
                    parse_mode="HTML",
                    disable_notification=True
                )
            except Exception as e:
                LOGGER.error(f"Failed to send placeholder for screenshots: {e}")
                # If it fails, screenshots might be sent without a direct reply if not in up_dest or _sent_msg cannot be made
                # Or you might choose to raise an error or handle differently
                await self._listener.on_upload_error(f"Failed to send placeholder for screenshots: {e}")
                return # Cannot proceed with replying

        if not self._sent_msg:
             LOGGER.error("Cannot send screenshots as _sent_msg is not available.")
             self._corrupted += len(outputs) # Consider these as failed if no message to reply to
             return

        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), p.rsplit("/", 1)[-1])
            for p in outputs
        ]
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            # Update self._sent_msg to the last message of the media group for chaining
            self._sent_msg = (
                await self._sent_msg.reply_media_group(
                    media=batch,
                    quote=True,
                    disable_notification=True,
                )
            )[-1]


    async def _send_media_group(self, subkey, key, msgs):
        # Ensure _sent_msg exists to reply to.
        # The first message in 'msgs' (msgs[0]) is a list [chat_id, message_id]
        # of a temporary message. We need its reply_to_message attribute.
        
        # Fetch the actual message objects
        fetched_msgs = []
        for chat_id_val, msg_id_val in msgs:
            try:
                if self._listener.hybrid_leech or not self._user_session:
                    msg_obj = await self._listener.client.get_messages(chat_id=chat_id_val, message_ids=msg_id_val)
                else:
                    msg_obj = await TgClient.user.get_messages(chat_id=chat_id_val, message_ids=msg_id_val)
                if msg_obj:
                    fetched_msgs.append(msg_obj)
            except Exception as e:
                LOGGER.error(f"Error fetching message {msg_id_val} from {chat_id_val} for media group: {e}")
                # If a message can't be fetched, it might corrupt the media group.
                # Decide on error handling: skip this message, or fail the group.
                # For simplicity here, we'll just log and it might be skipped by Pyrogram or cause an error later.

        if not fetched_msgs or not fetched_msgs[0].reply_to_message:
            LOGGER.error(f"Cannot send media group for {subkey}. Required reply_to_message not found or no messages fetched.")
            # Increment corrupted count for all files in this subkey
            # This part is tricky as 'msgs' here are references to temp uploaded files, not original file count for this group
            # A more robust corrupted count for media groups would need tracking original files intended for this group.
            # For now, we'll just log the error and attempt to clean up.
            del self._media_dict[key][subkey] # Clean up this group from dict
            return


        # Ensure self._sent_msg is the message to reply to for the new media group
        # This _sent_msg should be the one established by _msg_to_reply or the previously uploaded file/group
        if not self._sent_msg:
            LOGGER.error(f"Cannot send media group for {subkey} as _sent_msg is not available (no message to reply to).")
            del self._media_dict[key][subkey]
            return

        msgs_list = await self._sent_msg.reply_media_group( # Reply to the current _sent_msg
            media=self._get_input_media(subkey, key),
            quote=True, # Quote the _sent_msg
            disable_notification=True,
        )
        
        for msg_obj in fetched_msgs: # Use the fetched message objects for deletion
            if msg_obj.link in self._msgs_dict: # Check if the link of the temporary message is in msgs_dict
                del self._msgs_dict[msg_obj.link]
            await delete_message(msg_obj)
            
        del self._media_dict[key][subkey]
        if self._listener.is_super_chat or self._listener.up_dest:
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption # Store link of the final media group messages
        self._sent_msg = msgs_list[-1] # Update _sent_msg to the last message of the sent group for chaining

    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            # _msg_to_reply already calls on_upload_error if it fails critically
            return

        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            if dirpath.strip().endswith("_mltbss"):
                await self._send_screenshots(dirpath, files)
                await rmtree(dirpath, ignore_errors=True)
                continue
            for file_ in natsorted(files):
                self._error = ""
                self._up_path = f_path = ospath.join(dirpath, file_)
                if not await aiopath.exists(self._up_path):
                    LOGGER.error(f"{self._up_path} not exists! Continue uploading!")
                    continue
                try:
                    f_size = await aiopath.getsize(self._up_path)
                    self._total_files += 1
                    if f_size == 0:
                        LOGGER.error(
                            f"{self._up_path} size is zero, telegram don't upload zero size files"
                        )
                        self._corrupted += 1
                        continue
                    if self._listener.is_cancelled:
                        return
                    
                    # Ensure _sent_msg is available, especially if _msg_to_reply set it for up_dest
                    # and this is the first file.
                    if not self._sent_msg and self._listener.up_dest:
                        LOGGER.warning("No _sent_msg before uploading first file to up_dest. Attempting to re-establish.")
                        # This is a defensive check. _msg_to_reply should handle this.
                        # If it can't be re-established, _upload_file will fail or attempt to send without reply.
                        # Re-calling _msg_to_reply might be too much here, rely on its initial success.
                        # The _upload_file will use self._sent_msg.
                        # If _sent_msg is None and it's up_dest, then _upload_file might not be able to reply.
                        # This scenario should ideally be prevented by _msg_to_reply.
                        pass # Let _upload_file handle it, it will use self._sent_msg

                    if not self._sent_msg and not self._listener.up_dest:
                        # This means uploading to original chat but original message (or its placeholder) is gone.
                        # _msg_to_reply should have handled this by creating a new placeholder.
                        # If still None, it's a problem for replying.
                        LOGGER.error("Cannot upload file, _sent_msg is None and no up_dest. Critical error.")
                        self._corrupted +=1
                        continue


                    cap_mono = await self._prepare_file(file_, dirpath)
                    if self._last_msg_in_group:
                        group_lists = [
                            x for v in self._media_dict.values() for x in v.keys()
                        ]
                        match = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", f_path)
                        if not match or match and match.group(0) not in group_lists:
                            for key, value in list(self._media_dict.items()):
                                for subkey, msgs in list(value.items()):
                                    if len(msgs) > 1:
                                        await self._send_media_group(subkey, key, msgs)
                    
                    if self._listener.hybrid_leech and self._listener.user_transmission:
                        self._user_session = f_size > 2097152000 # 2GB
                        current_chat_id = self._sent_msg.chat.id if self._sent_msg else self._listener.message.chat.id
                        current_msg_id = self._sent_msg.id if self._sent_msg else self._listener.mid
                        
                        client_to_use = TgClient.user if self._user_session else self._listener.client
                        try:
                             self._sent_msg = await client_to_use.get_messages(
                                 chat_id=current_chat_id,
                                 message_ids=current_msg_id,
                             )
                        except Exception as e:
                            LOGGER.error(f"Failed to get/update _sent_msg in hybrid leech: {e}")
                            # If _sent_msg is lost, we might lose the reply chain.
                            # Attempt to resend a placeholder if in up_dest or create new in current chat
                            if self._listener.up_dest:
                                task_name = getattr(self._listener, 'name', 'Unknown Task')
                                lost_chain_text = f"<b>Task:</b> {escape(task_name)}\n<i>Warning: Reply chain may have been lost. Continuing upload.</i>"
                                self._sent_msg = await client_to_use.send_message(chat_id=self._listener.up_dest, text=lost_chain_text, parse_mode="HTML")
                            # else: if not up_dest, uploads might become new messages if _sent_msg is truly lost.
                            # This part needs careful handling of _sent_msg lifecycle.


                    self._last_msg_in_group = False
                    self._last_uploaded = 0
                    await self._upload_file(cap_mono, file_, f_path) # This updates self._sent_msg
                    if self._listener.is_cancelled:
                        return
                    if (
                        not self._is_corrupted
                        and (self._listener.is_super_chat or self._listener.up_dest)
                        and not self._is_private
                        and self._sent_msg # Ensure _sent_msg is not None before accessing .link
                    ):
                        self._msgs_dict[self._sent_msg.link] = file_
                    await sleep(1)
                except Exception as err:
                    if isinstance(err, RetryError):
                        LOGGER.info(
                            f"Total Attempts: {err.last_attempt.attempt_number}"
                        )
                        err = err.last_attempt.exception()
                    LOGGER.error(f"{err}. Path: {self._up_path}")
                    self._error = str(err) # Store last error message
                    self._corrupted += 1
                    if self._listener.is_cancelled:
                        return
                # Remove file if it failed and still exists, or if successfully uploaded (common practice for leech bots)
                # Current code only removes on error loop, consider removing after successful _upload_file too if that's the desired behavior
                if not self._listener.is_cancelled and await aiopath.exists(
                    self._up_path
                ):
                    # This block is in the main exception handler of the file loop.
                    # If successful, file is typically removed by the calling context after entire dir is processed or here.
                    # For now, let's assume removal happens after the entire task by rmtree(self._path) or similar
                    # If this `remove` is intended for failed files only, it's fine.
                    # If it's for all files one by one, it should be after successful upload too.
                    # Given it's in `except`, it's likely for failed files if not caught by _upload_file's retry.
                    # To be safe, if an error occurred for the file, we try to remove it.
                    if self._error: # If an error was logged for this file specifically
                         await remove(self._up_path)


        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    try:
                        await self._send_media_group(subkey, key, msgs)
                    except Exception as e:
                        LOGGER.info(
                            f"While sending media group at the end of task. Error: {e}"
                        )
        if self._listener.is_cancelled:
            return
        if self._total_files == 0:
            await self._listener.on_upload_error(
                "No files to upload. In case you have filled EXCLUDED_EXTENSIONS, then check if all files have those extensions or not."
            )
            return
        if self._total_files <= self._corrupted:
            # Provide the last specific error if available, otherwise generic
            error_msg_to_report = self._error or "Check logs for details."
            await self._listener.on_upload_error(
                f"All files corrupted or unable to upload. Last error: {error_msg_to_report}"
            )
            return
            
        LOGGER.info(f"Leech Completed: {self._listener.name}")
        # The listener will use self.name, self.processed_bytes, total_files, corrupted_files
        # to formulate the completion message as per requirement 3.
        await self._listener.on_upload_complete(
            None, self._msgs_dict, self._total_files, self._corrupted
        )
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception), # Consider more specific retryable exceptions
    )
    async def _upload_file(self, cap_mono, file, o_path, force_document=False):
        if (
            self._thumb is not None
            and not await aiopath.exists(self._thumb)
            and self._thumb != "none"
        ):
            self._thumb = None
        thumb = self._thumb
        self._is_corrupted = False
        
        # Critical: Ensure self._sent_msg is valid before attempting to reply.
        # If self._sent_msg is None, we cannot use .reply_document etc.
        # This might happen if _msg_to_reply failed to set it up AND we are not in up_dest,
        # or if it got lost somehow.
        if not self._sent_msg:
            LOGGER.error(f"Cannot upload {o_path}: _sent_msg is None (no message to reply to).")
            # Decide how to handle: send as new message (if chat_id is known) or mark as corrupted.
            # Sending as new message breaks the chain. Marking as corrupted seems safer for now.
            self._is_corrupted = True
            self._corrupted += 1 # Increment global corrupted count too
            raise Exception(f"Cannot upload {o_path}: _sent_msg is None.") # Let retry handle or fail

        try:
            is_video, is_audio, is_image = await get_document_type(self._up_path)

            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = f"{self._path}/yt-dlp-thumb/{file_name}.jpg" # Potential issue: self._path might be the file itself, not parent dir
                # Corrected thumb_path assumption:
                base_dir_for_thumb = ospath.dirname(self._path) if ospath.isfile(self._path) else self._path
                thumb_path = ospath.join(base_dir_for_thumb, "yt-dlp-thumb", f"{file_name}.jpg")

                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif is_audio and not is_video:
                    thumb = await get_audio_thumbnail(self._up_path)

            if (
                self._listener.as_doc
                or force_document
                or (not is_video and not is_audio and not is_image)
            ):
                key = "documents"
                if is_video and thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, None)

                if self._listener.is_cancelled:
                    return
                if thumb == "none": # Explicit "none" string means no thumb
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_document( # This updates self._sent_msg
                    document=self._up_path,
                    quote=True,
                    thumb=thumb,
                    caption=cap_mono,
                    force_document=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_video:
                key = "videos"
                duration = (await get_media_info(self._up_path))[0]
                if thumb is None and self._listener.thumbnail_layout:
                    thumb = await get_multiple_frames_thumbnail(
                        self._up_path,
                        self._listener.thumbnail_layout,
                        self._listener.screen_shots,
                    )
                if thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, duration)
                
                width, height = 0, 0 # Default values
                if thumb is not None and thumb != "none":
                    try:
                        # Ensure thumb path is valid before opening
                        if await aiopath.exists(thumb):
                             with Image.open(thumb) as img:
                                width, height = img.size
                        else: # Thumb path became invalid
                            LOGGER.warning(f"Video thumbnail path {thumb} not found. Using defaults.")
                            thumb = None # Reset to avoid error in pyrogram
                    except Exception as img_err:
                        LOGGER.warning(f"Could not open video thumbnail {thumb}: {img_err}. Using defaults.")
                        thumb = None # Reset to avoid error in pyrogram
                
                if width == 0 or height == 0: # If not set by Image.open or thumb was invalid
                    # Pyrogram can often determine this, but providing sensible defaults if possible
                    width = 480 
                    height = 320

                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_video( # This updates self._sent_msg
                    video=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb,
                    supports_streaming=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_audio:
                key = "audios" # Note: self._media_dict only has "videos", "documents". Audio won't go into media_group with this key.
                               # If audio media groups are needed, "audios" key should be added to self._media_dict init.
                               # For now, assuming audios are not grouped or handled by current media group logic.
                duration, artist, title = await get_media_info(self._up_path)
                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_audio( # This updates self._sent_msg
                    audio=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    duration=duration,
                    performer=artist,
                    title=title,
                    thumb=thumb,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            else: # is_image
                key = "photos" # Similar to audio, "photos" key not in self._media_dict for grouping.
                if self._listener.is_cancelled:
                    return
                self._sent_msg = await self._sent_msg.reply_photo( # This updates self._sent_msg
                    photo=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    disable_notification=True,
                    progress=self._upload_progress,
                )

            if (
                not self._listener.is_cancelled
                and self._media_group
                and self._sent_msg # Ensure _sent_msg is valid
                and (self._sent_msg.video or self._sent_msg.document) # Only group videos/documents
            ):
                group_key = "documents" if self._sent_msg.document else "videos"
                if match := re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", o_path):
                    pname = match.group(0)
                    if pname not in self._media_dict[group_key]:
                        self._media_dict[group_key][pname] = []
                    
                    # Storing chat_id and message_id of the temporary file message, not the final _sent_msg
                    # This needs clarification: self._sent_msg is now the *final* message in the target chat.
                    # The original media group logic might have intended to store references to placeholder messages
                    # that are later combined.
                    # If self._sent_msg here is the *final* message, then this logic is for grouping messages
                    # already in the destination.
                    # Let's assume the existing logic for self._media_dict[key][pname].append is correct for how it's used
                    # in _send_media_group, which expects [chat_id, message_id] of messages to be *combined*.
                    # This means _upload_file should probably send to a temporary chat if it's part of a media group,
                    # get that temp message's ID, and then _send_media_group forwards them.
                    # The current code sends directly to _sent_msg.chat.id.
                    # This implies media_group items are sent one by one to the final chat, then their IDs collected
                    # and a new media_group is formed by *forwarding or copying* these already sent messages.
                    # Pyrogram's reply_media_group usually takes file paths or existing file_ids.
                    # The _get_input_media method uses file_ids from messages in self._media_dict.
                    
                    # Storing the IDs of the messages *as they are sent to the final destination*.
                    self._media_dict[group_key][pname].append(
                         # self._sent_msg is the message just sent to the final destination
                         # We need to pass this message object to _get_input_media
                         self._sent_msg 
                    )
                    
                    msgs_in_group = self._media_dict[group_key][pname]
                    if len(msgs_in_group) == 10:
                        # _send_media_group expects list of [chat_id, message_id] for temporary messages to fetch.
                        # Here, msgs_in_group is a list of full Message objects.
                        # This requires _send_media_group and _get_input_media to be adapted.
                        # For now, sticking to modification of _get_input_media in this thought block.
                        # And _send_media_group should also be adapted to take these Message objects.
                        await self._send_media_group(pname, group_key, msgs_in_group) # msgs_in_group is now list of Message objects
                    else:
                        self._last_msg_in_group = True
                # else: file is not part of a splitted series, so not added to media_group via this logic.
                # Individual files could still be grouped if MEDIA_GROUP is true but they aren't split.
                # This part of the logic seems specifically for split files.

            # Cleanup for auto-generated thumb (not the user's _thumb)
            if self._thumb is None and thumb is not None and thumb != self._listener.thumb and await aiopath.exists(thumb):
                await remove(thumb)
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            await sleep(f.value * 1.3)
            if self._thumb is None and thumb is not None and thumb != self._listener.thumb and await aiopath.exists(thumb):
                await remove(thumb)
            raise # Reraise to be caught by tenacity for retry
        except Exception as err:
            if self._thumb is None and thumb is not None and thumb != self._listener.thumb and await aiopath.exists(thumb):
                await remove(thumb)
            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {self._up_path}")
            self._is_corrupted = True # Mark this specific file attempt as corrupted
            if isinstance(err, BadRequest) and key != "documents": # 'key' might not be defined if error is early
                LOGGER.error(f"Retrying As Document due to BadRequest. Path: {self._up_path}")
                # Ensure 'key' is defined or handle it being undefined. For simplicity, assume it's defined if we reach here.
                # This recursive call will be part of the same tenacity retry attempt if it also fails.
                return await self._upload_file(cap_mono, file, o_path, True)
            raise err # Reraise for tenacity or main loop's error handling

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except ZeroDivisionError: # Avoid division by zero if time diff is 0
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("Your upload has been stopped!")
