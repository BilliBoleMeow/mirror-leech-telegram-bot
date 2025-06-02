# File: bot/helper/mirror_leech_utils/telegram_uploader.py

from PIL import Image
from aioshutil import rmtree
from asyncio import sleep
from logging import getLogger
from natsort import natsorted
from os import walk, path as ospath
from time import time
from re import match as re_match, sub as re_sub
from pyrogram.errors import FloodWait, RPCError, BadRequest
from pyrogram.enums import ParseMode # Import ParseMode
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

from ...core.config_manager import Config
from ...core.mltb_client import TgClient
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.files_utils import is_archive, get_base_name
from ..telegram_helper.message_utils import delete_message
from ..ext_utils.media_utils import (
    get_media_info, 
    get_document_type,
    get_video_thumbnail,
    get_audio_thumbnail,
    get_multiple_frames_thumbnail,
    get_detailed_media_streams_info, # Ensure this is imported and implemented in media_utils.py
)
from html import escape

LOGGER = getLogger(__name__)

try:
    from pyrogram.errors import FloodPremiumWait
except ImportError:
    FloodPremiumWait = FloodWait

class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path # Root path of the download/leech task
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or (f"thumbnails/{listener.user_id}.jpg" if hasattr(listener, 'user_id') else "thumbnails/default.jpg")
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
        self._initial_up_dest_message = None 
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
        if self._thumb and self._thumb != "none" and not await aiopath.exists(self._thumb):
            LOGGER.warning(f"Custom thumbnail not found at {self._thumb}. Clearing.")
            self._thumb = None 

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            task_name = getattr(self._listener, 'name', 'Unknown Task')
            initial_message_text = f"<b>Task Initiated:</b> {escape(task_name)}\n\n<i>Uploading files, please wait...</i>"
            client_to_use = TgClient.user if self._user_session else self._listener.client
            try:
                sent_initial_msg = await client_to_use.send_message(
                    chat_id=self._listener.up_dest,
                    text=initial_message_text,
                    disable_web_page_preview=True,
                    message_thread_id=self._listener.chat_thread_id,
                    disable_notification=True,
                    parse_mode=ParseMode.HTML,
                )
                self._sent_msg = sent_initial_msg
                self._initial_up_dest_message = sent_initial_msg 
                if self._sent_msg and self._sent_msg.chat and self._sent_msg.chat.type : # Ensure chat and type exist
                         self._is_private = self._sent_msg.chat.type.name == "PRIVATE" 
            except Exception as e:
                LOGGER.error(f"Error sending initial message to up_dest: {e}")
                await self._listener.on_upload_error(f"Error sending initial message to upload destination: {e}")
                return False
        elif self._user_session: 
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id, message_ids=self._listener.mid
            )
            if self._sent_msg is None: 
                LOGGER.warning("Original command message deleted. Sending new placeholder message.")
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="<b>Warning:</b> Original command message was deleted. Uploads will proceed without a direct reply to it.",
                    disable_web_page_preview=True,
                    disable_notification=True,
                    parse_mode=ParseMode.HTML 
                )
        else: 
            self._sent_msg = self._listener.message
        
        if self._sent_msg is None and not self._listener.up_dest: 
             LOGGER.error("Could not determine a message to reply to and no up_dest is set.")
             await self._listener.on_upload_error("Failed to initialize reply message.")
             return False
        return True

    async def _prepare_file(self, file_original_name: str, current_dirpath: str) -> str:
        filename_display_for_caption = escape(file_original_name)
        base_caption_part = f"<code>{filename_display_for_caption}</code>"
        
        current_filename_on_disk = file_original_name

        if self._lprefix:
            safe_lprefix_for_disk = re_sub("<.*?>", "", self._lprefix) 
            base_caption_part = f"{self._lprefix} {base_caption_part}" 
            
            prefixed_disk_filename = f"{safe_lprefix_for_disk} {file_original_name}"
            new_disk_path = ospath.join(current_dirpath, prefixed_disk_filename)

            if self._up_path != new_disk_path:
                try:
                    if await aiopath.exists(self._up_path): 
                        await rename(self._up_path, new_disk_path)
                        self._up_path = new_disk_path 
                        current_filename_on_disk = prefixed_disk_filename 
                    else:
                        LOGGER.warning(f"Original path {self._up_path} not found for prefix rename. Skipping rename.")
                except Exception as e:
                    LOGGER.warning(f"Failed to apply prefix rename from {self._up_path} to {new_disk_path}: {e}")

        if len(current_filename_on_disk) > 60: 
            if is_archive(current_filename_on_disk):
                name = get_base_name(current_filename_on_disk)
                ext = current_filename_on_disk.split(name, 1)[1]
            elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", current_filename_on_disk):
                name = match.group(0)
                ext = current_filename_on_disk.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(current_filename_on_disk)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = current_filename_on_disk
                ext = ""
            
            extn = len(ext)
            remain = max(0, 60 - extn) 
            name = name[:remain]
            truncated_filename = f"{name}{ext}"
            
            if current_filename_on_disk != truncated_filename:
                new_truncated_path = ospath.join(ospath.dirname(self._up_path), truncated_filename)
                if self._up_path != new_truncated_path:
                    try:
                        if await aiopath.exists(self._up_path):
                            await rename(self._up_path, new_truncated_path)
                            self._up_path = new_truncated_path 
                        else:
                             LOGGER.warning(f"Path {self._up_path} not found for truncation rename. Skipping.")
                    except Exception as e:
                        LOGGER.warning(f"Failed to apply truncation rename for {self._up_path} to {new_truncated_path}: {e}")
        return base_caption_part

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg_obj in self._media_dict[key][subkey]: 
            caption_to_use = msg_obj.caption 
            if key == "videos" and msg_obj.video:
                input_media = InputMediaVideo(media=msg_obj.video.file_id, caption=caption_to_use)
                rlist.append(input_media)
            elif key == "documents" and msg_obj.document:
                input_media = InputMediaDocument(media=msg_obj.document.file_id, caption=caption_to_use)
                rlist.append(input_media)
        return rlist
        
    async def _send_screenshots(self, dirpath, outputs):
        if not self._sent_msg and self._listener.up_dest:
            LOGGER.warning("No _sent_msg for screenshots in up_dest, sending placeholder.")
            task_name = getattr(self._listener, 'name', 'Unknown Task')
            placeholder_text = f"<b>Task:</b> {escape(task_name)}\n\n<i>Sending screenshots...</i>"
            client_to_use = TgClient.user if self._user_session else self._listener.client
            try:
                self._sent_msg = await client_to_use.send_message(
                    chat_id=self._listener.up_dest, text=placeholder_text, 
                    parse_mode=ParseMode.HTML, disable_notification=True
                )
            except Exception as e:
                LOGGER.error(f"Failed to send placeholder for screenshots: {e}")
                await self._listener.on_upload_error(f"Failed to send placeholder for screenshots: {e}")
                return

        if not self._sent_msg: 
             LOGGER.error("Cannot send screenshots: _sent_msg is unavailable.")
             self._corrupted += len(outputs) 
             return

        inputs = [InputMediaPhoto(ospath.join(dirpath, p), ospath.basename(p)) for p in outputs]
        
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            try:
                sent_media_group_msgs = await self._sent_msg.reply_media_group(
                    media=batch, quote=True, disable_notification=True,
                )
                if sent_media_group_msgs: 
                    self._sent_msg = sent_media_group_msgs[-1] 
            except Exception as e:
                LOGGER.error(f"Error sending screenshot batch: {e}")
                self._corrupted += len(batch)

    async def _send_media_group(self, subkey, key, message_objects_list):
        if not self._sent_msg:
            LOGGER.error(f"Cannot send media group for {subkey}: _sent_msg is unavailable.")
            if key in self._media_dict and subkey in self._media_dict[key]:
                del self._media_dict[key][subkey] 
            return
        
        input_media_list = self._get_input_media(subkey, key) 

        if not input_media_list:
            LOGGER.error(f"No valid InputMedia to send for media group {subkey}.")
            if key in self._media_dict and subkey in self._media_dict[key]:
                del self._media_dict[key][subkey]
            return

        try:
            final_media_group_msgs = await self._sent_msg.reply_media_group(
                media=input_media_list, quote=True, disable_notification=True,
            )
            
            if key in self._media_dict and subkey in self._media_dict[key]:
                del self._media_dict[key][subkey]

            if final_media_group_msgs:
                if self._listener.is_super_chat or self._listener.up_dest:
                    for m in final_media_group_msgs:
                        if m.link: 
                             self._msgs_dict[m.link] = m.caption or subkey 
                self._sent_msg = final_media_group_msgs[-1] 
        except Exception as e:
            LOGGER.error(f"Error sending final media group for {subkey}: {e}")

    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res: return

        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if self._listener.is_cancelled: return
            if dirpath.strip().endswith("/yt-dlp-thumb"): 
                continue
            if dirpath.strip().endswith("_mltbss"): 
                 await self._send_screenshots(dirpath, natsorted(files))
                 if not self._listener.is_cancelled:
                     await rmtree(dirpath, ignore_errors=True)
                 continue 

            for file_original_iter_name in natsorted(files): 
                if self._listener.is_cancelled: return
                self._error = ""
                self._up_path = ospath.join(dirpath, file_original_iter_name)

                if not await aiopath.exists(self._up_path):
                     LOGGER.warning(f"File {self._up_path} not found. Skipping.")
                     continue
                try:
                    f_size = await aiopath.getsize(self._up_path)
                    self._total_files += 1
                    if f_size == 0:
                        LOGGER.error(f"{self._up_path} is zero size. Skipping.")
                        self._corrupted += 1
                        continue
                    
                    if not self._sent_msg: 
                        LOGGER.critical(f"Cannot proceed: _sent_msg is None before processing {file_original_iter_name}.")
                        await self._listener.on_upload_error("Internal error: Reply message context lost.")
                        return 

                    base_caption_part = await self._prepare_file(file_original_iter_name, dirpath) 
                                        
                    if self._last_msg_in_group:
                        group_lists_keys = {k for v in self._media_dict.values() for k in v.keys()}
                        current_file_base_for_group = get_base_name(file_original_iter_name) if is_archive(file_original_iter_name) else ospath.splitext(file_original_iter_name)[0]
                        match_for_current_group = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", current_file_base_for_group)
                        current_file_group_key = match_for_current_group.group(0) if match_for_current_group else None

                        if not current_file_group_key or current_file_group_key not in group_lists_keys:
                            for key_mg, value_mg in list(self._media_dict.items()):
                                for subkey_mg, msgs_mg in list(value_mg.items()):
                                    if len(msgs_mg) > 0 : 
                                        LOGGER.info(f"Sending pending media group {subkey_mg} before {file_original_iter_name}")
                                        await self._send_media_group(subkey_mg, key_mg, msgs_mg)
                    
                    if self._listener.hybrid_leech and self._listener.user_transmission and self._sent_msg:
                        self._user_session = f_size > 2097152000 
                        client_to_use = TgClient.user if self._user_session else self._listener.client
                        try: 
                             self._sent_msg = await client_to_use.get_messages(
                                 chat_id=self._sent_msg.chat.id, message_ids=self._sent_msg.id,
                             )
                        except Exception as e:
                            LOGGER.error(f"Failed to refresh _sent_msg in hybrid leech: {e}.")
                            if self._listener.up_dest:
                                task_name = getattr(self._listener, 'name', 'Unknown Task')
                                recovery_text = f"<b>Task:</b> {escape(task_name)}\n<i>Warning: Reply chain context issue.</i>"
                                try:
                                    self._sent_msg = await client_to_use.send_message(
                                        chat_id=self._listener.up_dest, text=recovery_text, parse_mode=ParseMode.HTML
                                    )
                                except Exception as final_e:
                                    LOGGER.critical(f"Failed to send recovery message: {final_e}. Upload may fail.")
                                    self._sent_msg = None 

                    self._last_msg_in_group = False 
                    self._last_uploaded = 0
                    
                    await self._upload_file(base_caption_part, file_original_iter_name, self._up_path) 
                    
                    if self._listener.is_cancelled: return

                    if not self._is_corrupted and self._sent_msg and self._sent_msg.link and \
                       (self._listener.is_super_chat or self._listener.up_dest) and not self._is_private:
                        self._msgs_dict[self._sent_msg.link] = self._sent_msg.caption or file_original_iter_name
                    
                    await sleep(1) 

                except Exception as err:
                    if isinstance(err, RetryError): 
                        LOGGER.warning(f"Upload failed for {self._up_path} after {err.last_attempt.attempt_number} attempts. Error: {err.last_attempt.exception()}")
                        err = err.last_attempt.exception() 
                    else:
                        LOGGER.error(f"Error processing {self._up_path}: {err}")
                    self._error = str(err) 
                    self._corrupted += 1
                    if self._listener.is_cancelled: return
                
        for key_mg, value_mg in list(self._media_dict.items()):
            for subkey_mg, msgs_mg in list(value_mg.items()):
                if len(msgs_mg) > 0: 
                    LOGGER.info(f"Sending final pending media group {subkey_mg}")
                    await self._send_media_group(subkey_mg, key_mg, msgs_mg)
        
        if self._listener.is_cancelled: return

        if self._total_files == 0:
            await self._listener.on_upload_error("No files were found to upload.")
            if self._initial_up_dest_message: 
                try: await delete_message(self._initial_up_dest_message)
                except Exception as e: LOGGER.warning(f"Could not delete initial msg (no files): {e}")
            return

        if self._corrupted >= self._total_files :
            err_report = self._error or "Multiple errors occurred. Check logs."
            await self._listener.on_upload_error(f"All uploads failed or files corrupted. Last error: {err_report}")
            if self._initial_up_dest_message: 
                try: await delete_message(self._initial_up_dest_message)
                except Exception as e: LOGGER.warning(f"Could not delete initial msg (all failed): {e}")
            return
            
        LOGGER.info(f"Leech completed for: {self._listener.name}")
        await self._listener.on_upload_complete(None, self._msgs_dict, self._total_files, self._corrupted)

        if self._initial_up_dest_message:
            try:
                LOGGER.info(f"Attempting to delete initial task message: {self._initial_up_dest_message.id}")
                await delete_message(self._initial_up_dest_message)
                LOGGER.info(f"Successfully deleted initial task message.")
            except Exception as e:
                LOGGER.warning(f"Could not delete initial task message {self._initial_up_dest_message.id}: {e}")
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8), 
        stop=stop_after_attempt(3), 
        retry=retry_if_exception_type(Exception), 
    )
    async def _upload_file(self, base_caption_part: str, file_original_name: str, file_path_on_disk: str, force_document: bool = False):
        if self._thumb and self._thumb != "none" and not await aiopath.exists(self._thumb):
            LOGGER.warning(f"User-defined thumbnail {self._thumb} not found. Clearing.")
            self._thumb = None 
        
        thumb_to_use_for_upload = self._thumb 
        self._is_corrupted = False 
        
        if not self._sent_msg: 
            LOGGER.critical(f"Cannot upload {file_path_on_disk}: _sent_msg is None.")
            self._is_corrupted = True 
            raise Exception(f"Cannot upload {file_path_on_disk}: _sent_msg is None (no message to reply to).")

        is_video, is_audio, is_image = await get_document_type(file_path_on_disk)
        generated_thumb_disk_path = None 
        final_caption_to_send = base_caption_part 

        # ---- Append detailed media info to caption (No Emojis, New Lines, No Subtitles, Extra Line Space) ----
        if is_video or is_audio:
            try:
                streams_info = await get_detailed_media_streams_info(file_path_on_disk)
                media_info_parts = []

                if is_video and streams_info.get("video_streams"):
                    vs = streams_info["video_streams"][0] 
                    v_codec = vs.get("codec_name", "N/A").upper()
                    v_height = vs.get("height")
                    quality = f"{v_height}p" if v_height else ""
                    info_str = f"{v_codec} {quality}".strip()
                    if info_str and info_str.lower() != "n/a": 
                        media_info_parts.append(f"Video: {info_str}")
                
                audio_data = streams_info.get("audio_streams", [])
                if audio_data:
                    langs = [s.get("tags", {}).get("language", "und").upper()[:3] for s in audio_data]
                    valid_langs = sorted(list(set(lang for lang in langs if lang != "UND"))) 
                    if not valid_langs and "UND" in langs: langs_to_show = ["UND"]
                    elif not valid_langs: langs_to_show = ["N/A"] 
                    else: langs_to_show = valid_langs
                    
                    langs_str = ", ".join(langs_to_show)
                    media_info_parts.append(f"Audio: {len(audio_data)} ({langs_str})")

                # Subtitle information is intentionally omitted.

                if media_info_parts:
                    # Each part on a new line, with an EXTRA blank line after the filename
                    media_info_string = "\n\n" + "\n".join(media_info_parts) # Added extra \n here
                    final_caption_to_send = f"{final_caption_to_send}{media_info_string}"
            except Exception as e_media_info:
                LOGGER.warning(f"Could not get/format detailed media info for {file_path_on_disk}: {e_media_info}")
        # ---- End detailed media info ----
        
        if not is_image:
            if thumb_to_use_for_upload is None: 
                fname_no_ext = ospath.splitext(file_original_name)[0]
                dlp_thumb_dir_base = self._path if ospath.isdir(self._path) else ospath.dirname(self._path)
                potential_dlp_thumb = ospath.join(dlp_thumb_dir_base, "yt-dlp-thumb", f"{fname_no_ext}.jpg")
                if await aiopath.isfile(potential_dlp_thumb):
                    thumb_to_use_for_upload = potential_dlp_thumb
                    generated_thumb_disk_path = potential_dlp_thumb 
                elif is_audio and not is_video: 
                    temp_thumb_path = await get_audio_thumbnail(file_path_on_disk)
                    if temp_thumb_path:
                        thumb_to_use_for_upload = temp_thumb_path
                        generated_thumb_disk_path = temp_thumb_path
        
        upload_as_type_key = "" 
        if thumb_to_use_for_upload == "none": 
            thumb_for_pyrogram = None
        elif thumb_to_use_for_upload and not await aiopath.exists(thumb_to_use_for_upload):
            LOGGER.warning(f"Final thumb path {thumb_to_use_for_upload} invalid. Clearing.")
            thumb_for_pyrogram = None
            if thumb_to_use_for_upload == generated_thumb_disk_path: generated_thumb_disk_path = None
        else:
            thumb_for_pyrogram = thumb_to_use_for_upload

        try:
            if self._listener.as_doc or force_document or (not is_video and not is_audio and not is_image):
                upload_as_type_key = "documents"
                if is_video and not thumb_for_pyrogram: 
                    temp_thumb = await get_video_thumbnail(file_path_on_disk, None)
                    if temp_thumb:
                        thumb_for_pyrogram = temp_thumb
                        if not generated_thumb_disk_path: generated_thumb_disk_path = temp_thumb
                
                if self._listener.is_cancelled: return
                self._sent_msg = await self._sent_msg.reply_document(
                    document=file_path_on_disk, quote=True, thumb=thumb_for_pyrogram, caption=final_caption_to_send,
                    force_document=True, disable_notification=True, progress=self._upload_progress,
                )
            elif is_video:
                upload_as_type_key = "videos"
                video_duration_info = await get_media_info(file_path_on_disk) 
                duration = video_duration_info[0] if video_duration_info and len(video_duration_info) > 0 else 0
                
                if not thumb_for_pyrogram:
                    if self._listener.thumbnail_layout: 
                        temp_thumb = await get_multiple_frames_thumbnail(
                            file_path_on_disk, self._listener.thumbnail_layout, self._listener.screen_shots,
                        )
                    else: 
                        temp_thumb = await get_video_thumbnail(file_path_on_disk, duration)
                    
                    if temp_thumb:
                        thumb_for_pyrogram = temp_thumb
                        if not generated_thumb_disk_path: generated_thumb_disk_path = temp_thumb
                
                width, height = 0, 0 
                if thumb_for_pyrogram and await aiopath.exists(thumb_for_pyrogram):
                    try:
                        with Image.open(thumb_for_pyrogram) as img: width, height = img.size
                    except Exception as img_err:
                        LOGGER.warning(f"Could not open video thumb {thumb_for_pyrogram} for W/H: {img_err}.")
                if width == 0 or height == 0: width, height = 480, 320

                if self._listener.is_cancelled: return
                self._sent_msg = await self._sent_msg.reply_video(
                    video=file_path_on_disk, quote=True, caption=final_caption_to_send, duration=duration, 
                    width=width, height=height, thumb=thumb_for_pyrogram, 
                    supports_streaming=True, disable_notification=True, progress=self._upload_progress,
                )
            elif is_audio:
                upload_as_type_key = "audios"
                duration, artist, title = await get_media_info(file_path_on_disk) 
                if self._listener.is_cancelled: return
                self._sent_msg = await self._sent_msg.reply_audio(
                    audio=file_path_on_disk, quote=True, caption=final_caption_to_send, 
                    duration=duration, performer=artist, title=title,
                    thumb=thumb_for_pyrogram, disable_notification=True, progress=self._upload_progress,
                )
            else: # is_image
                upload_as_type_key = "photos"
                if self._listener.is_cancelled: return
                self._sent_msg = await self._sent_msg.reply_photo(
                    photo=file_path_on_disk, quote=True, caption=final_caption_to_send,
                    disable_notification=True, progress=self._upload_progress,
                )

            if not self._listener.is_cancelled and self._media_group and self._sent_msg and \
               (self._sent_msg.video or self._sent_msg.document): 
                
                group_key = "documents" if self._sent_msg.document else "videos"
                split_match = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", file_original_name)
                if split_match:
                    group_name = split_match.group(0)
                    if group_name not in self._media_dict[group_key]:
                        self._media_dict[group_key][group_name] = []
                    
                    self._media_dict[group_key][group_name].append(self._sent_msg) 
                    
                    if len(self._media_dict[group_key][group_name]) == 10: 
                        await self._send_media_group(group_name, group_key, self._media_dict[group_key][group_name])
                    else:
                        self._last_msg_in_group = True 
            
            if generated_thumb_disk_path and generated_thumb_disk_path != self._thumb and await aiopath.exists(generated_thumb_disk_path):
                LOGGER.debug(f"Cleaning up generated thumb: {generated_thumb_disk_path}")
                await remove(generated_thumb_disk_path)

        except (FloodWait, FloodPremiumWait) as flood_err:
            LOGGER.warning(str(flood_err))
            if generated_thumb_disk_path and generated_thumb_disk_path != self._thumb and await aiopath.exists(generated_thumb_disk_path):
                await remove(generated_thumb_disk_path)
            raise 
        except Exception as upload_err:
            self._is_corrupted = True 
            if generated_thumb_disk_path and generated_thumb_disk_path != self._thumb and await aiopath.exists(generated_thumb_disk_path):
                await remove(generated_thumb_disk_path)

            err_log_type = "RPCError: " if isinstance(upload_err, RPCError) else ""
            LOGGER.error(f"{err_log_type}{upload_err}. Path: {file_path_on_disk}, Upload attempted as: {upload_as_type_key}")
            
            if isinstance(upload_err, BadRequest) and upload_as_type_key and upload_as_type_key != "documents":
                LOGGER.info(f"Retrying As Document due to BadRequest for {upload_as_type_key} at {file_path_on_disk}")
                # For document retry, use the simple base_caption_part to avoid complex caption on generic doc.
                return await self._upload_file(base_caption_part, file_original_name, file_path_on_disk, True) 
            raise upload_err

    @property
    def speed(self):
        try:
            elapsed_time = time() - self._start_time
            return self._processed_bytes / elapsed_time if elapsed_time > 0 else 0
        except: return 0 

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("Your upload has been stopped!")
