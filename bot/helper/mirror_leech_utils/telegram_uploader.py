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

# Assuming these are correctly pathed in your project structure
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
        if self._thumb != "none" and not await aiopath.exists(self._thumb):
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
                    parse_mode=ParseMode.HTML, # MODIFIED
                )
                self._sent_msg = sent_initial_msg
                self._initial_up_dest_message = sent_initial_msg
                if self._sent_msg:
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
                LOGGER.warning("Original command message deleted, cannot reply directly. Sending new message.")
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="<b>Warning:</b> Original command message was deleted. Uploads will proceed without a direct reply to it.",
                    disable_web_page_preview=True,
                    disable_notification=True,
                    parse_mode=ParseMode.HTML # MODIFIED
                )
        else:
            self._sent_msg = self._listener.message
        
        if self._sent_msg is None and not self._listener.up_dest :
             LOGGER.error("Could not determine a message to reply to and no up_dest is set.")
             await self._listener.on_upload_error("Failed to initialize reply message.")
             return False
        return True

    async def _prepare_file(self, file_, dirpath):
        if self._lprefix:
            cap_mono = f"{self._lprefix} <code>{escape(file_)}</code>"
            self._lprefix = re_sub("<.*?>", "", self._lprefix)
            new_path = ospath.join(dirpath, f"{self._lprefix} {file_}")
            await rename(self._up_path, new_path)
            self._up_path = new_path
        else:
            cap_mono = f"<code>{escape(file_)}</code>"
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
            if self._up_path != new_path :
                await rename(self._up_path, new_path)
                self._up_path = new_path
        return cap_mono

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg_obj in self._media_dict[key][subkey]:
            if key == "videos" and msg_obj.video:
                input_media = InputMediaVideo(
                    media=msg_obj.video.file_id, caption=msg_obj.caption
                )
                rlist.append(input_media)
            elif key == "documents" and msg_obj.document:
                input_media = InputMediaDocument(
                    media=msg_obj.document.file_id, caption=msg_obj.caption
                )
                rlist.append(input_media)
        return rlist
        
    async def _send_screenshots(self, dirpath, outputs):
        if not self._sent_msg and self._listener.up_dest:
            LOGGER.warning("No _sent_msg for screenshots in up_dest, attempting to send a placeholder.")
            task_name = getattr(self._listener, 'name', 'Unknown Task')
            placeholder_text = f"<b>Task:</b> {escape(task_name)}\n\n<i>Sending screenshots...</i>"
            client_to_use = TgClient.user if self._user_session else self._listener.client
            try:
                self._sent_msg = await client_to_use.send_message(
                    chat_id=self._listener.up_dest, text=placeholder_text, parse_mode=ParseMode.HTML, disable_notification=True # MODIFIED
                )
            except Exception as e:
                LOGGER.error(f"Failed to send placeholder for screenshots: {e}")
                await self._listener.on_upload_error(f"Failed to send placeholder for screenshots: {e}")
                return

        if not self._sent_msg:
             LOGGER.error("Cannot send screenshots as _sent_msg is not available.")
             self._corrupted += len(outputs)
             return

        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), ospath.basename(p))
            for p in outputs
        ]
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            try:
                sent_media_group = await self._sent_msg.reply_media_group(
                    media=batch, quote=True, disable_notification=True,
                )
                if sent_media_group:
                    self._sent_msg = sent_media_group[-1]
            except Exception as e:
                LOGGER.error(f"Error sending screenshot batch: {e}")
                self._corrupted += len(batch)


    async def _send_media_group(self, subkey, key, message_objects_list):
        if not self._sent_msg:
            LOGGER.error(f"Cannot send media group for {subkey} as _sent_msg is not available (no message to reply to).")
            if key in self._media_dict and subkey in self._media_dict[key]:
                del self._media_dict[key][subkey]
            return
        
        input_media_list = []
        for msg_obj in message_objects_list:
            caption_to_use = msg_obj.caption
            if key == "videos" and msg_obj.video:
                input_media_list.append(InputMediaVideo(media=msg_obj.video.file_id, caption=caption_to_use))
            elif key == "documents" and msg_obj.document:
                input_media_list.append(InputMediaDocument(media=msg_obj.document.file_id, caption=caption_to_use))

        if not input_media_list:
            LOGGER.error(f"No valid media to send for media group {subkey}.")
            if key in self._media_dict and subkey in self._media_dict[key]:
                del self._media_dict[key][subkey]
            return

        try:
            msgs_list_final = await self._sent_msg.reply_media_group(
                media=input_media_list, quote=True, disable_notification=True,
            )
            
            for msg_obj_to_delete in message_objects_list:
                if msg_obj_to_delete.link in self._msgs_dict:
                    del self._msgs_dict[msg_obj_to_delete.link]
                await delete_message(msg_obj_to_delete)
            
            if key in self._media_dict and subkey in self._media_dict[key]:
                del self._media_dict[key][subkey]

            if msgs_list_final:
                if self._listener.is_super_chat or self._listener.up_dest:
                    for m in msgs_list_final:
                        if m.link:
                             self._msgs_dict[m.link] = m.caption or ospath.basename(getattr(m, key[:-1]).file_name if hasattr(m, key[:-1]) and getattr(m, key[:-1]) else subkey)
                self._sent_msg = msgs_list_final[-1]
        except Exception as e:
            LOGGER.error(f"Error during _send_media_group for {subkey}: {e}")


    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            return

        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if self._listener.is_cancelled: return

            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            if dirpath.strip().endswith("_mltbss"):
                await self._send_screenshots(dirpath, files)
                if not self._listener.is_cancelled:
                    await rmtree(dirpath, ignore_errors=True)
                continue
            for file_ in natsorted(files):
                if self._listener.is_cancelled: return

                self._error = ""
                self._up_path = f_path = ospath.join(dirpath, file_)
                if not await aiopath.exists(self._up_path):
                    LOGGER.error(f"{self._up_path} not exists! Continue uploading!")
                    continue
                try:
                    f_size = await aiopath.getsize(self._up_path)
                    self._total_files += 1
                    if f_size == 0:
                        LOGGER.error(f"{self._up_path} size is zero, telegram doesn't upload zero size files")
                        self._corrupted += 1
                        continue
                    
                    if not self._sent_msg:
                        LOGGER.error(f"Critical: _sent_msg is None before processing file {file_}. Aborting this file.")
                        self._corrupted +=1
                        continue

                    cap_mono = await self._prepare_file(file_, dirpath)
                    
                    if self._last_msg_in_group:
                        group_lists = [
                            x for v in self._media_dict.values() for x in v.keys()
                        ]
                        original_file_base_for_group = get_base_name(file_) if is_archive(file_) else ospath.splitext(file_)[0]
                        match_for_group = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", original_file_base_for_group)
                        
                        current_file_group_name = None
                        if match_for_group:
                            current_file_group_name = match_for_group.group(0)

                        if not current_file_group_name or current_file_group_name not in group_lists:
                            for key_mg, value_mg in list(self._media_dict.items()):
                                for subkey_mg, msgs_mg in list(value_mg.items()):
                                    if len(msgs_mg) > 0 :
                                        await self._send_media_group(subkey_mg, key_mg, msgs_mg)
                    
                    if self._listener.hybrid_leech and self._listener.user_transmission and self._sent_msg:
                        self._user_session = f_size > 2097152000
                        client_to_use_for_get = TgClient.user if self._user_session else self._listener.client
                        try:
                             self._sent_msg = await client_to_use_for_get.get_messages(
                                 chat_id=self._sent_msg.chat.id,
                                 message_ids=self._sent_msg.id,
                             )
                        except Exception as e:
                            LOGGER.error(f"Failed to get/update _sent_msg in hybrid leech: {e}. Chain might be affected.")
                            if self._listener.up_dest: # Try to send a recovery message in up_dest
                                task_name = getattr(self._listener, 'name', 'Unknown Task')
                                lost_chain_text = f"<b>Task:</b> {escape(task_name)}\n<i>Warning: Reply chain may have been lost. Continuing upload.</i>"
                                try:
                                    self._sent_msg = await client_to_use_for_get.send_message(
                                        chat_id=self._listener.up_dest, 
                                        text=lost_chain_text, 
                                        parse_mode=ParseMode.HTML # MODIFIED
                                    )
                                except Exception as e_recovery:
                                    LOGGER.error(f"Failed to send recovery message for lost chain: {e_recovery}")
                                    # If this also fails, _sent_msg might be None, leading to failure for next file

                    self._last_msg_in_group = False
                    self._last_uploaded = 0
                    await self._upload_file(cap_mono, file_, self._up_path)
                    
                    if self._listener.is_cancelled: return

                    if (
                        not self._is_corrupted
                        and (self._listener.is_super_chat or self._listener.up_dest)
                        and not self._is_private
                        and self._sent_msg and self._sent_msg.link
                    ):
                        file_identifier_for_dict = self._sent_msg.caption if self._sent_msg.caption else file_
                        self._msgs_dict[self._sent_msg.link] = file_identifier_for_dict
                    
                    await sleep(1)

                except Exception as err:
                    if isinstance(err, RetryError):
                        LOGGER.info(f"Total Attempts for {self._up_path}: {err.last_attempt.attempt_number}")
                        err = err.last_attempt.exception()
                    LOGGER.error(f"Error processing {self._up_path}: {err}")
                    self._error = str(err)
                    self._corrupted += 1
                    if self._listener.is_cancelled: return
                
        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 0:
                    await self._send_media_group(subkey, key, msgs)
        
        if self._listener.is_cancelled: return

        if self._total_files == 0:
            await self._listener.on_upload_error(
                "No files to upload. Check EXCLUDED_EXTENSIONS or if path was empty."
            )
            if self._initial_up_dest_message:
                try: await delete_message(self._initial_up_dest_message)
                except Exception as e: LOGGER.warning(f"Could not delete initial task message on no files error: {e}")
            return

        if self._total_files <= self._corrupted:
            error_msg_to_report = self._error or "Check logs for details."
            await self._listener.on_upload_error(
                f"All files corrupted or unable to upload. Last error: {error_msg_to_report}"
            )
            if self._initial_up_dest_message:
                try: await delete_message(self._initial_up_dest_message)
                except Exception as e: LOGGER.warning(f"Could not delete initial task message on all files error: {e}")
            return
            
        LOGGER.info(f"Leech Completed: {self._listener.name}")
        await self._listener.on_upload_complete(
            None, self._msgs_dict, self._total_files, self._corrupted
        )

        if self._initial_up_dest_message:
            try:
                LOGGER.info(f"Attempting to delete initial task message: {self._initial_up_dest_message.id} in chat {self._initial_up_dest_message.chat.id}")
                await delete_message(self._initial_up_dest_message)
                LOGGER.info(f"Successfully deleted initial task message: {self._initial_up_dest_message.id}")
            except Exception as e:
                LOGGER.warning(f"Could not delete initial task message {self._initial_up_dest_message.id}: {e}")
        
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def _upload_file(self, cap_mono, file_original_name, o_path, force_document=False):
        if self._thumb is not None and self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None
        
        thumb_to_use = self._thumb
        self._is_corrupted = False
        
        if not self._sent_msg:
            LOGGER.error(f"Cannot upload {o_path}: _sent_msg is None (no message to reply to).")
            self._is_corrupted = True
            raise Exception(f"Cannot upload {o_path}: _sent_msg is None.")

        is_video, is_audio, is_image = await get_document_type(self._up_path)
        generated_thumb_path = None

        if not is_image and (thumb_to_use is None or (thumb_to_use == f"thumbnails/{self._listener.user_id}.jpg" and not await aiopath.exists(thumb_to_use))):
            file_name_no_ext = ospath.splitext(file_original_name)[0]
            base_dir_for_dlp_thumb = ospath.dirname(self._path) if ospath.isfile(self._path) else self._path
            potential_dlp_thumb = ospath.join(base_dir_for_dlp_thumb, "yt-dlp-thumb", f"{file_name_no_ext}.jpg")

            if await aiopath.isfile(potential_dlp_thumb):
                thumb_to_use = potential_dlp_thumb
                generated_thumb_path = potential_dlp_thumb
            elif is_audio and not is_video:
                generated_thumb_path = await get_audio_thumbnail(self._up_path)
                if generated_thumb_path: thumb_to_use = generated_thumb_path
        
        upload_key = ""
        try:
            if self._listener.as_doc or force_document or (not is_video and not is_audio and not is_image):
                upload_key = "documents"
                if is_video and (thumb_to_use is None or (thumb_to_use == f"thumbnails/{self._listener.user_id}.jpg" and not await aiopath.exists(thumb_to_use))):
                    temp_thumb = await get_video_thumbnail(self._up_path, None)
                    if temp_thumb:
                        thumb_to_use = temp_thumb
                        if not generated_thumb_path: generated_thumb_path = temp_thumb

                if self._listener.is_cancelled: return
                current_thumb_for_upload = None if thumb_to_use == "none" else thumb_to_use
                if current_thumb_for_upload and not await aiopath.exists(current_thumb_for_upload):
                    LOGGER.warning(f"Thumb path {current_thumb_for_upload} invalid before sending doc. Clearing thumb.")
                    current_thumb_for_upload = None

                self._sent_msg = await self._sent_msg.reply_document(
                    document=self._up_path, quote=True, thumb=current_thumb_for_upload, caption=cap_mono,
                    force_document=True, disable_notification=True, progress=self._upload_progress,
                )
            elif is_video:
                upload_key = "videos"
                duration = (await get_media_info(self._up_path))[0]
                
                if thumb_to_use is None or (thumb_to_use == f"thumbnails/{self._listener.user_id}.jpg" and not await aiopath.exists(thumb_to_use)):
                    if self._listener.thumbnail_layout:
                        temp_thumb = await get_multiple_frames_thumbnail(
                            self._up_path, self._listener.thumbnail_layout, self._listener.screen_shots,
                        )
                        if temp_thumb:
                             thumb_to_use = temp_thumb
                             if not generated_thumb_path: generated_thumb_path = temp_thumb
                    if thumb_to_use is None or (thumb_to_use == f"thumbnails/{self._listener.user_id}.jpg" and not await aiopath.exists(thumb_to_use)):
                        temp_thumb = await get_video_thumbnail(self._up_path, duration)
                        if temp_thumb:
                            thumb_to_use = temp_thumb
                            if not generated_thumb_path: generated_thumb_path = temp_thumb
                
                width, height = 0, 0
                current_thumb_for_upload = None if thumb_to_use == "none" else thumb_to_use
                if current_thumb_for_upload and await aiopath.exists(current_thumb_for_upload):
                    try:
                        with Image.open(current_thumb_for_upload) as img: width, height = img.size
                    except Exception as img_err:
                        LOGGER.warning(f"Could not open video thumbnail {current_thumb_for_upload}: {img_err}. Clearing thumb.")
                        current_thumb_for_upload = None
                else:
                     if current_thumb_for_upload: LOGGER.warning(f"Thumb path {current_thumb_for_upload} invalid for video. Clearing thumb.")
                     current_thumb_for_upload = None

                if width == 0 or height == 0: width, height = 480, 320

                if self._listener.is_cancelled: return
                self._sent_msg = await self._sent_msg.reply_video(
                    video=self._up_path, quote=True, caption=cap_mono, duration=duration, width=width, height=height,
                    thumb=current_thumb_for_upload, supports_streaming=True, disable_notification=True, progress=self._upload_progress,
                )
            elif is_audio:
                upload_key = "audios"
                duration, artist, title = await get_media_info(self._up_path)
                if self._listener.is_cancelled: return
                current_thumb_for_upload = None if thumb_to_use == "none" else thumb_to_use
                if current_thumb_for_upload and not await aiopath.exists(current_thumb_for_upload):
                     LOGGER.warning(f"Thumb path {current_thumb_for_upload} invalid for audio. Clearing thumb.")
                     current_thumb_for_upload = None

                self._sent_msg = await self._sent_msg.reply_audio(
                    audio=self._up_path, quote=True, caption=cap_mono, duration=duration, performer=artist, title=title,
                    thumb=current_thumb_for_upload, disable_notification=True, progress=self._upload_progress,
                )
            else: # is_image
                upload_key = "photos"
                if self._listener.is_cancelled: return
                self._sent_msg = await self._sent_msg.reply_photo(
                    photo=self._up_path, quote=True, caption=cap_mono,
                    disable_notification=True, progress=self._upload_progress,
                )

            if (
                not self._listener.is_cancelled and self._media_group and self._sent_msg and
                (self._sent_msg.video or self._sent_msg.document)
            ):
                group_key_for_dict = "documents" if self._sent_msg.document else "videos"
                match_split = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", file_original_name)
                if match_split:
                    pname = match_split.group(0)
                    if pname not in self._media_dict[group_key_for_dict]:
                        self._media_dict[group_key_for_dict][pname] = []
                    
                    self._media_dict[group_key_for_dict][pname].append(self._sent_msg)
                    
                    msgs_in_group = self._media_dict[group_key_for_dict][pname]
                    if len(msgs_in_group) == 10:
                        await self._send_media_group(pname, group_key_for_dict, msgs_in_group)
                    else:
                        self._last_msg_in_group = True
            
            if generated_thumb_path and generated_thumb_path != self._thumb and await aiopath.exists(generated_thumb_path):
                await remove(generated_thumb_path)

        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            if generated_thumb_path and generated_thumb_path != self._thumb and await aiopath.exists(generated_thumb_path):
                await remove(generated_thumb_path)
            raise
        except Exception as err:
            self._is_corrupted = True
            if generated_thumb_path and generated_thumb_path != self._thumb and await aiopath.exists(generated_thumb_path):
                await remove(generated_thumb_path)

            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {self._up_path}, Upload Key: {upload_key}")
            
            if isinstance(err, BadRequest) and upload_key and upload_key != "documents":
                LOGGER.error(f"Retrying As Document due to BadRequest for {upload_key}. Path: {self._up_path}")
                return await self._upload_file(cap_mono, file_original_name, o_path, True)
            raise err

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
