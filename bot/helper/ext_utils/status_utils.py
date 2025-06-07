from html import escape
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
from asyncio import iscoroutinefunction, gather

# Assuming these imports are available from your project structure
from ... import (
    task_dict,
    task_dict_lock,
    bot_start_time,
    status_dict,
    DOWNLOAD_DIR,
    sabnzbd_client, # Added from second file
)
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager # Added from second file
from ...core.jdownloader_booter import jdownloader # Added from second file
from ..telegram_helper.button_build import ButtonMaker
from ..helper.ext_utils.status_utils import ( # Assuming this path from second file
    MirrorStatus,
    get_readable_file_size,
    get_readable_time,
    speed_string_to_bytes,
)


# --- This is the new helper function to calculate overall speeds ---
async def get_download_status(download):
    """Helper to get status and speed from a download object."""
    tool = download.tool
    speed = 0
    if tool in ["telegram", "yt-dlp", "rclone", "gDriveApi"]:
        # Get speed for specific download types
        speed = speed_string_to_bytes(download.speed())
        
    status = (
        await download.status()
        if iscoroutinefunction(download.status)
        else download.status()
    )
    return status, speed


async def calculate_overall_speeds():
    """
    Calculates and returns the overall download, upload, and seed speeds
    across all active services and tasks.
    """
    dl_speed = 0
    up_speed = 0
    
    # Get overall torrent speeds
    torrent_dl_speed, seed_speed = await TorrentManager.overall_speed()
    dl_speed += torrent_dl_speed

    # Add Sabnzbd speed if active
    if sabnzbd_client.LOGGED_IN:
        sds = await sabnzbd_client.get_downloads()
        dl_speed += int(float(sds["queue"].get("kbpersec", "0"))) * 1024

    # Add JDownloader speed if active
    if jdownloader.is_connected:
        jdres = await jdownloader.device.downloadcontroller.get_speed_in_bytes()
        dl_speed += jdres

    # Add speeds from other individual tasks (gDrive, yt-dlp, etc.)
    async with task_dict_lock:
        if not task_dict:
            return dl_speed, up_speed, seed_speed
            
        status_results = await gather(
            *(get_download_status(dl) for dl in task_dict.values())
        )
        for status, speed in status_results:
            if status == MirrorStatus.STATUS_DOWNLOAD:
                dl_speed += speed
            elif status == MirrorStatus.STATUS_UPLOAD:
                up_speed += speed
                
    return dl_speed, up_speed, seed_speed

# --- Your original code, now modified to use the new function ---

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

STATUSES = {
    "ALL": "All",
    "DL": MirrorStatus.STATUS_DOWNLOAD,
    "UP": MirrorStatus.STATUS_UPLOAD,
    "QD": MirrorStatus.STATUS_QUEUEDL,
    "QU": MirrorStatus.STATUS_QUEUEUP,
    "AR": MirrorStatus.STATUS_ARCHIVE,
    "EX": MirrorStatus.STATUS_EXTRACT,
    "SD": MirrorStatus.STATUS_SEED,
    "CL": MirrorStatus.STATUS_CLONE,
    "CM": MirrorStatus.STATUS_CONVERT,
    "SP": MirrorStatus.STATUS_SPLIT,
    "SV": MirrorStatus.STATUS_SAMVID,
    "FF": MirrorStatus.STATUS_FFMPEG,
    "PA": MirrorStatus.STATUS_PAUSED,
    "CK": MirrorStatus.STATUS_CHECK,
}


async def get_task_by_gid(gid: str):
    async with task_dict_lock:
        for tk in task_dict.values():
            if hasattr(tk, "seeding"):
                await tk.update()
            if tk.gid() == gid:
                return tk
        return None


async def get_specific_tasks(status, user_id):
    if status == "All":
        if user_id:
            return [tk for tk in task_dict.values() if tk.listener.user_id == user_id]
        else:
            return list(task_dict.values())
    tasks_to_check = (
        [tk for tk in task_dict.values() if tk.listener.user_id == user_id]
        if user_id
        else list(task_dict.values())
    )
    coro_tasks = [tk for tk in tasks_to_check if iscoroutinefunction(tk.status)]
    coro_statuses = await gather(*(tk.status() for tk in coro_tasks))
    result = []
    coro_index = 0
    for tk in tasks_to_check:
        if tk in coro_tasks:
            st = coro_statuses[coro_index]
            coro_index += 1
        else:
            st = tk.status()
        if (st == status) or (
            status == MirrorStatus.STATUS_DOWNLOAD and st not in STATUSES.values()
        ):
            result.append(tk)
    return result


async def get_all_tasks(req_status: str, user_id):
    async with task_dict_lock:
        return await get_specific_tasks(req_status, user_id)


def get_progress_bar_string(pct):
    pct = float(pct.strip("%"))
    p = min(max(pct, 0), 100)
    cFull = int(p // 8)
    p_str = "■" * cFull
    p_str += "□" * (12 - cFull)
    return f"[{p_str}]"


async def get_readable_message(sid, is_user, page_no=1, status="All", page_step=1):
    msg = ""
    button = None

    tasks = await get_specific_tasks(status, sid if is_user else None)

    STATUS_LIMIT = Config.STATUS_LIMIT
    tasks_no = len(tasks)
    pages = (max(tasks_no, 1) + STATUS_LIMIT - 1) // STATUS_LIMIT
    if page_no > pages:
        page_no = (page_no - 1) % pages + 1
        status_dict[sid]["page_no"] = page_no
    elif page_no < 1:
        page_no = pages - (abs(page_no) % pages)
        status_dict[sid]["page_no"] = page_no
    start_position = (page_no - 1) * STATUS_LIMIT

    for index, task in enumerate(
        tasks[start_position : STATUS_LIMIT + start_position], start=1
    ):
        if status != "All":
            tstatus = status
        elif iscoroutinefunction(task.status):
            tstatus = await task.status()
        else:
            tstatus = task.status()
        if task.listener.is_super_chat:
            msg += f"<b>{index + start_position}.<a href='{task.listener.message.link}'>{tstatus}</a>: </b>"
        else:
            msg += f"<b>{index + start_position}.{tstatus}: </b>"
        msg += f"<code>{escape(f'{task.name()}')}</code>"
        if task.listener.subname:
            msg += f"\n<i>{task.listener.subname}</i>"
        if (
            tstatus not in [MirrorStatus.STATUS_SEED, MirrorStatus.STATUS_QUEUEUP]
            and task.listener.progress
        ):
            progress = task.progress()
            msg += f"\n{get_progress_bar_string(progress)} {progress}"
            if task.listener.subname:
                subsize = f"/{get_readable_file_size(task.listener.subsize)}"
                ac = len(task.listener.files_to_proceed)
                count = f"{task.listener.proceed_count}/{ac or '?'}"
            else:
                subsize = ""
                count = ""
            msg += f"\n<b>Processed:</b> {task.processed_bytes()}{subsize}"
            if count:
                msg += f"\n<b>Count:</b> {count}"
            msg += f"\n<b>Size:</b> {task.size()}"
            msg += f"\n<b>Speed:</b> {task.speed()}"
            msg += f"\n<b>ETA:</b> {task.eta()}"
            if (
                tstatus == MirrorStatus.STATUS_DOWNLOAD
                and task.listener.is_torrent
                or task.listener.is_qbit
            ):
                try:
                    msg += f"\n<b>Seeders:</b> {task.seeders_num()} | <b>Leechers:</b> {task.leechers_num()}"
                except:
                    pass
        elif tstatus == MirrorStatus.STATUS_SEED:
            msg += f"\n<b>Size: </b>{task.size()}"
            msg += f"\n<b>Speed: </b>{task.seed_speed()}"
            msg += f"\n<b>Uploaded: __{task.uploaded_bytes()}__"
            msg += f"\n<b>Ratio: </b>{task.ratio()}"
            msg += f" | <b>Time: </b>{task.seeding_time()}"
        else:
            msg += f"\n<b>Size: </b>{task.size()}"
        msg += f"\n<b>Cancel: </b><code>/c {task.gid()}</code>\n\n"

    if len(msg) == 0:
        if status == "All":
            return None, None
        else:
            msg = f"No Active {status} Tasks!\n\n"

    buttons = ButtonMaker()
    if not is_user:
        buttons.data_button("📜", f"status {sid} ov", position="header")
    if len(tasks) > STATUS_LIMIT:
        msg += f"<b>Page:</b> {page_no}/{pages} | <b>Tasks:</b> {tasks_no} | <b>Step:</b> {page_step}\n"
        buttons.data_button("<<", f"status {sid} pre", position="header")
        buttons.data_button(">>", f"status {sid} nex", position="header")
        if tasks_no > 30:
            for i in [1, 2, 4, 6, 8, 10, 15]:
                buttons.data_button(i, f"status {sid} ps {i}", position="footer")
    if status != "All" or tasks_no > 20:
        for label, status_value in list(STATUSES.items()):
            if status_value != status:
                buttons.data_button(label, f"status {sid} st {status_value}")
    buttons.data_button("♻️", f"status {sid} ref", position="header")
    button = buttons.build_menu(8)
    
    # ---- MODIFICATION: Call the new function and use the results ----
    dl_speed, up_speed, seed_speed = await calculate_overall_speeds()
    
    msg += f"<b>CPU:</b> {cpu_percent()}% | <b>FREE:</b> {get_readable_file_size(disk_usage(DOWNLOAD_DIR).free)}"
    msg += f"\n<b>RAM:</b> {virtual_memory().percent}% | <b>UPTIME:</b> {get_readable_time(time() - bot_start_time)}"
    
    # Corrected the multi-line f-string and used the calculated speed variables
    msg += f"""
<b>ODLS:</b> {get_readable_file_size(dl_speed)}/s
<b>OULS:</b> {get_readable_file_size(up_speed)}/s
<b>OSDS:</b> {get_readable_file_size(seed_speed)}/s"""
    
    return msg, button
