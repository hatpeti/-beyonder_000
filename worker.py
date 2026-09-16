import asyncio
import os
import websockets
import json
import random
import re
import time
import math
import hashlib
import mimetypes
import glob
import shutil
import contextlib
import subprocess
import html
import logging
from pathlib import Path

import nest_asyncio
import aria2p

from wzgram import Client, filters, enums, raw
from wzgram.handlers import MessageHandler, CallbackQueryHandler
from wzgram.types import InlineKeyboardMarkup, InlineKeyboardButton
from wzgram.errors import FloodWait, RPCError

nest_asyncio.apply()
logging.getLogger("wzgram").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================
MASTER_WS_URL = "wss://leech-production-214b.up.railway.app"
TARGET_CHANNEL = "@animedubsinhla"
WATERMARK = "@animesinhala1"

PART_SIZE = 512 * 1024
UPLOAD_WORKERS = 8
PART_RETRIES = 8
UI_INTERVAL = 2.0
MAX_FILE_SIZE = 2000 * 1024 * 1024
BIG_FILE_THRESHOLD = 10 * 1024 * 1024
CONCURRENCY_LIMIT = 3
TASK_SEMAPHORE = asyncio.Semaphore(CONCURRENCY_LIMIT)

SESSION_DIR = "/content/telegram_sessions"
os.makedirs(SESSION_DIR, exist_ok=True)
THUMB_DIR = "/content/bot_thumbnail"
os.makedirs(THUMB_DIR, exist_ok=True)
CUSTOM_THUMB_PATH = os.path.join(THUMB_DIR, "custom_thumb.jpg")

# Global Instances
app = None
upload_client = None
aria2_api = None

# Runtime State
batch_states = {}
thumbnail_states = {}
user_settings = {}
current_tasks = {}
cancel_flags = {}
upload_runtime = {}

# ============================================================
# HELPERS & FORMATTERS
# ============================================================
def check_gpu():
    try: return subprocess.run(["nvidia-smi"], capture_output=True, text=True).returncode == 0
    except: return False

def format_bytes(size):
    try: size = float(size)
    except: size = 0.0
    if size <= 0: return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = min(int(math.floor(math.log(size, 1024))), len(units) - 1)
    value = size / (1024 ** i)
    if value >= 100: return f"{value:.0f} {units[i]}"
    if value >= 10: return f"{value:.1f} {units[i]}"
    return f"{value:.2f} {units[i]}"

def format_mbps(bytes_per_second):
    return f"{(bytes_per_second * 8) / 1_000_000:.2f} Mbps"

def format_time(seconds):
    try: seconds = max(0, int(seconds))
    except: seconds = 0
    if seconds == 0: return "0s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def md5_file_sync(path):
    h = hashlib.md5()
    with open(path, "rb", buffering=1024 * 1024) as f:
        while True:
            chunk = f.read(4 * 1024 * 1024)
            if not chunk: break
            h.update(chunk)
    return h.hexdigest()

def get_mime_type(path):
    return mimetypes.guess_type(path)[0] or "application/octet-stream"

def safe_html(text): return html.escape(str(text), quote=False)

def parse_selection(selection_str, max_idx):
    if selection_str.lower() == "all": return list(range(1, max_idx + 1))
    indices = []
    for part in selection_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-", 1)
                start, end = int(start.strip()), int(end.strip())
                step = 1 if start <= end else -1
                for idx in range(start, end + step, step):
                    if 1 <= idx <= max_idx and idx not in indices: indices.append(idx)
            except: pass
        elif part.isdigit():
            idx = int(part)
            if 1 <= idx <= max_idx and idx not in indices: indices.append(idx)
    return indices

def build_file_tree(files, is_html=True):
    tree = {}
    for i, f in enumerate(files):
        parts = Path(f.path).parts
        current = tree
        for i_part, part in enumerate(parts):
            if i_part == len(parts) - 1:
                current[part] = {'index': i + 1, 'size': f.length}
            else:
                if part not in current: current[part] = {}
                current = current[part]

    def render_tree(node, prefix=""):
        lines = []
        keys = list(node.keys())
        for idx, key in enumerate(keys):
            is_last_item = (idx == len(keys) - 1)
            connector = "└── " if is_last_item else "├── "
            child_prefix = "    " if is_last_item else "│   "
            val = node[key]
            if isinstance(val, dict) and 'index' not in val:
                lines.append(f"{prefix}{connector}📁 {html.escape(key) if is_html else key}")
                lines.extend(render_tree(val, prefix + child_prefix))
            else:
                idx_str = f"[{val['index']}]"
                if is_html: idx_str = f"<code>{idx_str}</code>"
                lines.append(f"{prefix}{connector}📄 {idx_str} {html.escape(key) if is_html else key} ({val['size'] / (1024*1024):.2f} MB)")
        return lines
    return "\n".join(render_tree(tree))

def generate_res_name(original_name, target_res):
    name_without_ext = os.path.splitext(original_name)[0]
    def res_repl(m): return f"{target_res}P" if 'P' in m.group(0) else f"{target_res}p"
    enc_base_name = re.sub(r'(?i)(1080p|720p|480p|2160p|4k|1080)', res_repl, name_without_ext)
    if enc_base_name == name_without_ext: enc_base_name += f"_{target_res}p"
    return enc_base_name

# ============================================================
# MEDIA PROCESSING & WATERMARK
# ============================================================
async def probe_media_streams(filepath):
    cmd = ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", filepath]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
    if proc.returncode != 0: raise RuntimeError(stderr.decode("utf-8", errors="ignore")[:2000])
    return json.loads(stdout.decode("utf-8", errors="ignore"))

async def get_video_meta(filepath):
    try:
        info = await probe_media_streams(filepath)
        duration = int(float(info.get("format", {}).get("duration", 1) or 1))
        width, height = 1280, 720
        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                width = int(stream.get("width", 1280) or 1280)
                height = int(stream.get("height", 720) or 720)
                break
        return max(duration, 1), max(width, 1), max(height, 1)
    except: return 1, 1280, 720

async def apply_mkv_watermark(filepath, watermark=WATERMARK):
    if not filepath.lower().endswith(".mkv"): return filepath
    try:
        probe = await probe_media_streams(filepath)
        cmd = ["mkvpropedit", filepath, "--edit", "info", "--set", f"title={watermark}"]
        v_c, a_c, s_c = 0, 0, 0
        for stream in probe.get("streams", []):
            ctype = stream.get("codec_type")
            lang = stream.get("tags", {}).get("language", "")
            name = f"{watermark} [{lang.upper()}]" if lang else watermark
            if ctype == "video":
                v_c += 1
                cmd.extend(["--edit", f"track:v{v_c}", "--set", f"name={watermark}"])
            elif ctype == "audio":
                a_c += 1
                cmd.extend(["--edit", f"track:a{a_c}", "--set", f"name={name}"])
            elif ctype == "subtitle":
                s_c += 1
                cmd.extend(["--edit", f"track:s{s_c}", "--set", f"name={name}"])
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        await proc.communicate()
    except Exception as e: logger.error(f"Watermark error: {e}")
    return filepath

def subtitle_is_text_codec(codec):
    return codec.lower() in {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text"}

async def prepare_streamable_video(filepath):
    info = await probe_media_streams(filepath)
    streams = info.get("streams", [])
    videos = [s for s in streams if s.get("codec_type") == "video"]
    subtitles = [s for s in streams if s.get("codec_type") == "subtitle"]
    if not videos: return filepath, False, "No video stream"
    video_codec = (videos[0].get("codec_name") or "").lower()
    if video_codec not in {"h264", "hevc", "av1", "mpeg4", "vp9"}:
        return filepath, False, f"Video codec {video_codec} not suitable"
    for s in subtitles:
        if not subtitle_is_text_codec((s.get("codec_name") or "unknown").lower()):
            return filepath, False, "Image subtitle track detected"
    out = os.path.splitext(filepath)[0] + "_stream.mp4"
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", filepath, "-threads", "0", "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-c:v", "copy", "-c:a", "copy", "-c:s", "mov_text", "-movflags", "+faststart", out]
    proc = await asyncio.create_subprocess_exec(*cmd)
    await proc.communicate()
    if proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0: return out, True, "Faststart MP4 created"
    with contextlib.suppress(Exception): os.remove(out)
    return filepath, False, "MP4 remux failed"

async def extract_and_send_subtitles(client, filepath, chat_id, reply_to_message_id, temp_dir, custom_filename=None):
    try:
        info = await probe_media_streams(filepath)
        subtitles = [s for s in info.get("streams", []) if s.get("codec_type") == "subtitle"]
        if not subtitles: return
        ext_map = {"subrip": ".srt", "ass": ".ass", "ssa": ".ssa", "webvtt": ".vtt", "mov_text": ".srt"}
        sub_files = []
        base_name = os.path.splitext(custom_filename or os.path.basename(filepath))[0]
        for sub_index, sub in enumerate(subtitles, start=1):
            codec = (sub.get("codec_name") or "unknown").lower()
            ext = ext_map.get(codec, f".{codec}" if codec != "unknown" else ".srt")
            file_name = f"{base_name} - Subtitle {sub_index}{ext}" if len(subtitles) > 1 else f"{base_name}{ext}"
            out_path = os.path.join(temp_dir, file_name)
            cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", filepath, "-map", f"0:{sub.get('index')}", "-c:s", "copy", out_path]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.communicate()
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0: sub_files.append(out_path)
        for sub_file in sub_files:
            try: await client.send_document(chat_id=chat_id, document=sub_file, reply_to_message_id=reply_to_message_id)
            except FloodWait as e:
                await asyncio.sleep(int(getattr(e, "value", 0) or 0) + 1)
                await client.send_document(chat_id=chat_id, document=sub_file, reply_to_message_id=reply_to_message_id)
    except Exception as e: logger.error(f"Subtitle extract error: {e}")

async def get_thumbnail(filepath):
    thumb_path = filepath + "_thumb.jpg"
    try:
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", "00:00:02", "-i", filepath, "-vf", "scale='min(320,iw)':-2", "-vframes", "1", "-q:v", "5", thumb_path]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await asyncio.wait_for(proc.communicate(), timeout=60)
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0: return thumb_path
    except: pass
    return None

# ============================================================
# PROGRESS UI (Colab Batch Style)
# ============================================================
async def update_ui(message, action, filename, current, total, start_time, last_edit_time, task_id=None, force=False, extra_lines=None, is_encoding=False):
    now = time.time()
    if not force and (now - last_edit_time[0] < UI_INTERVAL) and current < total: return last_edit_time[0]
    last_edit_time[0] = now
    percentage = (current / total) * 100 if total > 0 else 0
    bar_length = 18
    filled = min(bar_length, int(bar_length * percentage / 100))
    bar = "█" * filled + "░" * (bar_length - filled)
    elapsed = max(now - start_time, 0.001)
    speed = current / elapsed
    remaining = max(total - current, 0)
    eta = remaining / speed if speed > 0 else 0

    msg = f"<b>🎬 {safe_html(filename)}</b>\n\n<b>ක්‍රියාවලිය:</b> {safe_html(action)}\n<code>[{bar}] {percentage:.1f}%</code>\n"
    if is_encoding: msg += f"<i>{format_time(current)} / {format_time(total)}</i>\n<b>Speed:</b> {speed:.2f}x\n<b>ETA:</b> {format_time(eta)}\n"
    else: msg += f"<i>{format_bytes(current)} / {format_bytes(total)}</i>\n<b>වේගය:</b> {format_bytes(speed)}/s ({format_mbps(speed)})\n<b>ETA:</b> {format_time(eta)}\n"
    if extra_lines: msg += "\n" + "\n".join(extra_lines) + "\n"
    
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Task", callback_data=f"stop_{task_id}")]]) if task_id else None
    try: await message.edit_text(msg, parse_mode=enums.ParseMode.HTML, reply_markup=markup)
    except: pass
    return last_edit_time[0]

# ============================================================
# ENCODING & UPLOADING ENGINE
# ============================================================
async def encode_video_gpu(input_path, output_path, total_duration, ui_msg, task_id, filename, resolution):
    has_nvenc = check_gpu()
    if resolution == 720: scale_vf, cq_val, crf_val = "scale=-2:'min(720,ih)'", "34", "32"
    else: scale_vf, cq_val, crf_val = "scale=-2:'min(480,ih)'", "38", "36"

    if has_nvenc:
        vcodec = ["-c:v", "h264_nvenc", "-preset", "p6", "-tune", "hq", "-cq", cq_val, "-pix_fmt", "yuv420p"]
        action_name = f"⚙️ Re-Encoding {resolution}p (GPU)"
    else:
        vcodec = ["-c:v", "libx264", "-preset", "veryfast", "-crf", crf_val, "-pix_fmt", "yuv420p"]
        action_name = f"⚙️ Re-Encoding {resolution}p (CPU)"

    cmd = ["ffmpeg", "-y", "-hwaccel", "auto", "-threads", "0", "-i", input_path, "-vf", scale_vf, *vcodec, "-c:a", "copy", "-max_muxing_queue_size", "8192", "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-c:s", "copy", "-progress", "pipe:1", "-nostats", "-loglevel", "error", output_path]
    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    start_time, last_edit_time = time.time(), [0]

    while True:
        if cancel_flags.get(task_id):
            process.terminate()
            raise asyncio.CancelledError("Encoding cancelled.")
        line = await process.stdout.readline()
        if not line: break
        line = line.decode("utf-8").strip()
        if line.startswith("out_time_us="):
            try:
                time_us = int(line.split("=")[1])
                await update_ui(ui_msg, action_name, filename, max(0, time_us / 1_000_000), max(total_duration, 1), start_time, last_edit_time, task_id, force=False, is_encoding=True)
            except: pass
    await process.wait()
    if process.returncode != 0: raise Exception("FFMPEG Failed")
    return os.path.exists(output_path)

async def upload_part(client, fd, file_id, part_no, total_parts, file_size, is_big, cancel_event):
    offset = part_no * PART_SIZE
    size_to_read = min(PART_SIZE, file_size - offset)
    if size_to_read <= 0: return 0
    data = await asyncio.to_thread(os.pread, fd, size_to_read, offset)
    if cancel_event.is_set(): raise asyncio.CancelledError
    
    last_error = None
    for attempt in range(1, PART_RETRIES + 1):
        if cancel_event.is_set(): raise asyncio.CancelledError
        try:
            if is_big: result = await client.invoke(raw.functions.upload.SaveBigFilePart(file_id=file_id, file_part=part_no, file_total_parts=total_parts, bytes=data))
            else: result = await client.invoke(raw.functions.upload.SaveFilePart(file_id=file_id, file_part=part_no, bytes=data))
            if result: return len(data)
        except FloodWait as e:
            wait_seconds = int(getattr(e, "value", 0) or 0)
            await asyncio.sleep(min(wait_seconds, 300) if wait_seconds > 0 else 5)
        except Exception as e:
            last_error = e
            if attempt >= PART_RETRIES: break
            await asyncio.sleep(0.5)
    raise RuntimeError(f"Part {part_no} failed: {last_error}")

async def parallel_upload_file(path, filename, task_id, ui_msg):
    global upload_client
    file_size = os.path.getsize(path)
    is_big = file_size > BIG_FILE_THRESHOLD
    total_parts = math.ceil(file_size / PART_SIZE)
    md5_checksum = await asyncio.to_thread(md5_file_sync, path) if not is_big else None
    worker_count = min(UPLOAD_WORKERS, total_parts)
    cancel_event = asyncio.Event()

    runtime = {"cancel_event": cancel_event, "upload_tasks": set(), "progress_task": None}
    upload_runtime[task_id] = runtime
    file_id = random.randint(-2**63, 2**63 - 1)
    queue = asyncio.Queue()
    for part_no in range(total_parts): queue.put_nowait(part_no)

    state = {"uploaded": 0, "lock": asyncio.Lock(), "last_bytes": 0, "last_time": time.time(), "instant": 0.0}
    start_time, last_edit_time = time.time(), [0]

    async def progress_pump():
        while not cancel_event.is_set():
            await asyncio.sleep(0.5)
            current = state["uploaded"]
            now = time.time()
            dt = max(now - state["last_time"], 0.001)
            state["instant"] = max(0.0, (current - state["last_bytes"]) / dt)
            state["last_bytes"] = current
            state["last_time"] = now
            if current >= file_size: break
            avg = current / max(now - start_time, 0.001)
            await update_ui(ui_msg, "📤 Telegram Upload (MTProto)", filename, current, file_size, start_time, last_edit_time, task_id, extra_lines=[f"<b>⚡ Instant:</b> {format_bytes(state['instant'])}/s", f"<b>AVG:</b> {format_bytes(avg)}/s"])

    runtime["progress_task"] = asyncio.create_task(progress_pump())
    fd = os.open(path, os.O_RDONLY)

    async def worker():
        while not cancel_event.is_set():
            try: part_no = queue.get_nowait()
            except asyncio.QueueEmpty: return
            try:
                sent_bytes = await upload_part(upload_client, fd, file_id, part_no, total_parts, file_size, is_big, cancel_event)
                async with state["lock"]: state["uploaded"] += sent_bytes
            except Exception:
                cancel_event.set()
                raise
            finally: queue.task_done()

    try:
        tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
        runtime["upload_tasks"] = set(tasks)
        await asyncio.gather(*tasks, return_exceptions=True)
        if cancel_event.is_set() and cancel_flags.get(task_id): raise asyncio.CancelledError
        if is_big: input_file = raw.types.InputFileBig(id=file_id, parts=total_parts, name=filename)
        else: input_file = raw.types.InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum=md5_checksum)
        await update_ui(ui_msg, f"✅ Upload complete", filename, file_size, file_size, start_time, last_edit_time, task_id, force=True)
        return input_file
    finally:
        cancel_event.set()
        with contextlib.suppress(Exception): os.close(fd)
        upload_runtime.pop(task_id, None)

async def send_uploaded_media(chat_id, input_file, filename, is_video, duration, width, height, thumb_path, caption):
    sender = upload_client
    thumb = None
    if thumb_path and os.path.exists(thumb_path):
        with contextlib.suppress(Exception): thumb = await sender.save_file(thumb_path)

    if is_video:
        mime_type = "video/mp4" if filename.lower().endswith(".mp4") else "video/x-matroska"
        attributes = [raw.types.DocumentAttributeVideo(duration=int(duration or 1), w=int(width or 1280), h=int(height or 720), supports_streaming=True), raw.types.DocumentAttributeFilename(file_name=filename)]
        media = raw.types.InputMediaUploadedDocument(file=input_file, thumb=thumb, mime_type=mime_type, attributes=attributes, force_file=None)
    else:
        media = raw.types.InputMediaUploadedDocument(file=input_file, thumb=thumb, mime_type=get_mime_type(filename), attributes=[raw.types.DocumentAttributeFilename(file_name=filename)], force_file=True)

    peer = await sender.resolve_peer(chat_id)
    parsed_data = await sender.parser.parse(caption or "", enums.ParseMode.HTML)
    
    try:
        sent_msg = await sender.invoke(raw.functions.messages.SendMedia(peer=peer, media=media, message=parsed_data.get("message", ""), entities=parsed_data.get("entities", None), random_id=sender.rnd_id()))
        
        # Forward to Database Channel
        if str(chat_id).lower() != TARGET_CHANNEL.lower():
            try:
                # Resolve Channel Peer & Send Forward or Copy Request (Using simple bot API here for safety)
                await sender.send_message(TARGET_CHANNEL, f"<b>New Upload:</b> <code>{filename}</code>", parse_mode=enums.ParseMode.HTML)
            except Exception as e:
                logger.error(f"Cannot forward to {TARGET_CHANNEL}: {e}")
        return sent_msg
    except FloodWait as e:
        await asyncio.sleep(int(getattr(e, "value", 0) or 0) + 2)
        return await send_uploaded_media(chat_id, input_file, filename, is_video, duration, width, height, thumb_path, caption)

# ============================================================
# EXECUTION LOGIC (PROCESSOR)
# ============================================================
async def execute_download_upload(client, chat_id, task_id, task_data, ui_msg):
    async with TASK_SEMAPHORE:
        user_id = task_data.get("user_id")
        is_doc_mode = user_settings.get(user_id, True)
        mode = task_data.get("mode", "batch")
        torrent_file_path, temp_dir, files = task_data["torrent_file_path"], task_data["temp_dir"], task_data["files"]
        custom_renames = task_data.get("renames", {})
        download_dir = f"/content/downloads_{task_id}"
        os.makedirs(download_dir, exist_ok=True)

        try:
            for i in task_data["selected_indices"]:
                if cancel_flags.get(task_id): break
                files_to_cleanup = set()
                file_obj = files[i - 1]
                orig_filename = os.path.basename(str(file_obj.path))
                orig_ext = os.path.splitext(orig_filename)[1]
                
                filename = custom_renames[i].strip() if i in custom_renames else orig_filename
                if i in custom_renames and not os.path.splitext(filename)[1]: filename += orig_ext

                # 1. DOWNLOAD
                dl_opts = {"select-file": str(i), "dir": download_dir, "allow-overwrite": "true"}
                active_dl, dl_path = None, None
                try:
                    active_dl = aria2_api.add_torrent(torrent_file_path, options=dl_opts)
                    start_time, last_edit_time = time.time(), [0]
                    while active_dl.status not in ["complete", "error", "removed"]:
                        if cancel_flags.get(task_id):
                            aria2_api.remove([active_dl], force=True, files=False)
                            break
                        await asyncio.sleep(2)
                        active_dl.update()
                        if active_dl.total_length > 0:
                            await update_ui(ui_msg, "📥 Torrent Download", filename, active_dl.completed_length, active_dl.total_length, start_time, last_edit_time, task_id, extra_lines=[f"<b>Speed:</b> {format_bytes(active_dl.download_speed)}/s"])
                    if cancel_flags.get(task_id): break
                    for f in active_dl.files:
                        if getattr(f, "selected", False) and os.path.exists(str(f.path)): dl_path = str(f.path); break
                finally:
                    if active_dl:
                        with contextlib.suppress(Exception): aria2_api.remove([active_dl], force=True, files=False)
                
                if not dl_path: continue
                original_path = dl_path
                files_to_cleanup.add(original_path)

                # Watermark
                original_path = await apply_mkv_watermark(original_path)
                
                is_video = original_path.lower().endswith((".mp4", ".mkv", ".avi", ".webm"))
                duration, width, height = 0, 0, 0

                try:
                    # 2. UPLOAD ORIGINAL
                    if is_video and not is_doc_mode:
                        prepared_path, made_mp4, _ = await prepare_streamable_video(original_path)
                        if made_mp4: filename = os.path.splitext(filename)[0] + ".mp4"
                        dl_path = prepared_path
                        files_to_cleanup.add(prepared_path)

                    thumb_path = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
                    if is_video:
                        duration, width, height = await get_video_meta(dl_path)
                        if not thumb_path:
                            thumb_path = await get_thumbnail(dl_path)
                            if thumb_path: files_to_cleanup.add(thumb_path)

                    input_file = await parallel_upload_file(dl_path, filename, task_id, ui_msg)
                    if cancel_flags.get(task_id): raise asyncio.CancelledError

                    caption = f"<code>{filename}</code>" if i in custom_renames else f"✅ {filename}"
                    await send_uploaded_media(chat_id, input_file, filename, is_video and not is_doc_mode, duration, width, height, thumb_path, caption)

                    if mode == "batch_sub" and is_video:
                        await extract_and_send_subtitles(client, original_path, chat_id, None, temp_dir, custom_filename=filename)

                except asyncio.CancelledError: break
                except Exception as e: logger.error(f"Original Upload error: {e}")

                # 3. RE-ENCODE 720p
                if mode in ["encode_720p", "encode_720p_480p"] and is_video and not cancel_flags.get(task_id):
                    e_name = generate_res_name(filename, 720) + ".mkv"
                    e_path = os.path.splitext(original_path)[0] + "_720p.mkv"
                    files_to_cleanup.add(e_path)
                    try:
                        if await encode_video_gpu(original_path, e_path, duration, ui_msg, task_id, e_name, 720) and not cancel_flags.get(task_id):
                            e_path = await apply_mkv_watermark(e_path)
                            e_dur, e_w, e_h = await get_video_meta(e_path)
                            e_file = await parallel_upload_file(e_path, e_name, task_id, ui_msg)
                            e_thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else await get_thumbnail(e_path)
                            if e_thumb and e_thumb != CUSTOM_THUMB_PATH: files_to_cleanup.add(e_thumb)
                            await send_uploaded_media(chat_id, e_file, e_name, not is_doc_mode, e_dur, e_w, e_h, e_thumb, f"✅ {e_name}")
                    except Exception as e: logger.error(f"720p Encode fail: {e}")

                # 4. RE-ENCODE 480p
                if mode == "encode_720p_480p" and is_video and not cancel_flags.get(task_id):
                    e_name = generate_res_name(filename, 480) + ".mkv"
                    e_path = os.path.splitext(original_path)[0] + "_480p.mkv"
                    files_to_cleanup.add(e_path)
                    try:
                        if await encode_video_gpu(original_path, e_path, duration, ui_msg, task_id, e_name, 480) and not cancel_flags.get(task_id):
                            e_path = await apply_mkv_watermark(e_path)
                            e_dur, e_w, e_h = await get_video_meta(e_path)
                            e_file = await parallel_upload_file(e_path, e_name, task_id, ui_msg)
                            e_thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else await get_thumbnail(e_path)
                            if e_thumb and e_thumb != CUSTOM_THUMB_PATH: files_to_cleanup.add(e_thumb)
                            await send_uploaded_media(chat_id, e_file, e_name, not is_doc_mode, e_dur, e_w, e_h, e_thumb, f"✅ {e_name}")
                    except Exception as e: logger.error(f"480p Encode fail: {e}")

                # Cleanup
                for p in files_to_cleanup:
                    if p and os.path.exists(p) and p != CUSTOM_THUMB_PATH:
                        with contextlib.suppress(Exception): os.remove(p)

            if cancel_flags.get(task_id): await ui_msg.edit_text("🚫 <b>ක්‍රියාවලිය අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)
            else: await ui_msg.edit_text("<b>✅ සියලුම තෝරාගත් files Process කර අවසන්!</b>", parse_mode=enums.ParseMode.HTML)

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
            shutil.rmtree(download_dir, ignore_errors=True)
            current_tasks.pop(task_id, None)
            cancel_flags.pop(task_id, None)


# ============================================================
# HANDLERS
# ============================================================
async def cmd_start(client, message):
    await message.reply_text(
        "⚡ <b>Colab Worker v4.0 is Online! (WebSocket Connected)</b>\n\n"
        "ඔයාට ඕන Magnet Link එකක් මට එවන්න.\n"
        "📦 <b>Modes:</b>\n"
        "• /batch - Normal Batch mode\n"
        "• /batch_sub - Extract & Send Soft Subs\n"
        "• /encode_720p - Encode to 720p & Upload\n"
        "• /encode_720p_480p - Encode 720p + 480p\n"
        "• /thumbnail - Set Custom Thumbnail\n"
        "• /settings - Change Upload Mode (Doc/Streamable)", parse_mode=enums.ParseMode.HTML
    )

async def cmd_settings(client, message):
    user_id = message.from_user.id
    is_doc_mode = user_settings.get(user_id, True)
    mode = "Send the original MKV as a document (Default)" if is_doc_mode else "Streamable MP4 via Telegram"
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Change Mode", callback_data="toggle_mode")]])
    await message.reply_text(f"⚙️ <b>Settings</b>\n\nCurrent: <i>{mode}</i>", reply_markup=markup, parse_mode=enums.ParseMode.HTML)

async def cmd_modes(client, message):
    user_id = message.from_user.id
    mode = message.command[0]
    batch_states[user_id] = {"is_active": True, "mode": mode, "queue": [], "last_msg": await message.reply_text(f"📦 <b>{mode.upper()} Mode ආරම්භ කළා!</b>\n\nLink එකින් එක එවන්න...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="batch_cancel")]]), parse_mode=enums.ParseMode.HTML)}

async def cmd_thumbnail(client, message):
    thumbnail_states[message.from_user.id] = True
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🖼️ View", callback_data="view_thumb"), InlineKeyboardButton("🗑️ Delete", callback_data="del_thumb")], [InlineKeyboardButton("⬅️ Cancel", callback_data="cancel_thumb")]])
    await message.reply_text("🖼 <b>Custom Thumbnail මෙනුව</b>\n\nඅලුත් Thumbnail එකක් දාන්න දැන් Photo එකක් මෙතනට එවන්න.", reply_markup=markup, parse_mode=enums.ParseMode.HTML)

async def handle_photo(client, message):
    user_id = message.from_user.id
    if thumbnail_states.get(user_id, False):
        msg = await message.reply_text("📥 Saving thumbnail...")
        await message.download(CUSTOM_THUMB_PATH)
        thumbnail_states[user_id] = False
        await msg.edit_text("✅ <b>Custom Thumbnail එක Save කරගත්තා!</b>", parse_mode=enums.ParseMode.HTML)

async def handle_magnet(client, message):
    user_id = message.from_user.id
    if user_id not in batch_states or not batch_states[user_id]["is_active"]:
        batch_states[user_id] = {"is_active": True, "mode": "batch", "queue": [], "last_msg": None}
    
    status_msg = await message.reply_text("🔍 Fetching torrent metadata...")
    temp_dir = f"/content/meta_{message.id}"
    os.makedirs(temp_dir, exist_ok=True)
    try:
        cmd = ["aria2c", "--bt-metadata-only=true", "--bt-save-metadata=true", "--dir", temp_dir, message.text.strip()]
        await (await asyncio.create_subprocess_exec(*cmd)).communicate()
        
        t_file = next((os.path.join(temp_dir, n) for n in os.listdir(temp_dir) if n.endswith(".torrent")), None)
        if not t_file: return await status_msg.edit_text("❌ Failed to fetch metadata.")

        meta_dl = aria2_api.add_torrent(t_file, options={"pause": "true"})
        files, t_name = meta_dl.files, meta_dl.name
        with contextlib.suppress(Exception): aria2_api.remove([meta_dl], force=True, files=False)
        
        file_list_html = build_file_tree(files, True)
        prompt_caption = f"✅ <b>ගොනුව හඳුනාගත්තා! (Mode: {batch_states[user_id]['mode'].upper()})</b>\n\n👉 <b>මෙම පණිවිඩයට Reply කරමින්</b> අවශ්‍ය file අංක දෙන්න (උදා: 1,2 ෙහෝ all).\n✏️ Rename: <code>[1] /rename NewName</code>"
        
        prompt_msg = await message.reply_text(f"<b>Files:</b>\n{file_list_html}\n\n{prompt_caption}", parse_mode=enums.ParseMode.HTML)
        task_id = str(prompt_msg.id)
        
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Next Torrent (All Files)", callback_data=f"b_next_{task_id}")], [InlineKeyboardButton("▶️ Start Batch (All Files)", callback_data=f"b_start_{task_id}")], [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{task_id}")]])
        await prompt_msg.edit_reply_markup(markup)
        
        current_tasks[task_id] = {"torrent_file_path": t_file, "temp_dir": temp_dir, "torrent_name": t_name, "files": files, "user_id": user_id, "mode": batch_states[user_id]['mode']}
        await status_msg.delete()
    except Exception as e: await status_msg.edit_text(f"❌ Error: {e}")

async def handle_reply(client, message):
    if not message.reply_to_message: return
    r_id = str(message.reply_to_message.id)
    if r_id not in current_tasks: return

    task_data = current_tasks.pop(r_id)
    text = message.text.strip()
    
    if text.lower() in ["cancel", "stop"]:
        shutil.rmtree(task_data.get("temp_dir", ""), ignore_errors=True)
        return await message.reply_text("🚫 Cancelled.")

    renames, selected = {}, []
    if "/rename" in text.lower():
        for line in text.split('\n'):
            match = re.search(r'^\[?(\d+)\]?.*?/rename\s+(.+)$', line.strip(), re.IGNORECASE)
            if match:
                idx = int(match.group(1))
                selected.append(idx)
                renames[idx] = match.group(2).strip()
    else: selected = parse_selection(text, len(task_data["files"]))

    if not selected: return await message.reply_text("❌ Invalid selection.")

    task_data.update({"renames": renames, "selected_indices": selected, "original_msg_id": int(r_id), "task_id": str(message.id), "selection_str": text})
    user_id = message.from_user.id
    
    if user_id not in batch_states: batch_states[user_id] = {"is_active": True, "mode": "batch", "queue": []}
    batch_states[user_id]["queue"].append(task_data)
    
    q_text = f"📦 <b>Queue:</b>\n" + "\n".join(f"{i+1}. {q['torrent_name']}" for i, q in enumerate(batch_states[user_id]["queue"]))
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Start Batch", callback_data="batch_start")], [InlineKeyboardButton("❌ Cancel Batch", callback_data="batch_cancel")]])
    
    if old := batch_states[user_id].get("last_msg"):
        with contextlib.suppress(Exception): await old.delete()
    batch_states[user_id]["last_msg"] = await client.send_message(message.chat.id, q_text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)

async def master_callback(client, cb):
    data = cb.data
    user_id = cb.from_user.id
    
    if data == "toggle_mode":
        user_settings[user_id] = not user_settings.get(user_id, True)
        mode = "Send the original MKV as a document (Default)" if user_settings[user_id] else "Streamable MP4 via Telegram"
        await cb.message.edit_text(f"⚙️ <b>Settings</b>\n\nCurrent: <i>{mode}</i>", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Change Mode", callback_data="toggle_mode")]]), parse_mode=enums.ParseMode.HTML)
    
    elif data == "view_thumb":
        if os.path.exists(CUSTOM_THUMB_PATH): await cb.message.reply_photo(photo=CUSTOM_THUMB_PATH, caption="🖼️ Custom Thumbnail")
        else: await cb.answer("❌ No thumbnail found.", show_alert=True)
    
    elif data == "del_thumb":
        if os.path.exists(CUSTOM_THUMB_PATH): os.remove(CUSTOM_THUMB_PATH)
        await cb.answer("🗑️ Thumbnail Deleted!", show_alert=True)
    
    elif data == "cancel_thumb":
        thumbnail_states[user_id] = False
        await cb.message.edit_text("🚫 Cancelled.")
        
    elif data == "batch_cancel":
        if user_id in batch_states:
            for q in batch_states[user_id].get("queue", []): shutil.rmtree(q.get("temp_dir", ""), ignore_errors=True)
            del batch_states[user_id]
        await cb.message.edit_text("🚫 Batch Mode cancelled.")
        
    elif data == "batch_start":
        if user_id not in batch_states or not batch_states[user_id]["queue"]: return await cb.message.edit_text("❌ Queue is empty!")
        queue = sorted(batch_states[user_id]["queue"], key=lambda x: x["original_msg_id"])
        del batch_states[user_id]
        await cb.message.edit_text("⏳ <b>Batch Processing Started...</b>", parse_mode=enums.ParseMode.HTML)
        for idx, task_data in enumerate(queue, 1):
            t_id = task_data["task_id"]
            cancel_flags[t_id] = False
            ui_msg = await client.send_message(cb.message.chat.id, f"⏳ <b>Task {idx}/{len(queue)}</b>\n<code>{task_data['torrent_name']}</code>", parse_mode=enums.ParseMode.HTML)
            await execute_download_upload(client, cb.message.chat.id, t_id, task_data, ui_msg)
        await client.send_message(cb.message.chat.id, "✅ <b>Batch Queue Completed!</b>", parse_mode=enums.ParseMode.HTML)
        
    elif data.startswith("b_next_") or data.startswith("b_start_"):
        t_id = data.split("_", 2)[2]
        if t_id not in current_tasks: return await cb.answer("Expired.", show_alert=True)
        task_data = current_tasks.pop(t_id)
        task_data.update({"selected_indices": list(range(1, len(task_data["files"])+1)), "task_id": str(cb.message.id), "renames": {}})
        if user_id not in batch_states: batch_states[user_id] = {"is_active": True, "mode": "batch", "queue": []}
        batch_states[user_id]["queue"].append(task_data)
        with contextlib.suppress(Exception): await cb.message.delete()
        
        if data.startswith("b_start_"):
            msg = await client.send_message(cb.message.chat.id, "⏳ <b>Processing...</b>", parse_mode=enums.ParseMode.HTML)
            cb.message = msg # Mock for start batch logic
            cb.data = "batch_start"
            await master_callback(client, cb)
        else:
            q_text = f"📦 <b>Queue:</b>\n" + "\n".join(f"{i+1}. {q['torrent_name']}" for i, q in enumerate(batch_states[user_id]["queue"]))
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Start Batch", callback_data="batch_start")], [InlineKeyboardButton("❌ Cancel Batch", callback_data="batch_cancel")]])
            if old := batch_states[user_id].get("last_msg"):
                with contextlib.suppress(Exception): await old.delete()
            batch_states[user_id]["last_msg"] = await client.send_message(cb.message.chat.id, q_text, reply_markup=markup, parse_mode=enums.ParseMode.HTML)

    elif data.startswith("cancel_") or data.startswith("stop_"):
        t_id = data.split("_", 1)[1]
        cancel_flags[t_id] = True
        if runtime := upload_runtime.get(t_id):
            runtime["cancel_event"].set()
            for t in runtime.get("upload_tasks", set()): t.cancel()
        if task := current_tasks.pop(t_id, None): shutil.rmtree(task.get("temp_dir", ""), ignore_errors=True)
        await cb.message.edit_text("🚫 <b>Cancelled.</b>", parse_mode=enums.ParseMode.HTML)

# ============================================================
# WEBSOCKET & MAIN APP LAUNCHER
# ============================================================
async def heartbeat_loop(ws):
    try:
        while True:
            await asyncio.sleep(10)
            await ws.send("ping")
            await ws.recv()
    except websockets.exceptions.ConnectionClosed: logger.warning("Master server disconnected!")

async def main():
    global app, upload_client, aria2_api
    
    aria2_api = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
    subprocess.Popen(["aria2c", "--enable-rpc=true", "--rpc-listen-all=false", "--rpc-listen-port=6800", "--daemon=true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    key = str(random.randint(100000, 999999))
    print("="*40)
    print(f"🔑 YOUR CONNECTION KEY IS: {key}")
    print("="*40)
    
    try:
        async with websockets.connect(MASTER_WS_URL) as ws:
            await ws.send(key)
            response = await ws.recv()
            data = json.loads(response)
            
            if data.get("status") != "AUTHORIZED": return
            
            api_id, api_hash, bot_token = data.get("API_ID"), data.get("API_HASH"), data.get("BOT_TOKEN")
            
            # Using wzgram with fast MTProto settings
            app = Client("colab_worker", api_id=api_id, api_hash=api_hash, bot_token=bot_token, in_memory=True, workers=32, max_concurrent_transmissions=UPLOAD_WORKERS)
            upload_client = app
            
            # Register Handlers
            app.add_handler(MessageHandler(cmd_start, filters.command("start")))
            app.add_handler(MessageHandler(cmd_settings, filters.command("settings")))
            app.add_handler(MessageHandler(cmd_thumbnail, filters.command("thumbnail")))
            app.add_handler(MessageHandler(cmd_modes, filters.command(["batch", "batch_sub", "encode_720p", "encode_720p_480p"])))
            app.add_handler(MessageHandler(handle_magnet, filters.regex(r"^magnet:\?xt=")))
            app.add_handler(MessageHandler(handle_reply, filters.reply & filters.text))
            app.add_handler(MessageHandler(handle_photo, filters.photo & filters.private))
            app.add_handler(CallbackQueryHandler(master_callback))
            
            asyncio.create_task(heartbeat_loop(ws))
            
            await app.start()
            logger.info("✅ Colab Worker v4.0 ONLINE (WebSocket + Batch UI + Parallel Uploads)")
            await ws.wait_closed()
            
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
    finally:
        if app and app.is_connected: await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
