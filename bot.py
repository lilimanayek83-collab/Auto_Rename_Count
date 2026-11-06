
#bot02_simplified.py
# ==================== IMPORTS & CONFIG ====================
import asyncio
import sys

if sys.version_info >= (3, 10):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

import os
import re
import time
import math
import logging
import datetime
import shutil
import subprocess
import heapq
import unicodedata
import inspect
import glob
from functools import lru_cache
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
from PIL import Image
import motor.motor_asyncio
from pymongo import ReturnDocument
from pyrogram import Client, filters, __version__, idle, ContinuePropagation
from pyrogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup
)
from pyrogram.errors import FloodWait, BadRequest
from pyrogram.enums import ChatMemberStatus, ParseMode

# Attempt to speed up Pyrogram with tgcrypto
try:
    import tgcrypto
except ImportError:
    tgcrypto = None

load_dotenv()

class Config:
    API_ID = int(os.getenv("API_ID", ""))
    API_HASH = os.getenv("API_HASH", "")
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    ADMIN = [int(admin) for admin in os.getenv("ADMIN", "").split(",")]
    DB_URL = os.getenv("DB_URL", " ")
    DB_NAME = os.getenv("DB_NAME", "")
    LOG_CHANNEL = int(os.getenv("LOG_CHANNEL", ""))
    START_PIC = os.getenv("START_PIC", "")
    WEBHOOK = os.getenv("WEBHOOK", "").lower() == "true"
    PORT = int(os.getenv("PORT", ""))
    BOT_UPTIME = time.time()
    TIMEOUT_SECONDS = 0
    MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024

class Txt:
    START_TXT = """<b>ʜᴇʏ! {}  

» ɪ ᴀᴍ ᴀᴅᴠᴀɴᴄᴇᴅ ʀᴇɴᴀᴍᴇ ʙᴏᴛ! ᴡʜɪᴄʜ ᴄᴀɴ ᴀᴜᴛᴏʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ғɪʟᴇs ᴡɪᴛʜ ᴄᴜsᴛᴏᴍ ᴄᴀᴘᴛɪᴏɴ ᴀɴᴅ ᴛʜᴜᴍʙɴᴀɪʟ ᴀɴᴅ ᴀʟsᴏ sᴇǫᴜᴇɴᴄᴇ ᴛʜᴇᴍ ᴘᴇʀғᴇᴄᴛʟʏ</b>"""

    HELP_TXT = """<b>📚 Available Commands:</b>

<b>⚙️ Setup Commands:</b>
• /autorename [format] - Set auto rename format
• /count [number] - View or set the starting {count} value
• /set_caption [caption] - Set custom caption
• /clear_caption - Reset caption to default (full renamed filename with extension)
• /settitle [title] - Set metadata title
• /setauthor [author] - Set metadata author
• /setartist [artist] - Set metadata artist
• /setaudio [audio] - Set audio metadata
• /setsubtitle [subtitle] - Set subtitle metadata
• /setvideo [video] - Set video metadata
• /setallmeta [text] - Set ALL metadata fields to same value
• /set_thumbnail - Set thumbnail from replied photo
• /view_thumbnail - View your thumbnail
• /delete_thumbnail - Delete thumbnail
• /set_cover - Reply to a photo to save it as your full-quality cover image (stored in the cover channel)
• /view_cover - View your saved cover image
• /delete_cover - Remove your saved cover image
• /delete_metadata - Delete metadata settings
• /mediatype - Choose output media type (Document/Video/Audio) – with buttons!
• /add_pattern [pattern] - Save a custom pattern, e.g. S{season} -  E{episode} [{quality} ⌯
• /patterns - List your saved patterns
• /del_pattern [n|all] - Remove saved pattern(s)
• /identify_from [filename|caption] - Where to read season/episode/quality from
• Wrap text in &lt; &gt; inside /autorename or /set_caption to make it <b>bold</b> in the caption, e.g. <code>&lt;Episode: {episode}&gt;</code>

<b>📊 View Commands:</b>
• /view_caption - View your caption
• /view_thumb - View your thumbnail
• /showmetadata - Show metadata settings
• /queue_stats - Check processing queue status
• /queue - Check your position in queue
• /failed_queue - View your files pending manual rename

<b>⚡ Control Commands:</b>
• /metadata - Open metadata settings interface
• /stop_renaming - Pause renaming queue (Admin)
• /start_renaming - Resume renaming queue (Admin)
• /admin_priority_on - Enable admin priority (Admin)
• /admin_priority_off - Disable admin priority (Admin)
• /clear_queue_user - Clear your own queued files (Admins can add a user ID)
• /skip_failed - Skip current manual rename request

<b>🔄 Sequence Sorting:</b>
• /ssequence - Start sequence mode (files are saved, NOT processed)
• /esequence - End sequence mode and choose sort mode (3 modes)
• /sequence_mode [1|2|3] - Set default sort mode
  Mode 1: Season → Quality → Episode
  Mode 2: Season → Episode → Quality
  Mode 3: Quality → Season → Episode

<b>👑 Admin Commands:</b>
• /stats - Bot statistics
• /clear_queue - Stop current task + clear ALL users' queues
• /broadcast - Broadcast message
• /set_channel - Register the cover/thumbnail storage channel (forward a message from it after making the bot admin there)
• /add_pattern_channel - Send INSIDE a channel (bot = admin) to register the pattern storage channel

<b>📖 Guide:</b>
1. First use setup commands
2. Send any file to auto rename it
3. Check queue status with /queue_stats
4. Customize with metadata commands
5. If a file can't extract season/episode/quality, it goes to failed queue
6. After all files done, bot asks you to manually provide the filename

<b>📝 Variables for Format:</b>
• {filename} - Original filename
• {season} - Season number
• {episode} - Episode number
• {quality} - Video quality
• {filesize} - File size
• {duration} - Duration
• {count} - Auto-incrementing counter (set starting value with /count). Cannot be combined with {season}/{episode}/{quality} in the same format.

<b>Example:</b>
<code>/autorename {filename} [S{season}E{episode}] - {quality}</code>
<code>/autorename Episode {count} - {filename}</code>"""

sequence_sessions = {}
failed_rename_queue: Dict[int, List[dict]] = {}
manual_rename_waiting: Dict[int, dict] = {}
renaming_paused = False
set_channel_waiting: Dict[int, bool] = {}

_QUALITY_LABELS = {4: '4K', 3: '2K', 2: '1080p', 1: '720p', 0: '480p'}

def quality_label(q: int) -> str:
    return _QUALITY_LABELS.get(q, 'HD')

def get_msg_fname(msg: Message) -> str:
    if msg.document: return msg.document.file_name or 'file'
    if msg.video:    return msg.video.file_name    or 'video.mp4'
    if msg.audio:    return msg.audio.file_name    or 'audio.mp3'
    return 'file'

def sort_key_for_mode(msg: Message, mode: int, source: str = 'filename', patterns: list = None):
    i = extract_variables(msg, get_msg_fname(msg), source, patterns or [])
    season, quality, episode = i['season'], quality_to_int(i['quality']), i['episode']
    if   mode == 1: return (season, quality, episode)
    elif mode == 2: return (season, episode, quality)
    else:           return (quality, season, episode)

def generate_sort_summary(sorted_files: list, mode: int, source: str = 'filename', patterns: list = None) -> str:
    if not sorted_files:
        return "No files."
    def info(m):
        i = extract_variables(m, get_msg_fname(m), source, patterns or [])
        return i['season'], quality_to_int(i['quality']), i['episode']
    lines: List[str] = []
    if mode == 1:
        lines.append("🔀 **Sorted: Season › Quality › Episode**\n")
        last_s = last_q = None
        for msg in sorted_files:
            s, q, e = info(msg)
            if s != last_s or q != last_q:
                lines.append(f"\n**S{s:02d} {quality_label(q)}:**")
            lines.append(f"  Ep{e:02d} - `{get_msg_fname(msg)[:40]}`")
            last_s, last_q = s, q
    elif mode == 2:
        lines.append("🔀 **Sorted: Season › Episode › Quality**\n")
        last_s = last_e = None
        for msg in sorted_files:
            s, q, e = info(msg)
            if s != last_s or e != last_e:
                lines.append(f"\n**S{s:02d} Ep{e:02d}:**")
            lines.append(f"  {quality_label(q)} - `{get_msg_fname(msg)[:40]}`")
            last_s, last_e = s, e
    else:
        lines.append("🔀 **Sorted: Quality › Season › Episode**\n")
        last_q = last_s = None
        for msg in sorted_files:
            s, q, e = info(msg)
            if q != last_q or s != last_s:
                lines.append(f"\n**{quality_label(q)} - S{s:02d}:**")
            lines.append(f"  Ep{e:02d} - `{get_msg_fname(msg)[:40]}`")
            last_q, last_s = q, s
    return "\n".join(lines)

def _get_failed_queue(user_id: int) -> List[dict]:
    if user_id not in failed_rename_queue:
        failed_rename_queue[user_id] = []
    return failed_rename_queue[user_id]

def _add_to_failed_queue(user_id: int, failed_item: dict):
    _get_failed_queue(user_id).append(failed_item)

def _has_pending_queue_tasks(user_id: int) -> bool:
    queue_info = processing_queue.get_queue_info()
    for task in queue_info['waiting_list']:
        if task['user_id'] == user_id:
            return True
    if queue_info['is_processing'] and queue_info['current']:
        if queue_info['current']['user_id'] == user_id:
            return True
    return False

async def _trigger_manual_rename_if_ready(user_id: int):
    if user_id in manual_rename_waiting:
        return
    failed = _get_failed_queue(user_id)
    if not failed:
        return
    if _has_pending_queue_tasks(user_id):
        return
    await _prompt_next_manual_rename(user_id)

async def _prompt_next_manual_rename(user_id: int):
    failed = _get_failed_queue(user_id)
    if not failed:
        return
    item = failed[0]
    manual_rename_waiting[user_id] = {'failed_item': item, 'awaiting': True}
    original_name = item.get('original_file_name', 'Unknown')
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭️ Skip This File", callback_data=f"skip_manual_{user_id}")],
        [InlineKeyboardButton("❌ Cancel All Manual Renames", callback_data=f"cancel_manual_{user_id}")]
    ])
    try:
        await app.send_message(
            chat_id=user_id,
            text=(
                f"Select The Output File Type File Name :- `{original_name}`\n\n"
                f"Please Enter New Filename...\n\n"
                f"Old File Name :- `{original_name}`"
            ),
            reply_markup=buttons
        )
    except Exception as e:
        print(f"Error prompting manual rename for user {user_id}: {e}")

class PriorityQueue:
    """
    Ordering guarantee:
    Items are popped in ascending (priority, message_id, task_counter) order.

    IMPORTANT: message_id (not time.time()) is used as the primary tie-breaker
    within the same priority tier. message_id is assigned by Telegram's servers
    in true send order, so sorting by it guarantees files come out in the exact
    order the user sent them -- even if two async handlers (workers=10) happen
    to reach add_to_queue() in a different order than the files were actually
    sent (e.g. due to an `await` in between receiving the update and enqueuing
    it). Using time.time() here would NOT give that guarantee, since wall-clock
    read order can differ from Telegram's true send order under concurrency.

    task_counter remains as a final tie-breaker only to keep heapq comparisons
    deterministic in the rare case two messages somehow share a message_id
    (should not happen in practice, since message_id is unique per chat).
    """
    def __init__(self):
        self.queue = []
        self.task_counter = 0
        self.current_task = None
        self.current_task_id = None
        self.is_processing = False
        self.paused_tasks = []
        self.admin_priority_mode = False
        self.lock = asyncio.Lock()
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.task_start_time = None
        self.admin_mode_active = False
        self.timeout_seconds = Config.TIMEOUT_SECONDS
        self.user_tasks = {}
        self.task_map = {}
        self.current_job = None      # asyncio.Task running process_queue_file
        self.cancelled_tasks = 0

    def _base_item(self, message: Message, user_id: int, manual_filename: Optional[str], task_id: str, queue_position: int):
        priority = 0 if user_id in Config.ADMIN else 1
        queue_item = {
            'task_id': task_id,
            'message_id': message.id,
            'chat_id': message.chat.id,
            'user_id': user_id,
            'file_name': '',
            'file_size': 0,
            'media_type': '',
            'added_time': time.time(),
            'status': 'waiting',
            'priority': priority,
            'is_admin': user_id in Config.ADMIN,
            'queue_position': queue_position,
            'completed': False,
            'success': False,
            'cancelled': False,
            'manual_filename': manual_filename,
        }
        if message.document:
            queue_item['file_name'] = message.document.file_name or "file"
            queue_item['file_size'] = message.document.file_size
            queue_item['media_type'] = 'document'
        elif message.video:
            queue_item['file_name'] = message.video.file_name or "video.mp4"
            queue_item['file_size'] = message.video.file_size
            queue_item['media_type'] = 'video'
        elif message.audio:
            queue_item['file_name'] = message.audio.file_name or "audio.mp3"
            queue_item['file_size'] = message.audio.file_size
            queue_item['media_type'] = 'audio'
        return queue_item

    def add_to_queue(self, message: Message, user_id: int):
        task_id = f"{user_id}_{int(time.time())}_{self.task_counter}"
        priority = 0 if user_id in Config.ADMIN else 1
        queue_position = len(self.queue) + 1
        queue_item = self._base_item(message, user_id, None, task_id, queue_position)
        # Use message.id (true Telegram send-order) instead of time.time()
        # so files always come out in the exact order they were sent,
        # regardless of async handler race conditions.
        heapq.heappush(self.queue, (priority, message.id, self.task_counter, queue_item))
        self.task_counter += 1
        self.task_map[task_id] = queue_item
        self.user_tasks.setdefault(user_id, []).append(task_id)
        return queue_position, task_id, queue_item

    def add_manual_task_to_queue(self, message: Message, user_id: int, manual_filename: str):
        # Manual renames are queued in the order the USER resolved them
        # (typed the filename), not the original file's send order --
        # so time.time() here is intentional and correct, reflecting
        # resolution order rather than original send order.
        task_id = f"{user_id}_manual_{int(time.time())}_{self.task_counter}"
        priority = 0 if user_id in Config.ADMIN else 1
        queue_position = len(self.queue) + 1
        queue_item = self._base_item(message, user_id, manual_filename, task_id, queue_position)
        heapq.heappush(self.queue, (priority, time.time(), self.task_counter, queue_item))
        self.task_counter += 1
        self.task_map[task_id] = queue_item
        self.user_tasks.setdefault(user_id, []).append(task_id)
        return queue_position, task_id, queue_item

    def get_next_task(self):
        while self.queue:
            priority, sort_key, counter, task = heapq.heappop(self.queue)
            if task.get('cancelled', False):
                uid = task['user_id']
                tid = task['task_id']
                if uid in self.user_tasks and tid in self.user_tasks[uid]:
                    self.user_tasks[uid].remove(tid)
                    if not self.user_tasks[uid]:
                        del self.user_tasks[uid]
                if tid in self.task_map:
                    del self.task_map[tid]
                continue
            return task
        return None

    def get_queue_length(self):
        return len([item for item in self.queue if not item[3].get('cancelled', False)])

    def clear_queue(self):
        self.queue.clear()
        self.user_tasks.clear()
        self.task_map.clear()

    async def clear_user_queue(self, user_id: int) -> Tuple[int, bool]:
        # NOTE: no `self.lock` here. queue_worker holds that lock for the whole
        # duration of a file, so taking it here made the command hang until
        # the current file finished. This body has no awaits -> already atomic.
        removed_count = 0
        has_current = bool(
            self.is_processing and self.current_task and self.current_task['user_id'] == user_id
        )
        for task_id in list(self.user_tasks.get(user_id, [])):
            task = self.task_map.get(task_id)
            if task and task.get('status') == 'waiting' and not task.get('cancelled', False):
                task['cancelled'] = True
                removed_count += 1
        if has_current:
            self.user_tasks[user_id] = [self.current_task_id]
        else:
            self.user_tasks.pop(user_id, None)
        return removed_count, has_current

    def cancel_current(self) -> bool:
        job = self.current_job
        if job and not job.done():
            job.cancel()
            return True
        return False

    def cancel_task(self, task_id: str) -> str:
        """'running' | 'queued' | 'gone'"""
        if self.is_processing and self.current_task_id == task_id:
            return 'running' if self.cancel_current() else 'gone'
        task = self.task_map.get(task_id)
        if task and task.get('status') == 'waiting' and not task.get('cancelled', False):
            task['cancelled'] = True
            return 'queued'
        return 'gone'

    def clear_all(self):
        """Admin /clear_queue: drop every waiting task of every user AND
        cancel the running job. Returns (affected_user_ids, removed_waiting, stopped_current)."""
        users, removed = set(), 0
        for item in self.queue:
            t = item[3]
            if not t.get('cancelled', False):
                removed += 1
                users.add(t['user_id'])
        if self.is_processing and self.current_task:
            users.add(self.current_task['user_id'])
        self.clear_queue()
        stopped = self.cancel_current()
        return users, removed, stopped

    def forget_task(self, task):
        """Called when a task finished/cancelled, so finished tasks don't pile up."""
        tid, uid = task['task_id'], task['user_id']
        self.task_map.pop(tid, None)
        lst = self.user_tasks.get(uid)
        if lst and tid in lst:
            lst.remove(tid)
            if not lst:
                del self.user_tasks[uid]

    def get_queue_info(self):
        waiting_tasks = [item for item in self.queue if not item[3].get('cancelled', False)]
        total_waiting = len(waiting_tasks)
        sorted_queue = sorted(waiting_tasks, key=lambda x: (x[0], x[1]))
        info = {
            'total': total_waiting,
            'current': self.current_task,
            'is_processing': self.is_processing,
            'completed': self.completed_tasks,
            'failed': self.failed_tasks,
            'paused': len(self.paused_tasks),
            'admin_priority': self.admin_priority_mode,
            'admin_mode': self.admin_mode_active,
            'waiting_list': [],
            'admin_waiting': 0,
            'user_waiting': 0,
            'user_stats': {}
        }
        admin_count = 0
        for i, (priority, sort_key, counter, item) in enumerate(sorted_queue):
            info['waiting_list'].append({
                'position': i + 1,
                'task_id': item['task_id'],
                'file_name': item['file_name'][:50] if item['file_name'] else 'Unknown',
                'user_id': item['user_id'],
                'is_admin': item.get('is_admin', False),
                'priority': 'High' if priority == 0 else 'Normal',
                'added_time': item.get('added_time', 0),
                'waiting_time': time.time() - item['added_time']
            })
            if item.get('is_admin', False):
                admin_count += 1
        info['admin_waiting'] = admin_count
        info['user_waiting'] = total_waiting - admin_count
        user_stats = {}
        for _, _, _, item in waiting_tasks:
            uid = item['user_id']
            user_stats[uid] = user_stats.get(uid, 0) + 1
        info['user_stats'] = user_stats
        return info

    def check_timeout(self):
        if self.is_processing and self.task_start_time:
            elapsed = time.time() - self.task_start_time
            return elapsed > self.timeout_seconds
        return False

    def mark_task_completed(self, task_id, success=True):
        if self.current_task and self.current_task['task_id'] == task_id:
            self.current_task['completed'] = True
            self.current_task['success'] = success
            self.current_task['completed_time'] = time.time()

processing_queue = PriorityQueue()

def _cancel_kb(task_id=None):
    tid = task_id or processing_queue.current_task_id
    if not tid:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_task_{tid}")]])

class Database:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(Config.DB_URL)
        self.db = self.client[Config.DB_NAME]
        self.col = self.db.users
        self.config_col = self.db.bot_config

    async def init_db(self):
        # No group allow-list to load anymore; kept as a no-op hook.
        pass

    def new_user(self, user_id):
        return {
            "_id": int(user_id),
            "join_date": datetime.now().isoformat(),
            "file_id": None,
            "caption": None,
            "metadata": True,
            "title": "Encoded by @AnimeMultiDub",
            "author": "@AnimeMultiDub",
            "artist": "@AnimeMultiDub",
            "audio": "By @AnimeMultiDub",
            "subtitle": "By @AnimeMultiDub",
            "video": "Encoded By @AnimeMultiDub",
            "format_template": None,
            "media_type": "document",
            "ban_status": {
                "is_banned": False,
                "ban_duration": 0,
                "banned_on": datetime.max.isoformat(),
                "ban_reason": ''
            },
            "sequence_mode": 1,
            "count": None,
            "patterns": [],
            "id_source": "filename",
        }

    async def add_user(self, user_id):
        try:
            await self.col.update_one(
                {"_id": int(user_id)},
                {"$setOnInsert": self.new_user(user_id)},
                upsert=True
            )
        except Exception as e:
            print(f"add_user error (ignored): {e}")

    async def is_user_exist(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return bool(user)

    async def total_users_count(self):
        return await self.col.count_documents({})

    async def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.col.delete_many({"_id": int(user_id)})

    async def set_thumbnail(self, user_id, file_id):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"file_id": file_id}})

    async def get_thumbnail(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("file_id", None) if user else None

    async def set_caption(self, user_id, caption):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"caption": caption}})

    async def get_caption(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("caption", None) if user else None

    async def set_format_template(self, user_id, format_template):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"format_template": format_template}})

    async def get_format_template(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("format_template", None) if user else None

    async def set_media_preference(self, user_id, media_type):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"media_type": media_type}})

    async def get_media_preference(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("media_type", "document") if user else "document"

    async def get_metadata(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("metadata", True) if user else True

    async def set_metadata(self, user_id, metadata):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"metadata": metadata}})

    async def get_title(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("title", "Encoded by @AnimeMultiDub") if user else "Encoded by @AnimeMultiDub"

    async def set_title(self, user_id, title):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"title": title}})

    async def get_author(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("author", "@AnimeMultiDub") if user else "@AnimeMultiDub"

    async def set_author(self, user_id, author):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"author": author}})

    async def get_artist(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("artist", "@AnimeMultiDub") if user else "@AnimeMultiDub"

    async def set_artist(self, user_id, artist):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"artist": artist}})

    async def get_audio(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("audio", "By @AnimeMultiDub") if user else "By @AnimeMultiDub"

    async def set_audio(self, user_id, audio):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"audio": audio}})

    async def get_subtitle(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("subtitle", "By @AnimeMultiDub") if user else "By @AnimeMultiDub"

    async def set_subtitle(self, user_id, subtitle):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"subtitle": subtitle}})

    async def get_video(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("video", "Encoded By @AnimeMultiDub") if user else "Encoded By @AnimeMultiDub"

    async def set_video(self, user_id, video):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"video": video}})

    async def get_sequence_mode(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("sequence_mode", 1) if user else 1

    async def set_sequence_mode(self, user_id, mode: int):
        await self.col.update_one({"_id": int(user_id)}, {"$set": {"sequence_mode": mode}})

    async def get_count(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        return user.get("count", None) if user else None

    async def set_count(self, user_id, value: int):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$set": {"count": int(value)}},
            upsert=True
        )

    async def increment_count(self, user_id):
        """Atomically increments count by 1 and returns the NEW value."""
        result = await self.col.find_one_and_update(
            {"_id": int(user_id)},
            {"$inc": {"count": 1}},
            return_document=ReturnDocument.AFTER
        )
        return result.get("count") if result else None

    # ---- Custom filename patterns + variable identify source ----
    async def get_patterns(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)}, {"patterns": 1})
        return (user or {}).get("patterns") or []

    async def add_pattern(self, user_id, pattern):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$addToSet": {"patterns": pattern}},
            upsert=True
        )

    async def set_patterns(self, user_id, patterns: list):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$set": {"patterns": patterns}},
            upsert=True
        )

    async def get_id_source(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)}, {"id_source": 1})
        return (user or {}).get("id_source") or "filename"

    async def set_id_source(self, user_id, source: str):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$set": {"id_source": source}},
            upsert=True
        )

    # ---- Pattern storage channel (bot-wide) + per-user message reference ----
    async def get_pattern_channel(self):
        cfg = await self.config_col.find_one({"_id": "pattern_channel"})
        return cfg.get("chat_id") if cfg else None

    async def set_pattern_channel(self, chat_id):
        await self.config_col.update_one(
            {"_id": "pattern_channel"}, {"$set": {"chat_id": chat_id}}, upsert=True
        )

    async def get_pattern_ref(self, user_id):
        user = await self.col.find_one(
            {"_id": int(user_id)}, {"pattern_chat_id": 1, "pattern_message_id": 1}
        )
        if user and user.get("pattern_chat_id") and user.get("pattern_message_id"):
            return {"chat_id": user["pattern_chat_id"], "message_id": user["pattern_message_id"]}
        return None

    async def set_pattern_ref(self, user_id, chat_id, message_id):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$set": {"pattern_chat_id": chat_id, "pattern_message_id": message_id}},
            upsert=True
        )

    async def clear_pattern_ref(self, user_id):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$unset": {"pattern_chat_id": "", "pattern_message_id": ""}}
        )

    # ---- Cover / thumbnail storage channel (bot-wide config) ----
    async def get_cover_channel(self):
        cfg = await self.config_col.find_one({"_id": "cover_channel"})
        return cfg.get("chat_id") if cfg else None

    async def set_cover_channel(self, chat_id):
        await self.config_col.update_one(
            {"_id": "cover_channel"},
            {"$set": {"chat_id": chat_id}},
            upsert=True
        )

    # ---- Per-user cover image reference (stored as a channel message) ----
    async def set_cover_message(self, user_id, chat_id, message_id):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$set": {"cover_channel_id": chat_id, "cover_message_id": message_id}},
            upsert=True
        )

    async def get_cover_message(self, user_id):
        user = await self.col.find_one({"_id": int(user_id)})
        if user and user.get("cover_message_id") and user.get("cover_channel_id"):
            return {"chat_id": user["cover_channel_id"], "message_id": user["cover_message_id"]}
        return None

    async def delete_cover_message(self, user_id):
        await self.col.update_one(
            {"_id": int(user_id)},
            {"$unset": {"cover_channel_id": "", "cover_message_id": ""}}
        )

db = Database()

def humanbytes(size):
    if not size:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def TimeFormatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "ᴅ, ") if days else "") + \
          ((str(hours) + "ʜ, ") if hours else "") + \
          ((str(minutes) + "ᴍ, ") if minutes else "") + \
          ((str(seconds) + "ꜱ, ") if seconds else "")
    return tmp[:-2] or "0 s"

async def progress_for_pyrogram(current, total, ud_type, message, start):
    now = time.time()
    diff = now - start
    if round(diff % 10.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff if diff > 0 else 0
        elapsed_time = round(diff) * 1000
        time_to_completion = round((total - current) / speed) * 1000 if speed > 0 else 0
        estimated_total_time = elapsed_time + time_to_completion
        estimated_total_time = TimeFormatter(milliseconds=time_to_completion)
        progress = "{0}{1}".format(
            ''.join(["█" for _ in range(math.floor(percentage / 5))]),
            ''.join(["░" for _ in range(20 - math.floor(percentage / 5))])
        )
        tmp = f"""\n
<b>» Size</b> : {humanbytes(current)} | {humanbytes(total)}
<b>» Done</b> : {round(percentage, 2)}%
<b>» Speed</b> : {humanbytes(speed)}/s
<b>» ETA</b> : {estimated_total_time if estimated_total_time else "0 s"} """
        try:
            if message and hasattr(message, 'edit'):
                await message.edit(text=f"{ud_type}\n\n{progress}{tmp}", reply_markup=_cancel_kb())
        except:
            pass

async def safe_send_message(chat_id, text, reply_to_message_id=None, retry_count=3):
    for attempt in range(retry_count):
        try:
            return await app.send_message(
                chat_id=chat_id,
                text=text,
                reply_to_message_id=reply_to_message_id
            )
        except FloodWait as e:
            if attempt < retry_count - 1:
                await asyncio.sleep(e.value)
                continue
            else:
                raise
        except Exception as e:
            print(f"Error sending message: {e}")
            if attempt < retry_count - 1:
                await asyncio.sleep(2)
                continue
            else:
                raise

async def cleanup_files(*paths):
    for path in paths:
        try:
            if path and os.path.exists(path):
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
        except Exception as e:
            print(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    """
    Prepares a Bot-API-COMPLIANT small thumbnail: JPEG, <=320x320, <=200KB.
    This is what Telegram allows for the `thumb` parameter on
    send_document/send_video/send_audio -- it is NOT the cover image, and
    Telegram enforces these limits server-side regardless of what we upload,
    so there is no way to make `thumb` itself high quality. Use
    process_cover_image() below for a genuine full-quality video cover.
    """
    if not thumb_path or not os.path.exists(thumb_path):
        return None
    try:
        with Image.open(thumb_path) as img:
            if img.mode != 'RGB':
                img = img.convert('RGB')
            img.thumbnail((320, 320))
            img.save(thumb_path, "JPEG", quality=85)
        return thumb_path
    except Exception as e:
        print(f"Thumbnail processing error: {e}")
        await cleanup_files(thumb_path)
        return None

async def process_cover_image(cover_path):
    """
    Prepares a FULL-QUALITY cover image for send_video()'s `cover` parameter
    (Bot API 7.9+ / pyrofork). Unlike the `thumb` param, `cover` is uploaded
    to Telegram as a real Photo and is NOT limited to 200KB/320px -- it just
    needs to respect Telegram's normal photo limits (JPEG/PNG, max dimension
    10000px, width+height ratio <= 20, under ~10MB). We only downscale if the
    source image is absurdly large, we never force it down to thumbnail size.
    """
    if not cover_path or not os.path.exists(cover_path):
        return None
    try:
        MAX_DIM = 4096  # generous ceiling, well within Telegram's photo limits
        MAX_BYTES = 10 * 1024 * 1024
        with Image.open(cover_path) as img:
            if img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')
            elif img.mode == 'L':
                img = img.convert('RGB')
            if img.width > MAX_DIM or img.height > MAX_DIM:
                img.thumbnail((MAX_DIM, MAX_DIM))
            # Save at high quality first
            img.save(cover_path, "JPEG", quality=95)
        # If still too large in bytes (rare, huge originals), step quality down
        quality = 90
        while os.path.getsize(cover_path) > MAX_BYTES and quality > 40:
            with Image.open(cover_path) as img:
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                img.save(cover_path, "JPEG", quality=quality)
            quality -= 10
        return cover_path
    except Exception as e:
        print(f"Cover image processing error: {e}")
        await cleanup_files(cover_path)
        return None

async def add_metadata_correct(input_path, output_path, user_id):
    ffmpeg_path = None
    for path in ['ffmpeg', '/usr/bin/ffmpeg', '/usr/local/bin/ffmpeg', '/bin/ffmpeg']:
        if shutil.which(path):
            ffmpeg_path = path
            break
    if not ffmpeg_path:
        raise RuntimeError("FFmpeg not found.")
    title = await db.get_title(user_id)
    artist = await db.get_artist(user_id)
    author = await db.get_author(user_id)
    video_title = await db.get_video(user_id)
    audio_title = await db.get_audio(user_id)
    subtitle_title = await db.get_subtitle(user_id)
    def escape_metadata(text):
        return text.replace('"', '\\"').replace("'", "\\'")
    cmd = [
        ffmpeg_path,
        '-i', input_path,
        '-map', '0',
        '-c:v', 'copy',
        '-c:a', 'copy',
        '-c:s', 'copy',
        '-metadata', f'title={escape_metadata(title)}',
        '-metadata', f'artist={escape_metadata(artist)}',
        '-metadata', f'author={escape_metadata(author)}',
        '-metadata:s:v', f'title={escape_metadata(video_title)}',
        '-metadata:s:a', f'title={escape_metadata(audio_title)}',
        '-metadata:s:s', f'title={escape_metadata(subtitle_title)}',
        '-y',
        output_path
    ]
    process = process2 = None
    try:
        process = await asyncio.wait_for(
            asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            ),
            timeout=180
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            alt_cmd = [
                ffmpeg_path, '-i', input_path,
                '-map', '0',
                '-c', 'copy',
                '-metadata', f'title={escape_metadata(title)}',
                '-metadata', f'artist={escape_metadata(artist)}',
                '-metadata', f'author={escape_metadata(author)}',
                '-y', output_path
            ]
            process2 = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *alt_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=180
            )
            await process2.communicate()
            if process2.returncode != 0:
                shutil.copy2(input_path, output_path)
                return output_path
    except asyncio.CancelledError:
        for _p in (process, process2):
            if _p and _p.returncode is None:
                try:
                    _p.kill()
                except ProcessLookupError:
                    pass
        raise
    except asyncio.TimeoutError:
        shutil.copy2(input_path, output_path)
        return output_path
    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        shutil.copy2(input_path, output_path)
    return output_path

def extract_file_info(file_name: str):
    season = 0
    episode = 0
    quality = 0

    # 1. Combined bracket [Sxx-yy] or [Sxx yy] e.g. [S03-12]
    match = re.search(r'\[S(\d+)[\s\-]+(\d+)\]', file_name, re.IGNORECASE)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
    else:
        # 2. Non‑bracket Sxx_yy or Sxx-yy (with word boundary guard)
        match = re.search(r'\bS(\d+)[_\-](\d+)\b', file_name, re.IGNORECASE)
        if match:
            season = int(match.group(1))
            episode = int(match.group(2))

    # 3. Fallback: try to find a season number if still 0
    if season == 0:
        season_patterns = [
            r'\[S(\d+)\]', r'Season\s*(\d+)', r'Saison\s*(\d+)',
            r'\bS(\d+)\b',
        ]
        for pat in season_patterns:
            m = re.search(pat, file_name, re.IGNORECASE)
            if m:
                season = int(m.group(1))
                break

    # 4. Fallback: try to find episode if still 0
    if episode == 0:
        # [<episode> - ...] e.g. [10 - Isekai ...]
        m = re.search(r'\[(\d+)\s*[-–]', file_name)
        if m:
            episode = int(m.group(1))
        else:
            # [E<episode> - ...] or [EP<episode> - ...]
            m = re.search(r'\[E(?:P)?(\d+)\s*[-–]', file_name, re.IGNORECASE)
            if m:
                episode = int(m.group(1))
            else:
                # Classic episode patterns
                ep_patterns = [
                    r'\[E(?:P)?(\d+)\]', r'Episode\s*(\d+)', r'\bE(?:P)?(\d+)\b'
                ]
                for pat in ep_patterns:
                    m = re.search(pat, file_name, re.IGNORECASE)
                    if m:
                        episode = int(m.group(1))
                        break

    # 5. [SO] heuristic – if there’s a [SO] tag and no season found yet, set season = 1
    if season == 0 and re.search(r'\[SO\]', file_name, re.IGNORECASE):
        season = 1

    # 6. Quality detection (same as before, numeric mapping)
    quality_map = {
        '4k': 4, '2160p': 4, 'uhd': 4,
        '2k': 3, '1440p': 3, 'qhd': 3,
        '1080p': 2, 'fhd': 2, 'full hd': 2,
        '720p': 1, 'hd': 1,
        '480p': 0, 'sd': 0, '360p': 0,
    }
    fname_lower = file_name.lower()
    for key in sorted(quality_map, key=lambda k: len(k), reverse=True):
        if key in fname_lower:
            quality = quality_map[key]
            break

    return season, quality, episode

# ==================== CUSTOM PATTERN ENGINE ====================
_EMOJI_RE = re.compile(
    "["
    "\U0001F000-\U0001FAFF"   # emoji, pictographs, stickers, skin tones, flags
    "\U00002600-\U000027BF"   # misc symbols, dingbats
    "\U00002B00-\U00002BFF"
    "\U000E0020-\U000E007F"   # tag chars
    "\u20E3\uFE00-\uFE0F\u200B-\u200F\u2060\uFEFF"  # keycap, selectors, zero-width
    "]+"
)

def clean_text(text) -> str:
    """Normalise file name / caption so emojis, stickers, fancy fonts,
    zero-width and control chars never break matching. Applied to BOTH
    the text and the saved pattern, so they stay comparable."""
    if not text:
        return ""
    try:
        text = unicodedata.normalize("NFKC", str(text))   # 𝐒𝟎𝟏 -> S01, ［ -> [
    except Exception:
        text = str(text)
    text = _EMOJI_RE.sub(" ", text)
    text = "".join(
        c for c in text
        if unicodedata.category(c) not in ("Cc", "Cf", "Cs", "Co") or c in "\n\t"
    )
    return re.sub(r"\s+", " ", text).strip()

_PLACEHOLDERS = {
    'season':  r'\d{1,4}',
    'episode': r'\d{1,4}',
    'quality': r'(?:\d{3,4}[pi]|[248]k|uhd|fhd|qhd|hd|sd)',
}
_PH_RE = re.compile(r'\{(\w+)\}')

def _lit(s: str) -> str:
    if not s:
        return ""
    core = r"\s*".join(re.escape(t) for t in s.split())
    if not core:
        return r"\s*"
    return (r"\s*" if s[0].isspace() else "") + core + (r"\s*" if s[-1].isspace() else "")

@lru_cache(maxsize=512)
def compile_pattern(pattern: str):
    """'S{season} - E{episode} [{quality} ⌯' -> regex. Raises ValueError if invalid."""
    p = clean_text(pattern)
    parts, last, seen = [], 0, set()
    for m in _PH_RE.finditer(p):
        name = m.group(1).lower()
        if name not in _PLACEHOLDERS:
            raise ValueError(f"Unknown variable {{{name}}}. Allowed: {{season}}, {{episode}}, {{quality}}")
        parts.append(_lit(p[last:m.start()]))
        if name in seen:
            parts.append(f"(?P={name})")
        else:
            parts.append(f"(?P<{name}>{_PLACEHOLDERS[name]})")
            seen.add(name)
        last = m.end()
    parts.append(_lit(p[last:]))
    if not seen:
        raise ValueError("Pattern must contain at least one of {season}, {episode}, {quality}")
    return re.compile("".join(parts), re.IGNORECASE)

def norm_quality(raw: str) -> str:
    r = raw.lower()
    if r in ('4k', '2160p', 'uhd'): return '4K'
    if r in ('2k', '1440p', 'qhd'): return '2K'
    if r == 'fhd': return '1080p'
    if r in ('hd', 'sd'): return r.upper()
    return r  # 720p, 1080p, 480p ...

def quality_to_int(q) -> int:
    if not q:
        return 0
    q = q.lower()
    if q == '4k': return 4
    if q == '2k': return 3
    if q == 'hd': return 1
    m = re.match(r'(\d+)', q)
    if m:
        h = int(m.group(1))
        return 4 if h >= 2160 else 3 if h >= 1440 else 2 if h >= 1080 else 1 if h >= 720 else 0
    return 0

def detect_quality_str(text: str):
    m = re.search(r'(?<!\d)(2160p|1440p|1080p|720p|576p|540p|480p|360p)', text, re.I)
    if m:
        return norm_quality(m.group(1))
    m = re.search(r'(?<![a-z0-9])(4k|2k|uhd|qhd|fhd)(?![a-z0-9])', text, re.I)
    return norm_quality(m.group(1)) if m else None

def extract_variables(message, file_name: str, source: str, patterns: list) -> dict:
    """1) default patterns  2) user's saved patterns (only fills what's missing).
    source = 'filename' | 'caption'"""
    raw = (getattr(message, "caption", None) or "") if source == "caption" else (file_name or "")
    text = clean_text(raw)
    season, _, episode = extract_file_info(text)
    quality = detect_quality_str(text)
    via = "default"
    if patterns and (not season or not episode or not quality):
        for p in patterns:
            try:
                m = compile_pattern(p).search(text)
            except Exception:
                continue
            if not m:
                continue
            g = m.groupdict()
            if not season and g.get('season'):
                season = int(g['season']); via = "saved"
            if not episode and g.get('episode'):
                episode = int(g['episode']); via = "saved"
            if not quality and g.get('quality'):
                quality = norm_quality(g['quality']); via = "saved"
            if season and episode and quality:
                break
    return {'season': season, 'episode': episode, 'quality': quality, 'via': via}

async def get_variables(user_id: int, message, file_name: str) -> dict:
    source = await db.get_id_source(user_id)
    patterns = await load_patterns(user_id)
    return extract_variables(message, file_name, source, patterns)

# ---- Channel-backed pattern store -------------------------------------------
# Each user's patterns live in ONE message inside the pattern channel
# (header line + one pattern per line). MongoDB only keeps the message
# reference (chat id + message id). Reads are cached in memory.
_pattern_cache: Dict[int, List[str]] = {}
_pattern_locks: Dict[int, asyncio.Lock] = {}

def _pattern_lock(user_id: int) -> asyncio.Lock:
    lock = _pattern_locks.get(user_id)
    if lock is None:
        lock = _pattern_locks[user_id] = asyncio.Lock()
    return lock

async def save_patterns(user_id: int, pats: List[str]):
    """Write the user's full pattern list to the channel message. Raises on failure."""
    chat_id = await db.get_pattern_channel()
    if not chat_id:
        raise RuntimeError("Pattern channel is not set")
    ref = await db.get_pattern_ref(user_id)
    if not pats:
        if ref:
            try:
                await app.delete_messages(ref['chat_id'], ref['message_id'])
            except Exception as e:
                print(f"Pattern message delete error: {e}")
            await db.clear_pattern_ref(user_id)
        _pattern_cache[user_id] = []
        return
    text = f"PATTERNS | user_id={user_id}\n" + "\n".join(pats)
    done = False
    if ref and ref['chat_id'] == chat_id:
        try:
            await app.edit_message_text(
                chat_id, ref['message_id'], text,
                parse_mode=ParseMode.DISABLED, disable_web_page_preview=True
            )
            done = True
        except Exception as e:
            if type(e).__name__ == "MessageNotModified":
                done = True
            else:
                print(f"Pattern message edit failed, creating a new one: {e}")
    if not done:
        sent = await app.send_message(
            chat_id, text, parse_mode=ParseMode.DISABLED, disable_web_page_preview=True
        )
        await db.set_pattern_ref(user_id, sent.chat.id, sent.id)
    _pattern_cache[user_id] = list(pats)

async def _load_patterns_inner(user_id: int) -> List[str]:
    ref = await db.get_pattern_ref(user_id)
    pats = None
    if ref:
        try:
            m = await app.get_messages(ref['chat_id'], ref['message_id'])
            if m and not getattr(m, "empty", False) and m.text:
                pats = [ln.strip() for ln in m.text.split("\n")[1:] if ln.strip()]
            else:                       # message was deleted
                await db.clear_pattern_ref(user_id)
                pats = []
        except Exception as e:
            print(f"Pattern channel read error (user {user_id}): {e}")
            return []                   # transient error: don't cache
    if pats is None:
        # Legacy: patterns saved earlier in MongoDB. Use them, and move them
        # into the channel as soon as a pattern channel exists.
        legacy = await db.get_patterns(user_id)
        pats = list(legacy)
        if legacy and await db.get_pattern_channel():
            try:
                await save_patterns(user_id, legacy)
                await db.set_patterns(user_id, [])
            except Exception as e:
                print(f"Pattern migration error (user {user_id}): {e}")
    _pattern_cache[user_id] = pats
    return list(pats)

async def load_patterns(user_id: int, _locked: bool = False) -> List[str]:
    if user_id in _pattern_cache:
        return list(_pattern_cache[user_id])
    if _locked:
        return await _load_patterns_inner(user_id)
    async with _pattern_lock(user_id):
        if user_id in _pattern_cache:
            return list(_pattern_cache[user_id])
        return await _load_patterns_inner(user_id)

# ---- <bold> markers in /autorename and /set_caption -------------------------
# "<Episode: {episode}>" -> "Episode: 05" (bold in caption, plain in file name).
# Real HTML tags the user may already use (<b>, <i>, <code>, <a href=..> ...) are left alone.
_B_OPEN, _B_CLOSE = "\uE000", "\uE001"
_HTML_TAGS = r"(?:b|i|u|s|a|em|strong|code|pre|ins|del|strike|blockquote|tg-spoiler|tg-emoji|span)"
_BOLD_RE = re.compile(r"<(?!/?" + _HTML_TAGS + r"(?:\s[^<>]*)?>)([^<>\n]+)>")

def _mark_bold(t: str) -> str:
    return _BOLD_RE.sub(lambda m: _B_OPEN + m.group(1) + _B_CLOSE, t)

def _to_html_bold(t: str) -> str:
    return _BOLD_RE.sub(lambda m: "<b>" + m.group(1) + "</b>", t)

def _esc(t) -> str:
    """Escape text so it is shown literally inside bot replies."""
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _idsrc_buttons(cur: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"{'✅ ' if cur == 'filename' else ''}File Name", callback_data="set_idsrc_filename"),
        InlineKeyboardButton(f"{'✅ ' if cur == 'caption' else ''}Caption", callback_data="set_idsrc_caption"),
    ]])

def check_template_needs_variables(format_template: str) -> set:
    needed = set()
    for var in ['{season}', '{episode}', '{quality}']:
        if var in format_template:
            needed.add(var)
    return needed

def check_extraction_failed(format_template: str, season: int, episode: int, quality_str: str) -> list:
    failed = []
    needed = check_template_needs_variables(format_template)
    if '{season}' in needed and season == 0:
        failed.append('{season}')
    if '{episode}' in needed and episode == 0:
        failed.append('{episode}')
    return failed

def check_count_conflict(template: str) -> bool:
    """True if {count} is combined with {season}/{episode}/{quality} in the same template."""
    if not template or '{count}' not in template:
        return False
    return any(v in template for v in ['{season}', '{episode}', '{quality}'])

def _cover_kwargs(cover_path):
    """Different forks name the video-cover arg differently:
    pyrofork -> `cover`, wzgram -> `video_cover`. Pick whichever the installed
    send_video() actually accepts (a wrong kwarg raises TypeError)."""
    if not cover_path:
        return {}
    try:
        params = inspect.signature(Client.send_video).parameters
    except Exception:
        params = {}
    for name in ("video_cover", "cover"):
        if name in params:
            return {name: cover_path}
    print("⚠️ WARNING: installed Pyrogram fork has no video cover parameter; "
          "only the small thumbnail will be used.")
    return {}

# ==================== FILE SENDING ====================
async def _send_file(target_chat_id, output_path, send_as, final_filename, caption,
                     thumb_path, duration, message, status_msg, upload_start,
                     cover_path=None):
    sent_msg = None
    if send_as == "document":
        sent_msg = await app.send_document(
            chat_id=target_chat_id,
            document=output_path,
            caption=caption[:1024] if caption else None,
            thumb=thumb_path,
            file_name=final_filename,
            progress=progress_for_pyrogram,
            progress_args=("📤 Uploading...", status_msg if status_msg else message, upload_start),
            reply_to_message_id=message.id if message and target_chat_id == message.chat.id else None
        )
    elif send_as == "video":
        try:
            sent_msg = await app.send_video(
                chat_id=target_chat_id,
                video=output_path,
                caption=caption[:1024] if caption else None,
                thumb=thumb_path,
                # `cover` uploads a real, full-quality Photo that Telegram
                # displays as the video's cover -- it is a *separate* field
                # from `thumb` (which stays capped at 200KB/320px by
                # Telegram itself, no matter what we upload for it).
                **_cover_kwargs(cover_path),
                duration=duration,
                file_name=final_filename,
                progress=progress_for_pyrogram,
                progress_args=("📤 Uploading...", status_msg if status_msg else message, upload_start),
                reply_to_message_id=message.id if message and target_chat_id == message.chat.id else None
            )
        except TypeError:
            # Installed pyrogram build doesn't know the `cover` kwarg
            # (i.e. it's not pyrofork / a Bot-API-7.9+-aware fork).
            # Fall back to a normal send_video without a high-quality cover
            # rather than crashing the whole task.
            print("send_video() got an unexpected 'cover' kwarg -- your installed "
                  "pyrogram build doesn't support high-quality video covers. "
                  "Install 'pyrofork' instead of 'pyrogram' to enable this.")
            sent_msg = await app.send_video(
                chat_id=target_chat_id,
                video=output_path,
                caption=caption[:1024] if caption else None,
                thumb=thumb_path,
                duration=duration,
                file_name=final_filename,
                progress=progress_for_pyrogram,
                progress_args=("📤 Uploading...", status_msg if status_msg else message, upload_start),
                reply_to_message_id=message.id if message and target_chat_id == message.chat.id else None
            )
        except Exception as upload_error:
            print(f"Video upload failed, falling back to document: {upload_error}")
            sent_msg = await app.send_document(
                chat_id=target_chat_id,
                document=output_path,
                caption=caption[:1024] if caption else None,
                thumb=thumb_path,
                file_name=final_filename,
                progress=progress_for_pyrogram,
                progress_args=("📤 Uploading (fallback)...", status_msg if status_msg else message, upload_start),
                reply_to_message_id=message.id if message and target_chat_id == message.chat.id else None
            )
    elif send_as == "audio":
        sent_msg = await app.send_audio(
            chat_id=target_chat_id,
            audio=output_path,
            caption=caption[:1024] if caption else None,
            thumb=thumb_path,
            duration=duration,
            file_name=final_filename,
            progress=progress_for_pyrogram,
            progress_args=("📤 Uploading...", status_msg if status_msg else message, upload_start),
            reply_to_message_id=message.id if message and target_chat_id == message.chat.id else None
        )
    else:
        sent_msg = await app.send_document(
            chat_id=target_chat_id,
            document=output_path,
            caption=caption[:1024] if caption else None,
            thumb=thumb_path,
            file_name=final_filename,
            progress=progress_for_pyrogram,
            progress_args=("📤 Uploading...", status_msg if status_msg else message, upload_start),
            reply_to_message_id=message.id if message and target_chat_id == message.chat.id else None
        )
    return sent_msg

# ==================== QUEUE WORKER ====================
async def queue_worker():
    print("👷 Queue Worker: Started")
    while True:
        if renaming_paused:
            await asyncio.sleep(2)
            continue
        try:
            if processing_queue.get_queue_length() == 0:
                await asyncio.sleep(2)
                continue
            if processing_queue.is_processing:
                if processing_queue.check_timeout():
                    print(f"⚠️ Task timeout detected: {processing_queue.current_task_id}")
                    processing_queue.is_processing = False
                    processing_queue.current_task = None
                    processing_queue.current_task_id = None
                    processing_queue.task_start_time = None
                    processing_queue.failed_tasks += 1
                await asyncio.sleep(1)
                continue
            async with processing_queue.lock:
                processing_queue.is_processing = True
                task = processing_queue.get_next_task()
                if not task:
                    processing_queue.is_processing = False
                    await asyncio.sleep(2)
                    continue
                processing_queue.current_task = task
                processing_queue.current_task_id = task['task_id']
                processing_queue.task_start_time = time.time()
                task['status'] = 'processing'
                task['start_time'] = time.time()
                print(f"Processing task: {task['task_id']} - Admin: {task.get('is_admin')}")
                task_user_id = task['user_id']
                try:
                    message = await app.get_messages(
                        chat_id=task['chat_id'],
                        message_ids=task['message_id']
                    )
                    if not message:
                        print(f"Message not found: {task['message_id']}")
                        processing_queue.completed_tasks += 1
                        processing_queue.failed_tasks += 1
                        processing_queue.is_processing = False
                        processing_queue.current_task = None
                        processing_queue.current_task_id = None
                        asyncio.create_task(_trigger_manual_rename_if_ready(task_user_id))
                        continue
                    # Run as a separate asyncio.Task so the Cancel button /
                    # /clear_queue can stop it at ANY stage (download, ffmpeg, upload).
                    job = asyncio.create_task(process_queue_file(message, task['user_id'], task))
                    processing_queue.current_job = job
                    await asyncio.wait({job})
                    if job.cancelled():
                        print(f"🚫 Task cancelled: {task['task_id']}")
                        processing_queue.cancelled_tasks += 1
                        processing_queue.mark_task_completed(task['task_id'], success=False)
                    else:
                        exc = job.exception()
                        if exc is None:
                            processing_queue.completed_tasks += 1
                            processing_queue.mark_task_completed(task['task_id'], success=True)
                            print(f"✅ Task completed successfully: {task['task_id']}")
                        elif isinstance(exc, asyncio.TimeoutError):
                            print(f"⏰ Task {task['task_id']} timed out")
                            processing_queue.failed_tasks += 1
                            processing_queue.mark_task_completed(task['task_id'], success=False)
                        else:
                            print(f"❌ Error in task processing: {str(exc)[:100]}")
                            processing_queue.failed_tasks += 1
                            processing_queue.mark_task_completed(task['task_id'], success=False)
                except Exception as e:
                    print(f"❌ Error getting message: {e}")
                    processing_queue.failed_tasks += 1
                    processing_queue.mark_task_completed(task['task_id'], success=False)
                finally:
                    processing_queue.current_job = None
                    processing_queue.forget_task(task)
                    processing_queue.is_processing = False
                    processing_queue.current_task = None
                    processing_queue.current_task_id = None
                    processing_queue.task_start_time = None
                    await asyncio.sleep(1)
                    asyncio.create_task(_trigger_manual_rename_if_ready(task_user_id))
        except Exception as e:
            print(f"⚠️ Queue worker error: {e}")
            await asyncio.sleep(5)

# ==================== CORE PROCESSING FUNCTION ====================
async def process_queue_file(message, user_id, task_info):
    format_template = await db.get_format_template(user_id)
    if not format_template:
        try:
            await safe_send_message(
                chat_id=user_id,
                text="❌ Please set a rename format first!\n"
                     "Use: `/autorename Your Format Here`\n\n"
                     "**Example:** `/autorename {filename} [S{season}E{episode}]`",
            )
        except:
            pass
        return

    if message.document:
        file_name = message.document.file_name or "file"
        file_size = message.document.file_size
        media_type = "document"
        duration = 0
    elif message.video:
        file_name = message.video.file_name or "video.mp4"
        file_size = message.video.file_size
        media_type = "video"
        duration = message.video.duration
    elif message.audio:
        file_name = message.audio.file_name or "audio.mp3"
        file_size = message.audio.file_size
        media_type = "audio"
        duration = message.audio.duration
    else:
        return

    if file_size and file_size > Config.MAX_FILE_SIZE:
        try:
            await message.reply_text(
                f"❌ **File Too Large**\n\n"
                f"File `{file_name}` is {humanbytes(file_size)} which exceeds "
                f"the maximum limit of {humanbytes(Config.MAX_FILE_SIZE)}."
            )
        except:
            pass
        return

    base_name = os.path.splitext(file_name)[0]
    original_ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')

    # Variables: default patterns first, then user's saved patterns (/add_pattern),
    # read from file name or caption depending on /identify_from.
    info = await get_variables(user_id, message, file_name)
    season_int, episode_int = info['season'], info['episode']
    season = f"{season_int:02d}" if season_int else 'None'
    episode = f"{episode_int:02d}" if episode_int else 'None'
    quality = info['quality'] or "HD"

    manual_filename = task_info.get('manual_filename')

    # ---- {count} handling ----
    filename_uses_count = '{count}' in format_template
    caption_template = await db.get_caption(user_id)
    caption_uses_count = bool(caption_template) and '{count}' in caption_template
    uses_count = filename_uses_count or caption_uses_count

    count_value = None
    if manual_filename is None and uses_count:
        count_value = await db.get_count(user_id)
        if count_value is None:
            try:
                await safe_send_message(
                    chat_id=user_id,
                    text=(
                        "❌ **`{count}` starting value not set!**\n\n"
                        "Your format uses `{count}` but you haven't set a starting value yet.\n"
                        "Use: `/count <number>` (e.g. `/count 1`) and then resend the file."
                    ),
                )
            except:
                pass
            return

    if manual_filename is None:
        if filename_uses_count:
            # {count} in the filename format skips season/episode/quality scanning entirely.
            failed_vars = []
        else:
            failed_vars = check_extraction_failed(format_template, season_int, episode_int, quality)
    else:
        failed_vars = []

    if failed_vars:
        failed_item = {
            'original_file_name': file_name,
            'file_size': file_size,
            'chat_id': message.chat.id,
            'message_id': message.id,
            'media_type': media_type,
            'duration': duration,
            'original_ext': original_ext,
            'failed_vars': failed_vars,
            'user_id': user_id,
            'task_info': task_info,
        }
        _add_to_failed_queue(user_id, failed_item)
        failed_var_list = ', '.join(failed_vars)
        try:
            await safe_send_message(
                chat_id=user_id,
                text=(
                    f"⚠️ **Auto-Rename Failed — Variable Extraction Error**\n\n"
                    f"**File:** `{file_name}`\n"
                    f"**Could not extract:** `{failed_var_list}`\n\n"
                    f"This file has been saved to your **manual rename queue**.\n"
                    f"After all your current files are processed, the bot will ask you "
                    f"to provide the filename manually.\n\n"
                    f"**Failed queue size:** `{len(_get_failed_queue(user_id))}`\n"
                    f"Use /failed_queue to view pending files.\n"
                    f"💡 Teach the bot this format with /add_pattern."
                )
            )
        except:
            pass
        return

    target_chat_id = message.chat.id

    media_pref = await db.get_media_preference(user_id)
    if media_pref == "video":
        display_ext = original_ext
        send_as = "video"
    elif media_pref == "audio":
        display_ext = original_ext if media_type == "audio" else ".mp3"
        send_as = "audio"
    else:
        display_ext = original_ext
        send_as = "document"

    if manual_filename is not None:
        new_filename = manual_filename
    else:
        new_filename = _mark_bold(format_template)
        replacements = {
            '{filename}': base_name,
            '{season}': season,
            '{episode}': episode,
            '{quality}': quality,
            '{filesize}': humanbytes(file_size),
            '{duration}': str(timedelta(seconds=duration)) if duration else '00:00:00',
            '{count}': str(count_value) if count_value is not None else '',
        }
        for key, value in replacements.items():
            new_filename = new_filename.replace(key, value)

    new_filename = re.sub(r'[<>:"/\\|?*]', '', new_filename)
    bold_filename = new_filename        # still holds the bold markers
    new_filename = new_filename.replace(_B_OPEN, "").replace(_B_CLOSE, "")
    final_filename = new_filename.strip() + display_ext

    try:
        status_msg = await message.reply_text(
            f"🔄 **Processing Started**\n"
            f"**File:** `{file_name}`\n"
            f"**New Name:** `{final_filename[:50]}`\n"
            f"**Output Type:** `{send_as.upper()}`"
            + (" _(Manual Rename)_" if manual_filename else ""),
            reply_markup=_cancel_kb(task_info.get('task_id'))
        )
    except Exception as e:
        print(f"Error sending status message: {e}")
        status_msg = None

    download_path = f"downloads/{user_id}_{int(time.time())}{original_ext}"
    output_path = None
    metadata_path = None
    thumb_path = None
    cover_path = None
    try:
        start_time = time.time()
        if status_msg:
            try:
                await status_msg.edit_text(f"📥 **Downloading...**\n`{file_name}`", reply_markup=_cancel_kb())
            except:
                pass
        try:
            file_path = await message.download(
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("📥 Downloading...", status_msg if status_msg else message, start_time)
            )
        except TypeError:
            file_path = await message.download(file_name=download_path)

        if not file_path or not os.path.exists(file_path):
            if status_msg:
                try:
                    await status_msg.edit_text("❌ Download failed!")
                except:
                    pass
            return

        file_size = os.path.getsize(file_path)
        if status_msg:
            try:
                await status_msg.edit_text(f"✅ **Downloaded!**\n\n**Size:** {humanbytes(file_size)}\n\n⚙️ **Processing...**", reply_markup=_cancel_kb())
            except:
                pass

        output_path = file_path
        metadata_enabled = await db.get_metadata(user_id)
        if metadata_enabled:
            try:
                metadata_path = f"temp/{user_id}_metadata{original_ext}"
                output_path = await add_metadata_correct(file_path, metadata_path, user_id)
                if output_path != file_path:
                    await cleanup_files(file_path)
            except Exception as e:
                print(f"Metadata error: {e}")
                output_path = file_path

        # Cover channel image (full/original quality) takes priority over the
        # regular file_id-based thumbnail. It is fetched fresh from the
        # storage channel for every file and deleted from the server right
        # after processing (handled by the existing cleanup_files() call in
        # the `finally` block below).
        #
        # IMPORTANT: we download the cover TWICE into two separate files:
        #   - thumb_path  -> squeezed down to Telegram's mandatory
        #                    <=200KB/<=320px `thumb` limit (used for
        #                    document/audio, and as send_video's `thumb`).
        #   - cover_path  -> kept at full/original quality, only ever used
        #                    for send_video's dedicated `cover` field, which
        #                    Telegram stores as a real Photo and is NOT
        #                    subject to the 200KB/320px thumb restriction.
        cover_info = await db.get_cover_message(user_id)
        if cover_info:
            try:
                cover_msg = await app.get_messages(
                    chat_id=cover_info['chat_id'],
                    message_ids=cover_info['message_id']
                )
                if cover_msg and cover_msg.photo:
                    # cover_msg.photo.file_id refers to Telegram's largest
                    # stored size for that photo, i.e. original/full quality.
                    if send_as == "video":
                        cover_path = f"temp/{user_id}_cover_full.jpg"
                        await app.download_media(cover_msg.photo.file_id, file_name=cover_path)
                        cover_path = await process_cover_image(cover_path)

                    thumb_path = f"temp/{user_id}_cover_thumb.jpg"
                    await app.download_media(cover_msg.photo.file_id, file_name=thumb_path)
                    thumb_path = await process_thumbnail(thumb_path)
                else:
                    thumb_path = None
            except Exception as e:
                print(f"Cover image fetch error for user {user_id}: {e}")
                thumb_path = None
                cover_path = None

        if not thumb_path:
            user_thumb = await db.get_thumbnail(user_id)
            if user_thumb:
                try:
                    thumb_path = f"temp/{user_id}_thumb.jpg"
                    await app.download_media(user_thumb, file_name=thumb_path)
                    thumb_path = await process_thumbnail(thumb_path)
                except:
                    thumb_path = None
        if not thumb_path and media_type == "video" and message.video and message.video.thumbs:
            try:
                thumb = message.video.thumbs[0]
                thumb_path = f"temp/{user_id}_video_thumb.jpg"
                await app.download_media(thumb.file_id, file_name=thumb_path)
                thumb_path = await process_thumbnail(thumb_path)
            except:
                thumb_path = None

        if caption_template is None:
            caption = (bold_filename.strip() + display_ext).replace(_B_OPEN, "<b>").replace(_B_CLOSE, "</b>")
        else:
            caption = _to_html_bold(caption_template).replace("{filename}", os.path.splitext(final_filename)[0])\
                                     .replace("{filesize}", humanbytes(file_size))\
                                     .replace("{duration}", str(timedelta(seconds=duration)) if duration else '00:00:00')\
                                     .replace("{season}", season)\
                                     .replace("{episode}", episode)\
                                     .replace("{quality}", quality)\
                                     .replace("{count}", str(count_value) if count_value is not None else '')

        if status_msg:
            try:
                await status_msg.edit_text("📤 **Uploading renamed file...**", reply_markup=_cancel_kb())
            except:
                pass

        upload_start = time.time()
        try:
            await _send_file(
                target_chat_id=target_chat_id,
                output_path=output_path,
                send_as=send_as,
                final_filename=final_filename,
                caption=caption,
                thumb_path=thumb_path,
                duration=duration,
                message=message,
                status_msg=status_msg,
                upload_start=upload_start,
                cover_path=cover_path,
            )
            if manual_filename is None and uses_count and count_value is not None:
                # Auto-increment {count} only after a successful upload.
                try:
                    await db.increment_count(user_id)
                except Exception as e:
                    print(f"Error incrementing count for user {user_id}: {e}")
            if status_msg:
                try:
                    await status_msg.delete()
                except:
                    pass
        except Exception as upload_error:
            if status_msg:
                try:
                    await status_msg.edit_text(f"❌ **Upload Error:** {str(upload_error)[:200]}")
                except:
                    pass
            raise
    except asyncio.CancelledError:
        try:
            if status_msg:
                await status_msg.edit_text(f"🚫 **Task cancelled.**\n`{file_name}`", reply_markup=None)
        except Exception:
            pass
        raise
    except asyncio.TimeoutError:
        error_text = "⏰ **Processing timeout!**"
        try:
            if status_msg:
                await status_msg.edit_text(error_text)
        except:
            pass
        raise
    except Exception as e:
        error_text = f"❌ **Error:** {str(e)[:200]}"
        try:
            if status_msg:
                await status_msg.edit_text(error_text)
        except:
            pass
        raise
    finally:
        # also remove partial downloads (Pyrogram's *.temp) and half-written metadata file
        await cleanup_files(
            download_path,
            *glob.glob(os.path.splitext(download_path)[0] + "*"),
            metadata_path,
            output_path if output_path and output_path != download_path else None,
            thumb_path,
            cover_path,
        )

# ==================== BOT CLIENT ====================
os.makedirs("downloads", exist_ok=True)
os.makedirs("temp", exist_ok=True)

app = Client(
    "auto_rename_bot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workers=10,
    sleep_threshold=10,
    no_updates=False,
    in_memory=True
)

# ==================== FILE HANDLER (private chat only) ====================
@app.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def private_file_handler(client, message):
    user_id = message.from_user.id

    if user_id in sequence_sessions and sequence_sessions[user_id].get('active'):
        session = sequence_sessions[user_id]
        session['files'].append(message)
        count = len(session['files'])
        try:
            await message.reply_text(f"📥 Saved for sequencing ({count} files so far).")
        except:
            pass
        return

    try:
        await db.add_user(user_id)

        file_size = 0
        if message.document:
            file_size = message.document.file_size or 0
        elif message.video:
            file_size = message.video.file_size or 0
        elif message.audio:
            file_size = message.audio.file_size or 0

        if file_size > Config.MAX_FILE_SIZE:
            try:
                await message.reply_text(
                    f"❌ **File Too Large**\n\n"
                    f"File size ({humanbytes(file_size)}) exceeds the maximum limit of {humanbytes(Config.MAX_FILE_SIZE)}."
                )
            except:
                pass
            return

        # message.id is captured here at receive time and is what gets
        # used for ordering inside add_to_queue -- so even if this handler
        # instance is delayed relative to another (workers=10), the final
        # queue order still reflects true Telegram send order, not
        # handler-arrival order.
        queue_position, task_id, task_item = processing_queue.add_to_queue(message, user_id)

        if message.document:
            file_name = message.document.file_name or "file"
        elif message.video:
            file_name = message.video.file_name or "video.mp4"
        elif message.audio:
            file_name = message.audio.file_name or "audio.mp3"
        else:
            file_name = "Unknown"

        status_text = (
            f"✅ **File added to queue!**\n\n"
            f"**File:** `{file_name[:50]}`\n"
            f"**Queue Position:** `{queue_position}`\n"
            f"**Queue Size:** `{processing_queue.get_queue_length()}`\n"
            f"⚡ **Normal rename mode**\n"
            f"⏳ **Please wait, files are processed one by one in the order you sent them...**"
        )

        try:
            await message.reply_text(status_text, reply_markup=_cancel_kb(task_id))
        except Exception as e:
            print(f"Error sending queue message: {e}")

        print(f"Added file to queue. Task ID: {task_id}, Position: {queue_position}")

    except Exception as e:
        print(f"Error in private file handler: {e}")

# ==================== MANUAL RENAME TEXT HANDLER ====================
@app.on_message(filters.private & filters.text & ~filters.command(
    ["start", "help", "autorename", "count", "set_caption", "view_caption", "clear_caption",
     "metadata", "view_metadata", "delete_metadata", "showmetadata",
     "settitle", "setauthor", "setartist", "setaudio", "setsubtitle", "setvideo", "setallmeta",
     "set_thumbnail", "view_thumbnail", "delete_thumbnail", "view_thumb",
     "mediatype", "queue", "queue_stats", "failed_queue", "skip_failed",
     "admin_priority_on", "admin_priority_off", "clear_queue", "clear_queue_user",
     "stats", "ssequence", "esequence", "sequence_mode",
     "stop_renaming", "start_renaming",
     "set_cover", "view_cover", "delete_cover", "set_channel", "register",
     "add_pattern", "patterns", "del_pattern", "identify_from"]
))
async def manual_rename_text_handler(client, message):
    user_id = message.from_user.id
    text = message.text.strip() if message.text else ""

    if user_id not in manual_rename_waiting:
        return
    waiting_info = manual_rename_waiting[user_id]
    if not waiting_info.get('awaiting'):
        return
    new_name = text
    if not new_name:
        await message.reply_text("❌ Please send a valid filename (non-empty text).")
        return
    new_name = re.sub(r'[<>:"/\\|?*]', '', new_name)
    failed_item = waiting_info['failed_item']
    del manual_rename_waiting[user_id]
    failed_list = _get_failed_queue(user_id)
    if failed_item in failed_list:
        failed_list.remove(failed_item)
    chat_id = failed_item['chat_id']
    message_id = failed_item['message_id']
    try:
        original_message = await app.get_messages(chat_id=chat_id, message_ids=message_id)
    except Exception as e:
        await message.reply_text(
            f"❌ **Could not retrieve the original file.**\n"
            f"Error: `{str(e)[:100]}`\n\n"
            f"The file may have been deleted. Skipping to next..."
        )
        await _prompt_next_manual_rename(user_id)
        return
    if not original_message:
        await message.reply_text(
            "❌ **Original file message not found** (may have been deleted).\n"
            "Skipping to next..."
        )
        await _prompt_next_manual_rename(user_id)
        return
    queue_position, task_id, task_item = processing_queue.add_manual_task_to_queue(
        original_message, user_id, new_name
    )
    await message.reply_text(
        f"✅ **File added to rename queue!**\n\n"
        f"**Original File:** `{failed_item['original_file_name']}`\n"
        f"**New Name:** `{new_name}`\n"
        f"**Queue Position:** `{queue_position}`\n\n"
        f"⏳ The file will be processed shortly."
    )
    print(f"Manual rename queued: '{failed_item['original_file_name']}' -> '{new_name}' for user {user_id}")
    remaining = len(_get_failed_queue(user_id))
    if remaining > 0:
        await asyncio.sleep(1)
        await _prompt_next_manual_rename(user_id)
    else:
        await message.reply_text(
            "🎉 **All manual renames done!** No more files pending manual rename."
        )

# ==================== COMMAND HANDLERS ====================
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    try:
        user = message.from_user
        await db.add_user(user.id)
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📚 ʜᴇʟᴘ", callback_data='help'), InlineKeyboardButton("⚙️ ᴍᴇᴛᴀᴅᴀᴛᴀ", callback_data='metadata')],
            [
                InlineKeyboardButton('📢 ᴜᴘᴅᴀᴛᴇs', url='https://t.me/AnimeMultiDub'),
                InlineKeyboardButton('🆘 sᴜᴘᴘᴏʀᴛ', url='https://t.me/AnimeMultiDub')
            ],
            [
                InlineKeyboardButton('📊 Queue Stats', callback_data='queue_status'),
                InlineKeyboardButton('👑 Admin Priority', callback_data='admin_priority')
            ]
        ])
        if Config.START_PIC:
            await message.reply_photo(
                Config.START_PIC,
                caption=Txt.START_TXT.format(user.mention),
                reply_markup=buttons
            )
        else:
            await message.reply_text(
                Txt.START_TXT.format(user.mention),
                reply_markup=buttons
            )
    except Exception as e:
        print(f"Error in start handler: {e}")

@app.on_message(filters.command("help") & filters.private)
async def help_handler(client, message):
    try:
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')],
            [InlineKeyboardButton("⚙️ ᴍᴇᴛᴀᴅᴀᴛᴀ", callback_data='metadata'), InlineKeyboardButton("📊 Queue", callback_data='queue_status')],
            [InlineKeyboardButton("👑 Admin Priority", callback_data='admin_priority')]
        ])
        await message.reply_text(
            Txt.HELP_TXT,
            reply_markup=buttons,
            disable_web_page_preview=True
        )
    except Exception as e:
        print(f"Error in help handler: {e}")

@app.on_message(filters.command("autorename") & filters.private)
async def autorename_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text(
                "**Please provide a rename format!**\n\n"
                "**Example:** `/autorename {filename} [S{season}E{episode}] - {quality}`\n"
                "**Or (counter-based):** `/autorename Episode {count} - {filename}`\n\n"
                "**Available variables:**\n"
                "- `{filename}`: Original filename\n"
                "- `{season}`: Season number\n"
                "- `{episode}`: Episode number\n"
                "- `{quality}`: Video quality\n"
                "- `{filesize}`: File size\n"
                "- `{duration}`: Duration (for videos)\n"
                "- `{count}`: Auto-incrementing counter (set with `/count <number>`). "
                "Cannot be combined with `{season}`, `{episode}`, or `{quality}`.\n\n"
                "**Note:** This setting is saved per user."
            )
            return
        format_template = message.text.split(" ", 1)[1]

        if check_count_conflict(format_template):
            current_template = await db.get_format_template(user_id)
            await message.reply_text(
                "❌ **Invalid Format!**\n\n"
                "`{count}` cannot be combined with `{season}`, `{episode}`, or `{quality}` in the same format. "
                "When `{count}` is used, season/episode/quality are not scanned at all.\n\n"
                f"**Your current active format:** `{_esc(current_template) if current_template else 'Not set'}`\n\n"
                "Please correct your format and try again with just `{count}` OR the "
                "season/episode/quality variables (not both)."
            )
            return

        await db.set_format_template(user_id, format_template)

        if '{count}' in format_template:
            current_count = await db.get_count(user_id)
            if current_count is None:
                await message.reply_text(
                    f"**✅ Rename format set successfully!**\n\n"
                    f"**Your format:** `{_esc(format_template)}`\n\n"
                    "⚠️ This format uses `{count}` but you haven't set a starting value yet.\n"
                    "Use `/count <number>` (e.g. `/count 1`) before sending files."
                )
            else:
                await message.reply_text(
                    f"**✅ Rename format set successfully!**\n\n"
                    f"**Your format:** `{_esc(format_template)}`\n\n"
                    f"🔢 Current `{{count}}` value: `{current_count}` (will auto-increment per file)\n\n"
                    "Now send me any file to rename it automatically."
                )
        else:
            await message.reply_text(
                f"**✅ Rename format set successfully!**\n\n"
                f"**Your format:** `{_esc(format_template)}`\n\n"
                "Now send me any file to rename it automatically."
            )
    except Exception as e:
        print(f"Error in autorename handler: {e}")

@app.on_message(filters.command("count") & filters.private)
async def count_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            current = await db.get_count(user_id)
            if current is None:
                await message.reply_text(
                    "**🔢 `{count}` starting value is not set!**\n\n"
                    "Use `/count <number>` to set it.\n"
                    "**Example:** `/count 1`\n\n"
                    "Once set, use `{count}` in your `/autorename` or `/set_caption` format "
                    "and it will replace it with this value, then auto-increment after every file."
                )
            else:
                await message.reply_text(
                    f"**🔢 Current `{{count}}` value:** `{current}`\n\n"
                    "This value will be used for the next processed file, then auto-increment.\n"
                    "Use `/count <number>` to change it."
                )
            return
        try:
            value = int(message.command[1])
        except ValueError:
            await message.reply_text("❌ Please provide a valid whole number.\n\n**Example:** `/count 1`")
            return
        await db.set_count(user_id, value)
        await message.reply_text(
            f"**✅ `{{count}}` value set to:** `{value}`\n\n"
            "It will be used the next time you send a file (with `{count}` in your rename or caption format) "
            "and will auto-increment by 1 after each file processed."
        )
    except Exception as e:
        print(f"Error in count handler: {e}")

@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text(
                "**Please provide a caption format!**\n\n"
                "**Example:** `/set_caption {filename} | {filesize}`\n\n"
                "**Available variables:**\n"
                "- `{filename}`: Original filename\n"
                "- `{filesize}`: File size\n"
                "- `{duration}`: Duration (for videos)\n"
                "- `{season}` / `{episode}` / `{quality}`\n"
                "- `{count}`: Auto-incrementing counter (set with `/count <number>`). "
                "Cannot be combined with `{season}`, `{episode}`, or `{quality}`.\n\n"
                "**Note:** Leave empty to remove caption."
            )
            return
        caption = message.text.split(" ", 1)[1]

        if check_count_conflict(caption):
            current_caption = await db.get_caption(user_id)
            await message.reply_text(
                "❌ **Invalid Caption Format!**\n\n"
                "`{count}` cannot be combined with `{season}`, `{episode}`, or `{quality}` in the same caption.\n\n"
                f"**Your current active caption:** `{_esc(current_caption) if current_caption else 'Not set'}`\n\n"
                "Please correct your caption and try again."
            )
            return

        await db.set_caption(user_id, caption)
        await message.reply_text(f"**✅ Caption format set successfully!**\n\n**Your caption:** `{_esc(caption)}`")
    except Exception as e:
        print(f"Error in set_caption handler: {e}")

@app.on_message(filters.command("view_caption") & filters.private)
async def view_caption_handler(client, message):
    try:
        user_id = message.from_user.id
        caption = await db.get_caption(user_id)
        if caption:
            await message.reply_text(
                f"**📝 Your Current Caption Format:**\n\n"
                f"`{_esc(caption)}`\n\n"
                "**Variables Available:**\n"
                "- `{filename}`: Original filename\n"
                "- `{filesize}`: File size\n"
                "- `{duration}`: Duration (for videos)\n"
                "- `{season}` / `{episode}` / `{quality}`\n"
                "- `{count}`: Auto-incrementing counter"
            )
        else:
            await message.reply_text(
                "**❌ No caption format set!**\n\n"
                "Use `/set_caption [format]` to set a caption format."
            )
    except Exception as e:
        print(f"Error in view_caption handler: {e}")

@app.on_message(filters.command("clear_caption") & filters.private)
async def clear_caption_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        await db.set_caption(user_id, None)
        await message.reply_text(
            "✅ **Caption reset to default!**\n\n"
            "From now on, the caption will be the **full renamed file name with its original extension**.\n"
            "Example: `My Video [S01E02] - 1080p.mkv`\n\n"
            "To set a custom caption, use `/set_caption`."
        )
    except Exception as e:
        print(f"Error in clear_caption handler: {e}")

@app.on_message(filters.command("metadata") & filters.private)
async def metadata_interface_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        metadata_enabled = await db.get_metadata(user_id)
        status = "✅ **ENABLED**" if metadata_enabled else "❌ **DISABLED**"
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔧 View Metadata", callback_data='view_metadata')],
            [InlineKeyboardButton(f"{'❌ Disable' if metadata_enabled else '✅ Enable'} Metadata",
                                  callback_data='toggle_metadata')],
            [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')]
        ])
        await message.reply_text(
            f"**⚙️ Metadata Settings**\n\n"
            f"**Current Status:** {status}\n\n"
            f"Metadata adds title, author, artist, and other info to your files.\n"
            f"How to set metadata: use /settitle /setauthor /setartist /setaudio /setsubtitle /setvideo /setallmeta\n"
            f"Example: `/setaudio @AnimeMultiDub`",
            reply_markup=buttons
        )
    except Exception as e:
        print(f"Error in metadata interface handler: {e}")

@app.on_message(filters.command("view_metadata") & filters.private)
async def view_metadata_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        title = await db.get_title(user_id)
        author = await db.get_author(user_id)
        artist = await db.get_artist(user_id)
        audio = await db.get_audio(user_id)
        subtitle = await db.get_subtitle(user_id)
        video = await db.get_video(user_id)
        metadata_enabled = await db.get_metadata(user_id)
        status = "✅ **ENABLED**" if metadata_enabled else "❌ **DISABLED**"
        await message.reply_text(
            f"**📊 Your Metadata Settings**\n\n"
            f"**Metadata Status:** {status}\n\n"
            f"**Title:** `{title}`\n"
            f"**Author:** `{author}`\n"
            f"**Artist:** `{artist}`\n"
            f"**Audio:** `{audio}`\n"
            f"**Subtitle:** `{subtitle}`\n"
            f"**Video:** `{video}`\n\n"
            f"**Commands to change:**\n"
            f"- `/settitle [text]`\n"
            f"- `/setauthor [text]`\n"
            f"- `/setartist [text]`\n"
            f"- `/setaudio [text]`\n"
            f"- `/setsubtitle [text]`\n"
            f"- `/setvideo [text]`\n"
            f"- `/setallmeta [text]` (sets all at once)\n"
            f"- `/metadata` to open settings interface"
        )
    except Exception as e:
        print(f"Error in view_metadata handler: {e}")

@app.on_message(filters.command("delete_metadata") & filters.private)
async def delete_metadata_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.set_title(user_id, "Encoded by @AnimeMultiDub")
        await db.set_author(user_id, "@AnimeMultiDub")
        await db.set_artist(user_id, "@AnimeMultiDub")
        await db.set_audio(user_id, "By @AnimeMultiDub")
        await db.set_subtitle(user_id, "By @AnimeMultiDub")
        await db.set_video(user_id, "Encoded By @AnimeMultiDub")
        await message.reply_text(
            "**✅ Metadata reset to default values!**\n\n"
            "All metadata fields have been reset to default values."
        )
    except Exception as e:
        print(f"Error in delete_metadata handler: {e}")

@app.on_message(filters.command("showmetadata") & filters.private)
async def showmetadata_handler(client, message):
    await view_metadata_handler(client, message)

@app.on_message(filters.command("settitle") & filters.private)
async def settitle_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text("**Please provide a title!**\n\nExample: `/settitle My Custom Title`")
            return
        title = message.text.split(" ", 1)[1]
        await db.set_title(user_id, title)
        await message.reply_text(f"**✅ Title set to:** `{title}`")
    except Exception as e:
        print(f"Error in settitle handler: {e}")

@app.on_message(filters.command("setauthor") & filters.private)
async def setauthor_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text("**Please provide an author!**\n\nExample: `/setauthor Author Name`")
            return
        author = message.text.split(" ", 1)[1]
        await db.set_author(user_id, author)
        await message.reply_text(f"**✅ Author set to:** `{author}`")
    except Exception as e:
        print(f"Error in setauthor handler: {e}")

@app.on_message(filters.command("setartist") & filters.private)
async def setartist_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text("**Please provide an artist!**\n\nExample: `/setartist Artist Name`")
            return
        artist = message.text.split(" ", 1)[1]
        await db.set_artist(user_id, artist)
        await message.reply_text(f"**✅ Artist set to:** `{artist}`")
    except Exception as e:
        print(f"Error in setartist handler: {e}")

@app.on_message(filters.command("setaudio") & filters.private)
async def setaudio_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text("**Please provide audio metadata!**\n\nExample: `/setaudio Audio Metadata`")
            return
        audio = message.text.split(" ", 1)[1]
        await db.set_audio(user_id, audio)
        await message.reply_text(f"**✅ Audio metadata set to:** `{audio}`")
    except Exception as e:
        print(f"Error in setaudio handler: {e}")

@app.on_message(filters.command("setsubtitle") & filters.private)
async def setsubtitle_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text("**Please provide subtitle metadata!**\n\nExample: `/setsubtitle Subtitle Metadata`")
            return
        subtitle = message.text.split(" ", 1)[1]
        await db.set_subtitle(user_id, subtitle)
        await message.reply_text(f"**✅ Subtitle metadata set to:** `{subtitle}`")
    except Exception as e:
        print(f"Error in setsubtitle handler: {e}")

@app.on_message(filters.command("setvideo") & filters.private)
async def setvideo_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text("**Please provide video metadata!**\n\nExample: `/setvideo Video Metadata`")
            return
        video = message.text.split(" ", 1)[1]
        await db.set_video(user_id, video)
        await message.reply_text(f"**✅ Video metadata set to:** `{video}`")
    except Exception as e:
        print(f"Error in setvideo handler: {e}")

@app.on_message(filters.command("setallmeta") & filters.private)
async def setallmeta_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) < 2:
            await message.reply_text(
                "**Please provide a value for all metadata fields!**\n\n"
                "**Usage:** `/setallmeta <text>`\n\n"
                "**Example:** `/setallmeta @AnimeMultiDub`\n\n"
                "This will set **title, author, artist, audio, subtitle, and video** to the same text.\n"
                "You can still change individual fields with their respective commands."
            )
            return
        meta_value = message.text.split(" ", 1)[1]
        await db.set_title(user_id, meta_value)
        await db.set_author(user_id, meta_value)
        await db.set_artist(user_id, meta_value)
        await db.set_audio(user_id, meta_value)
        await db.set_subtitle(user_id, meta_value)
        await db.set_video(user_id, meta_value)
        await message.reply_text(
            f"**✅ All metadata fields set to:** `{meta_value}`\n\n"
            f"**Updated fields:** Title, Author, Artist, Audio, Subtitle, Video\n\n"
            f"Use `/showmetadata` to view all settings."
        )
    except Exception as e:
        print(f"Error in setallmeta handler: {e}")

@app.on_message(filters.command("set_thumbnail") & filters.private)
async def set_thumbnail_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if message.reply_to_message and message.reply_to_message.photo:
            photo = message.reply_to_message.photo
            file_id = photo.file_id
            await db.set_thumbnail(user_id, file_id)
            await message.reply_text(
                "**✅ Thumbnail set successfully!**\n\n"
                "This thumbnail will be used for your uploaded files.\n"
                "Use `/view_thumbnail` to see it or `/delete_thumbnail` to remove it."
            )
        else:
            await message.reply_text(
                "**Please reply to a photo to set it as thumbnail!**\n\n"
                "**Usage:**\n"
                "1. Send a photo\n"
                "2. Reply to it with `/set_thumbnail`"
            )
    except Exception as e:
        print(f"Error in set_thumbnail handler: {e}")

@app.on_message(filters.command("view_thumbnail") & filters.private)
async def view_thumbnail_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        thumbnail = await db.get_thumbnail(user_id)
        if thumbnail:
            await message.reply_photo(
                thumbnail,
                caption="**📸 Your Current Thumbnail**\n\n"
                        "Use `/delete_thumbnail` to remove this thumbnail."
            )
        else:
            await message.reply_text(
                "**📸 No thumbnail set!**\n\n"
                "Use `/set_thumbnail` to set a thumbnail from a photo.\n"
                "Reply to any photo with `/set_thumbnail` to set it as your thumbnail."
            )
    except Exception as e:
        print(f"Error in view_thumbnail handler: {e}")

@app.on_message(filters.command("delete_thumbnail") & filters.private)
async def delete_thumbnail_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.set_thumbnail(user_id, None)
        await message.reply_text(
            "**✅ Thumbnail deleted successfully!**\n\n"
            "Your files will now use default thumbnails.\n"
            "Use `/set_thumbnail` to set a new thumbnail."
        )
    except Exception as e:
        print(f"Error in delete_thumbnail handler: {e}")

@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_handler(client, message):
    await view_thumbnail_handler(client, message)

# ==================== COVER / THUMBNAIL CHANNEL SETUP (Admin) ====================
@app.on_message(filters.command("set_channel") & filters.private)
async def set_channel_handler(client, message):
    try:
        if message.from_user.id not in Config.ADMIN:
            await message.reply_text("❌ **Admin only command!**")
            return
        set_channel_waiting[message.from_user.id] = True
        current = await db.get_cover_channel()
        current_line = f"\n**Currently set channel ID:** `{current}`\n" if current else ""
        await message.reply_text(
            "📢 **Set Cover/Thumbnail Storage Channel**\n\n"
            "**Steps:**\n"
            "1️⃣ Add this bot as an **admin** in the channel\n"
            "2️⃣ Give it **all permissions** (especially posting messages)\n"
            "3️⃣ Now **forward any message** from that channel here to register it\n\n"
            "**Alternative:** After making the bot admin with full permissions, "
            "you can just type `/register` directly **inside the channel** and it "
            "will be set automatically — no need to forward anything here.\n"
            f"{current_line}"
        )
    except Exception as e:
        print(f"Error in set_channel handler: {e}")

@app.on_message(filters.private & filters.forwarded, group=-1)
async def forwarded_message_router(client, message):
    """
    Runs before the normal handlers (group=-1). If the sender is an admin
    currently waiting to register a cover channel and this is a forward
    from a channel, it registers that channel and stops here. Otherwise it
    raises ContinuePropagation so the message falls through to the normal
    handlers (file handler, manual rename text handler, etc.) unaffected.
    """
    user_id = message.from_user.id if message.from_user else None

    if (
        user_id
        and set_channel_waiting.get(user_id)
        and not message.document and not message.video and not message.audio
    ):
        fwd_chat = message.forward_from_chat
        if fwd_chat and str(fwd_chat.type).lower().endswith("channel"):
            try:
                member = await client.get_chat_member(fwd_chat.id, "me")
                is_admin = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
            except Exception:
                is_admin = False
            if not is_admin:
                await message.reply_text(
                    "❌ **The bot is not an admin in that channel yet!**\n\n"
                    "Please add the bot as admin with full permissions first, "
                    "then forward a message from the channel again."
                )
                raise ContinuePropagation
            await db.set_cover_channel(fwd_chat.id)
            set_channel_waiting.pop(user_id, None)
            await message.reply_text(
                f"✅ **Cover/Thumbnail channel set!**\n\n"
                f"**Channel:** {fwd_chat.title or fwd_chat.id}\n"
                f"**ID:** `{fwd_chat.id}`\n\n"
                "Users can now use `/set_cover` to save their full-quality cover images."
            )
            return

    raise ContinuePropagation

@app.on_message(filters.channel & filters.text & filters.command("register", prefixes="/"))
async def register_channel_handler(client, message):
    chat = message.chat
    try:
        member = await client.get_chat_member(chat.id, "me")
        is_admin = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception as e:
        print(f"Error checking admin status in channel {chat.id}: {e}")
        is_admin = False

    if not is_admin:
        try:
            await message.reply_text(
                "❌ Please make this bot an **admin** with full permissions first, "
                "then send /register again."
            )
        except:
            pass
        return

    await db.set_cover_channel(chat.id)
    try:
        await message.reply_text(
            f"✅ **This channel is now set as the Cover/Thumbnail storage channel!**\n\n"
            f"**Channel:** {chat.title or chat.id}\n"
            f"**ID:** `{chat.id}`"
        )
    except:
        pass
    for admin_id in Config.ADMIN:
        try:
            await app.send_message(
                admin_id,
                f"✅ **Cover/Thumbnail channel registered via /register**\n\n"
                f"**Channel:** {chat.title or chat.id}\n"
                f"**ID:** `{chat.id}`"
            )
        except:
            pass

# ==================== PER-USER COVER IMAGE (full quality) ====================
@app.on_message(filters.command("set_cover") & filters.private)
async def set_cover_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)

        cover_channel_id = await db.get_cover_channel()
        if not cover_channel_id:
            await message.reply_text(
                "❌ **Cover/Thumbnail channel is not set up yet!**\n\n"
                "Ask the bot admin to configure it first using `/set_channel` in private, "
                "or `/register` inside the channel."
            )
            return

        if not (message.reply_to_message and message.reply_to_message.photo):
            await message.reply_text(
                "**Please reply to a photo to set it as your cover image!**\n\n"
                "**Usage:**\n"
                "1. Send a photo\n"
                "2. Reply to it with `/set_cover`\n\n"
                "The image will be stored (in full quality) in the cover channel and used "
                "as the thumbnail/cover for your processed files."
            )
            return

        try:
            forwarded = await message.reply_to_message.forward(cover_channel_id)
        except Exception as e:
            await message.reply_text(
                f"❌ **Could not save the cover image:** `{str(e)[:150]}`\n\n"
                "Make sure the bot is still an admin in the cover channel with post permission."
            )
            return

        await db.set_cover_message(user_id, forwarded.chat.id, forwarded.id)
        await message.reply_text(
            "✅ **Cover image saved!**\n\n"
            "This image will now be downloaded in full/original quality and used as the "
            "cover every time one of your files is processed as a **video**. "
            "It's fetched fresh each time and removed from the server right after.\n\n"
            "Note: full-quality covers only apply to `/mediatype video` uploads -- Telegram "
            "documents and audio files only support a small (<=200KB) thumbnail, which is "
            "always generated automatically from this same image.\n\n"
            "Use `/view_cover` to check it or `/delete_cover` to remove it."
        )
    except Exception as e:
        print(f"Error in set_cover handler: {e}")

@app.on_message(filters.command("view_cover") & filters.private)
async def view_cover_handler(client, message):
    try:
        user_id = message.from_user.id
        cover = await db.get_cover_message(user_id)
        if not cover:
            await message.reply_text(
                "📸 **No cover image set!**\n\n"
                "Reply to a photo with `/set_cover` to save one."
            )
            return
        try:
            original = await app.get_messages(chat_id=cover['chat_id'], message_ids=cover['message_id'])
        except Exception as e:
            await message.reply_text(f"⚠️ Error retrieving cover image: `{str(e)[:150]}`")
            return
        if original and original.photo:
            await message.reply_photo(
                original.photo.file_id,
                caption="**📸 Your Current Cover Image**\n\nUse `/delete_cover` to remove it."
            )
        else:
            await message.reply_text(
                "⚠️ **Could not find the saved cover image** "
                "(it may have been deleted from the channel)."
            )
    except Exception as e:
        print(f"Error in view_cover handler: {e}")

@app.on_message(filters.command("delete_cover") & filters.private)
async def delete_cover_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.delete_cover_message(user_id)
        await message.reply_text(
            "✅ **Cover image removed!**\n\n"
            "Files will now fall back to your regular `/set_thumbnail` thumbnail, if any."
        )
    except Exception as e:
        print(f"Error in delete_cover handler: {e}")

# ==================== CUSTOM PATTERNS & IDENTIFY SOURCE ====================
@app.on_message(filters.channel & filters.text & filters.command("add_pattern_channel", prefixes="/"))
async def add_pattern_channel_handler(client, message):
    """Send /add_pattern_channel INSIDE the channel (bot must be admin there)."""
    chat = message.chat
    if message.from_user and message.from_user.id not in Config.ADMIN:
        return
    try:
        member = await client.get_chat_member(chat.id, "me")
        is_admin = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception as e:
        print(f"Error checking admin status in channel {chat.id}: {e}")
        is_admin = False
    if not is_admin:
        try:
            await message.reply_text(
                "❌ Please make this bot an **admin** with full permissions first, "
                "then send /add_pattern_channel again."
            )
        except Exception:
            pass
        return
    await db.set_pattern_channel(chat.id)
    try:
        await message.reply_text(
            f"✅ **This channel is now the Pattern storage channel!**\n\n"
            f"**Channel:** {chat.title or chat.id}\n**ID:** `{chat.id}`\n\n"
            "Every user's saved patterns will be stored here (one message per user)."
        )
    except Exception:
        pass
    for admin_id in Config.ADMIN:
        try:
            await app.send_message(
                admin_id,
                f"✅ **Pattern channel registered via /add_pattern_channel**\n\n"
                f"**Channel:** {chat.title or chat.id}\n**ID:** `{chat.id}`"
            )
        except Exception:
            pass

@app.on_message(filters.command("add_pattern") & filters.private)
async def add_pattern_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        parts = message.text.split(None, 1)
        if len(parts) < 2:
            await message.reply_text(
                "**Usage:** `/add_pattern <pattern>`\n\n"
                "**Variables:** `{season}` `{episode}` `{quality}`\n"
                "**Example:** `/add_pattern S{season} -  E{episode} [{quality} ⌯`\n\n"
                "Matches e.g. `Title S01 -  E02 [720p ⌯ Dual]` → season=01, episode=02, quality=720p.\n"
                "Used only when the built-in patterns fail. /patterns to list, /del_pattern to remove."
            )
            return
        if not await db.get_pattern_channel():
            await message.reply_text(
                "❌ **Pattern channel is not set up yet!**\n\n"
                "Ask the bot admin to send `/add_pattern_channel` inside the pattern "
                "storage channel (the bot must be admin there)."
            )
            return
        # Emojis/stickers/zero-width chars are stripped (matching ignores them anyway).
        pattern = clean_text(parts[1])
        if not pattern:
            await message.reply_text("❌ Pattern is empty after removing emojis/special symbols.")
            return
        if len(pattern) > 100:
            await message.reply_text("❌ Pattern too long (max 100 chars).")
            return
        try:
            compile_pattern(pattern)
        except ValueError as e:
            await message.reply_text(f"❌ **Invalid pattern:** {_esc(e)}")
            return
        except re.error as e:
            await message.reply_text(f"❌ **Invalid pattern:** `{_esc(e)}`")
            return
        async with _pattern_lock(user_id):
            existing = await load_patterns(user_id, _locked=True)
            if pattern in existing:
                await message.reply_text("ℹ️ This pattern is already saved.")
                return
            if len(existing) >= 30:
                await message.reply_text("❌ Max 30 patterns. Remove some with /del_pattern.")
                return
            try:
                await save_patterns(user_id, existing + [pattern])
            except Exception as e:
                await message.reply_text(
                    f"❌ **Could not save to the pattern channel:** `{_esc(str(e)[:150])}`\n\n"
                    "Make sure the bot is still an admin there."
                )
                return
        await message.reply_text(
            f"✅ **Pattern saved!** (#{len(existing) + 1})\n\n`{_esc(pattern)}`\n\n"
            "Now resend the file that failed."
        )
    except Exception as e:
        print(f"Error in add_pattern handler: {e}")

@app.on_message(filters.command("patterns") & filters.private)
async def patterns_handler(client, message):
    try:
        user_id = message.from_user.id
        pats = await load_patterns(user_id)
        src = await db.get_id_source(user_id)
        if not pats:
            await message.reply_text(
                f"📭 No saved patterns.\n**Identify from:** `{src}`\n\nUse `/add_pattern <pattern>`."
            )
            return
        text = f"📋 **Saved Patterns ({len(pats)})** — identify from: `{src}`\n\n"
        for i, p in enumerate(pats, 1):
            text += f"**{i}.** `{_esc(p)}`\n"
        text += "\nRemove: `/del_pattern <number>` or `/del_pattern all`"
        await message.reply_text(text)
    except Exception as e:
        print(f"Error in patterns handler: {e}")

@app.on_message(filters.command("del_pattern") & filters.private)
async def del_pattern_handler(client, message):
    try:
        user_id = message.from_user.id
        if len(message.command) < 2:
            await message.reply_text("**Usage:** `/del_pattern <number>` or `/del_pattern all`")
            return
        arg = message.command[1].lower()
        async with _pattern_lock(user_id):
            pats = await load_patterns(user_id, _locked=True)
            if arg == "all":
                try:
                    await save_patterns(user_id, [])
                except Exception as e:
                    await message.reply_text(f"❌ Could not update the pattern channel: `{_esc(str(e)[:150])}`")
                    return
                await message.reply_text(f"✅ Removed all {len(pats)} pattern(s).")
                return
            if not arg.isdigit() or not (1 <= int(arg) <= len(pats)):
                await message.reply_text("❌ Invalid number. See /patterns.")
                return
            new_list = list(pats)
            removed = new_list.pop(int(arg) - 1)
            try:
                await save_patterns(user_id, new_list)
            except Exception as e:
                await message.reply_text(f"❌ Could not update the pattern channel: `{_esc(str(e)[:150])}`")
                return
        await message.reply_text(f"✅ Removed: `{_esc(removed)}`")
    except Exception as e:
        print(f"Error in del_pattern handler: {e}")

@app.on_message(filters.command("identify_from") & filters.private)
async def identify_from_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) >= 2:
            src = message.command[1].lower()
            src = "filename" if src in ("filename", "file", "name") else src
            if src not in ("filename", "caption"):
                await message.reply_text("❌ Use `/identify_from filename` or `/identify_from caption`.")
                return
            await db.set_id_source(user_id, src)
            await message.reply_text(f"✅ Variables will now be identified from: `{src}`")
            return
        cur = await db.get_id_source(user_id)
        await message.reply_text(
            f"**🔍 Identify variables from**\n\nCurrent: `{cur}`\n\n"
            "Default patterns run first, then your saved patterns (/add_pattern).",
            reply_markup=_idsrc_buttons(cur)
        )
    except Exception as e:
        print(f"Error in identify_from handler: {e}")

@app.on_message(filters.command("failed_queue") & filters.private)
async def failed_queue_handler(client, message):
    user_id = message.from_user.id
    failed = _get_failed_queue(user_id)
    if not failed:
        await message.reply_text(
            "✅ **No files pending manual rename!**\n\n"
            "All your files have been processed successfully."
        )
        return
    text = f"📋 **Files Pending Manual Rename: {len(failed)}**\n\n"
    for i, item in enumerate(failed, 1):
        fname = item.get('original_file_name', 'Unknown')
        fsize = humanbytes(item.get('file_size', 0))
        failed_vars = ', '.join(item.get('failed_vars', []))
        text += f"**{i}.** `{fname[:50]}`\n"
        text += f"   Size: `{fsize}` | Failed: `{failed_vars}`\n\n"
    is_waiting = user_id in manual_rename_waiting
    if is_waiting:
        text += "\n⏳ _Bot is currently waiting for your filename input._"
    elif not _has_pending_queue_tasks(user_id):
        text += "\n💡 _Use /skip_failed to skip the current file or wait for the bot to prompt you._"
        await _prompt_next_manual_rename(user_id)
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭️ Skip Current", callback_data=f"skip_manual_{user_id}")],
        [InlineKeyboardButton("❌ Clear All Failed", callback_data=f"cancel_manual_{user_id}")]
    ])
    await message.reply_text(text, reply_markup=buttons)

@app.on_message(filters.command("skip_failed") & filters.private)
async def skip_failed_handler(client, message):
    user_id = message.from_user.id
    if user_id not in manual_rename_waiting:
        failed = _get_failed_queue(user_id)
        if failed:
            await message.reply_text(
                f"ℹ️ You have {len(failed)} file(s) in your failed queue, "
                "but the bot is not currently prompting you.\n"
                "Use /failed_queue to trigger the prompt."
            )
        else:
            await message.reply_text("✅ No files pending manual rename.")
        return
    waiting_info = manual_rename_waiting[user_id]
    failed_item = waiting_info['failed_item']
    del manual_rename_waiting[user_id]
    failed_list = _get_failed_queue(user_id)
    if failed_item in failed_list:
        failed_list.remove(failed_item)
    original_name = failed_item.get('original_file_name', 'Unknown')
    remaining = len(_get_failed_queue(user_id))
    await message.reply_text(
        f"⏭️ **Skipped:** `{original_name}`\n\n"
        f"**Remaining in failed queue:** `{remaining}`"
    )
    if remaining > 0:
        await asyncio.sleep(0.5)
        await _prompt_next_manual_rename(user_id)
    else:
        await message.reply_text("✅ **Failed queue is now empty!**")

@app.on_message(filters.command("stop_renaming") & filters.private)
async def stop_renaming_cmd(client, message):
    if message.from_user.id not in Config.ADMIN:
        return await message.reply_text("❌ Admin only command.")
    global renaming_paused
    renaming_paused = True
    await message.reply_text("⏸️ Renaming process stopped. Use /start_renaming to resume.")

@app.on_message(filters.command("start_renaming") & filters.private)
async def start_renaming_cmd(client, message):
    if message.from_user.id not in Config.ADMIN:
        return await message.reply_text("❌ Admin only command.")
    global renaming_paused
    renaming_paused = False
    await message.reply_text("▶️ Renaming process resumed.")

@app.on_message(filters.command("mediatype") & filters.private)
async def mediatype_handler(client, message):
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)
        if len(message.command) >= 2:
            media_type = message.command[1].lower()
            if media_type not in ["document", "video", "audio"]:
                await message.reply_text(
                    "**Invalid media type!**\n\n"
                    "**Available options:** document, video, audio\n"
                    "**Example:** `/mediatype video`"
                )
                return
            await db.set_media_preference(user_id, media_type)
            await message.reply_text(
                f"**✅ Output media type set to:** `{media_type}`\n\n"
                f"**Video files will keep their original extension.**\n"
                f"To change back, use `/mediatype document`."
            )
            return
        current = await db.get_media_preference(user_id)
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    f"{'✅ ' if current == 'document' else ''}Document",
                    callback_data="set_mediatype_document"
                ),
                InlineKeyboardButton(
                    f"{'✅ ' if current == 'video' else ''}Video",
                    callback_data="set_mediatype_video"
                ),
                InlineKeyboardButton(
                    f"{'✅ ' if current == 'audio' else ''}Audio",
                    callback_data="set_mediatype_audio"
                )
            ],
            [InlineKeyboardButton("🏠 Cancel", callback_data="home")]
        ])
        await message.reply_text(
            f"**🎬 Choose output media type**\n\n"
            f"Currently: `{current}`\n\n"
            "Tap a button to change:",
            reply_markup=buttons
        )
    except Exception as e:
        print(f"Error in mediatype handler: {e}")

@app.on_message(filters.command("queue") & filters.private)
async def queue_handler(client, message):
    try:
        user_id = message.from_user.id
        queue_info = processing_queue.get_queue_info()
        user_tasks = []
        for task in queue_info['waiting_list']:
            if task['user_id'] == user_id:
                user_tasks.append(task)
        if not user_tasks and queue_info['current'] and queue_info['current']['user_id'] != user_id:
            await message.reply_text(
                "**📭 You have no files in the queue!**\n\n"
                "Send any file to add it to the queue."
            )
            return
        response = "**📊 Your Queue Status**\n\n"
        if queue_info['current'] and queue_info['current']['user_id'] == user_id:
            current = queue_info['current']
            response += f"🔄 **Currently Processing:**\n"
            response += f"• `{current.get('file_name', 'Unknown')[:40]}`\n"
            if 'start_time' in current:
                elapsed = time.time() - current['start_time']
                response += f"• Processing for: `{TimeFormatter(elapsed*1000)}`\n"
            response += "\n"
        if user_tasks:
            response += f"**📋 Waiting in Queue:** `{len(user_tasks)}` files\n\n"
            for i, task in enumerate(user_tasks[:5]):
                wait_time_seconds = task.get('waiting_time', time.time() - task.get('added_time', 0))
                response += f"**#{task['position']}** - `{task['file_name'][:40]}`\n"
                response += f"  ⏱️ Waiting: `{TimeFormatter(wait_time_seconds*1000)}`\n\n"
            if len(user_tasks) > 5:
                response += f"... and {len(user_tasks) - 5} more files\n\n"
        failed = _get_failed_queue(user_id)
        if failed:
            response += f"⚠️ **Failed Queue (manual rename needed):** `{len(failed)}`\n"
            response += f"Use /failed_queue to view them.\n\n"
        response += f"\n**📈 Queue Statistics:**\n"
        response += f"• Total in queue: `{queue_info['total']}`\n"
        response += f"• Admin waiting: `{queue_info['admin_waiting']}`\n"
        response += f"• Users waiting: `{queue_info['user_waiting']}`\n"
        response += f"• Currently processing: `{'Yes' if queue_info['is_processing'] else 'No'}`\n"
        await message.reply_text(response)
    except Exception as e:
        print(f"Error in queue handler: {e}")

@app.on_message(filters.command("queue_stats") & filters.private)
async def queue_stats_handler(client, message):
    try:
        queue_info = processing_queue.get_queue_info()
        if queue_info['total'] == 0 and not queue_info['is_processing'] and queue_info['paused'] == 0:
            await message.reply_text("📭 **Queue is empty!**\nNo files in processing queue.")
            return
        status_text = "📊 **Queue Statistics**\n\n"
        admin_priority_status = "✅ **ENABLED**" if queue_info['admin_priority'] else "❌ **DISABLED**"
        status_text += f"**Admin Priority:** {admin_priority_status}\n"
        if queue_info['admin_mode']:
            status_text += "**Admin Mode:** 🚨 **ACTIVE**\n\n"
        else:
            status_text += "**Admin Mode:** ✅ **INACTIVE**\n\n"
        if queue_info['is_processing'] and queue_info.get('current'):
            current = queue_info['current']
            priority_text = "🚨 **ADMIN**" if current.get('is_admin') else "👤 **USER**"
            status_text += f"🔄 **Currently Processing ({priority_text}):**\n"
            status_text += f"   • `{current.get('file_name', 'Unknown')[:30]}`\n"
            status_text += f"   • User ID: `{current.get('user_id', 'Unknown')}`\n"
            if 'start_time' in current:
                elapsed = time.time() - current['start_time']
                status_text += f"   • Processing for: `{TimeFormatter(elapsed*1000)}`\n"
            status_text += "\n"
        status_text += f"📋 **Waiting in Queue:** `{queue_info['total']}` files\n"
        status_text += f"   • 👑 Admin: `{queue_info['admin_waiting']}`\n"
        status_text += f"   • 👤 Users: `{queue_info['user_waiting']}`\n"
        if message.from_user.id in queue_info['user_stats']:
            status_text += f"\n**Your Tasks in Queue:** `{queue_info['user_stats'][message.from_user.id]}`\n"
        failed = _get_failed_queue(message.from_user.id)
        if failed:
            status_text += f"\n⚠️ **Your Failed Queue:** `{len(failed)}` (need manual rename)\n"
        status_text += f"\n**Overall Statistics:**\n"
        status_text += f"• ✅ Completed: `{queue_info['completed']}`\n"
        status_text += f"• ❌ Failed: `{queue_info['failed']}`\n"
        status_text += f"• ⏸️ Paused: `{queue_info['paused']}`\n"
        await message.reply_text(status_text)
    except Exception as e:
        print(f"Error in queue_stats handler: {e}")

@app.on_message(filters.command("admin_priority_on") & filters.private)
async def admin_priority_on_handler(client, message):
    try:
        if message.from_user.id not in Config.ADMIN:
            await message.reply_text("❌ **Admin only command!**")
            return
        processing_queue.admin_priority_mode = True
        await message.reply_text(
            "✅ **Admin Priority Mode ENABLED!**\n\n"
            "Admin files will now be processed with highest priority.\n"
            "User files may be paused if admin files are added to queue."
        )
    except Exception as e:
        print(f"Error in admin_priority_on handler: {e}")

@app.on_message(filters.command("admin_priority_off") & filters.private)
async def admin_priority_off_handler(client, message):
    try:
        if message.from_user.id not in Config.ADMIN:
            await message.reply_text("❌ **Admin only command!**")
            return
        processing_queue.admin_priority_mode = False
        await message.reply_text(
            "✅ **Admin Priority Mode DISABLED!**\n\n"
            "All files will now be processed in normal queue order."
        )
    except Exception as e:
        print(f"Error in admin_priority_off handler: {e}")

@app.on_message(filters.command("clear_queue") & filters.private)
async def clear_queue_handler(client, message):
    """ADMIN: stop the running task and clear every queue (all users, incl. admin)."""
    try:
        admin_id = message.from_user.id
        if admin_id not in Config.ADMIN:
            await message.reply_text("❌ **Admin only command!**")
            return
        users, removed, stopped = processing_queue.clear_all()
        failed_cleared = sum(len(v) for v in failed_rename_queue.values())
        failed_rename_queue.clear()
        manual_rename_waiting.clear()
        await message.reply_text(
            f"✅ **Everything cleared!**\n\n"
            f"• Waiting tasks removed: `{removed}`\n"
            f"• Running task stopped: `{'Yes' if stopped else 'No'}`\n"
            f"• Failed/manual-rename entries cleared: `{failed_cleared}`\n"
            f"• Users affected: `{len(users)}`"
        )
        for uid in users:
            if uid == admin_id:
                continue
            try:
                await app.send_message(uid, "⚠️ **Your queued/processing files were cleared by an admin.**")
            except Exception:
                pass
    except Exception as e:
        print(f"Error in clear_queue handler: {e}")

@app.on_message(filters.command("clear_queue_user") & filters.private)
async def clear_queue_user_handler(client, message):
    """EVERYONE: clears the caller's own waiting files (admins too).
    Admins may pass a user id to clear someone else's."""
    try:
        user_id = message.from_user.id
        target_id = user_id
        if len(message.command) > 1:
            if user_id not in Config.ADMIN:
                await message.reply_text("❌ Only admins can clear other users' queue.")
                return
            try:
                target_id = int(message.command[1])
            except ValueError:
                await message.reply_text("❌ Invalid user ID.")
                return
        removed_count, has_current = await processing_queue.clear_user_queue(target_id)
        if removed_count == 0 and not has_current:
            await message.reply_text("❌ No pending files in queue.")
            return
        response = ""
        if removed_count > 0:
            response += f"✅ Cleared `{removed_count}` waiting file(s) from the queue.\n"
        if has_current:
            response += "ℹ️ A file is being processed right now — tap **❌ Cancel** on its status message to stop it."
        await message.reply_text(response)
    except Exception as e:
        print(f"Error in clear_queue_user handler: {e}")

@app.on_message(filters.command("stats") & filters.private)
async def stats_handler(client, message):
    try:
        if message.from_user.id not in Config.ADMIN:
            await message.reply_text("❌ **Admin only command!**")
            return
        uptime_seconds = time.time() - Config.BOT_UPTIME
        uptime_str = str(timedelta(seconds=int(uptime_seconds)))
        total_users = await db.total_users_count()
        queue_info = processing_queue.get_queue_info()
        total_failed_pending = sum(len(v) for v in failed_rename_queue.values())
        stats_text = f"""
📊 **Bot Statistics**

🤖 **Bot Info:**
• Uptime: `{uptime_str}`
• Pyrogram Version: `{__version__}`
• Total Users: `{total_users}`

👑 **Admin Priority:**
• Status: `{"✅ ENABLED" if processing_queue.admin_priority_mode else "❌ DISABLED"}`

📋 **Queue Status:**
• Currently Processing: `{"Yes" if queue_info['is_processing'] else "No"}`
• Waiting in Queue: `{queue_info['total']}`
• Admin Waiting: `{queue_info['admin_waiting']}`
• Users Waiting: `{queue_info['user_waiting']}`
• Failed (Manual Rename Pending): `{total_failed_pending}`

📈 **Task History:**
• Completed Tasks: `{queue_info['completed']}`
• Failed Tasks: `{queue_info['failed']}`
• Total Processed: `{queue_info['completed'] + queue_info['failed']}`
"""
        await message.reply_text(stats_text)
    except Exception as e:
        print(f"Error in stats handler: {e}")

@app.on_message(filters.command("sequence_mode") & filters.private)
async def sequence_mode_handler(client, message):
    uid = message.from_user.id
    await db.add_user(uid)
    MODES = {
        1: "Season → Quality → Episode",
        2: "Season → Episode → Quality",
        3: "Quality → Season → Episode",
    }
    if len(message.command) == 1:
        current_mode = await db.get_sequence_mode(uid)
        await message.reply_text(
            f"🔄 **Current Sequence Mode:** `{current_mode}` — {MODES[current_mode]}\n\n"
            "**Available modes:**\n"
            "• Mode 1: Season → Quality → Episode\n"
            "• Mode 2: Season → Episode → Quality\n"
            "• Mode 3: Quality → Season → Episode\n\n"
            "Use `/sequence_mode [1|2|3]` to change."
        )
        return
    try:
        mode = int(message.command[1])
    except ValueError:
        await message.reply_text("❌ Use 1, 2, or 3.")
        return
    if mode not in (1, 2, 3):
        await message.reply_text("❌ Use 1, 2, or 3.")
        return
    await db.set_sequence_mode(uid, mode)
    await message.reply_text(f"✅ Sequence mode set to **{mode}** — {MODES[mode]}")

@app.on_message(filters.command("ssequence") & filters.private)
async def ssequence_handler(client, message):
    user_id = message.from_user.id
    if user_id in sequence_sessions and sequence_sessions[user_id].get('active'):
        await message.reply_text("⚠️ You are already in sequence mode. Send files or use /esequence to finish.")
        return
    sequence_sessions[user_id] = {'active': True, 'files': [], 'sorted_files': []}
    await message.reply_text(
        "🔄 **Sequence mode started!**\n\n"
        "Send me all the files you want to sort. They will be saved and **not** processed yet.\n\n"
        "When done, use `/esequence` to choose sort mode and sort them.\n\n"
        "Use `/sequence_mode` to view/set your default sort mode."
    )

@app.on_message(filters.command("esequence") & filters.private)
async def esequence_handler(client, message):
    user_id = message.from_user.id
    if user_id not in sequence_sessions or not sequence_sessions[user_id].get('active'):
        await message.reply_text("❌ You are not in sequence mode. Start with `/ssequence` first.")
        return
    session = sequence_sessions[user_id]
    session['active'] = False
    files = session['files']
    count = len(files)
    if count == 0:
        del sequence_sessions[user_id]
        await message.reply_text("⚠️ No files were saved. Sequence mode cancelled.")
        return
    btns = InlineKeyboardMarkup([
        [InlineKeyboardButton("1️⃣ Season → Quality → Episode", callback_data=f"sort_seq_1_{user_id}")],
        [InlineKeyboardButton("2️⃣ Season → Episode → Quality", callback_data=f"sort_seq_2_{user_id}")],
        [InlineKeyboardButton("3️⃣ Quality → Season → Episode", callback_data=f"sort_seq_3_{user_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_seq")]
    ])
    await message.reply_text(
        f"📁 **{count} files saved.**\n\n"
        "Choose a **sort mode**:",
        reply_markup=btns
    )

@app.on_callback_query()
async def callback_query_handler(client, callback_query):
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data

        if data.startswith("skip_manual_"):
            parts = data.split("_")
            target_uid = int(parts[2])
            if user_id != target_uid:
                await callback_query.answer("This is not for you.", show_alert=True)
                return
            if target_uid not in manual_rename_waiting:
                failed = _get_failed_queue(target_uid)
                if failed:
                    await callback_query.answer("Triggering manual rename prompt...", show_alert=False)
                    await _prompt_next_manual_rename(target_uid)
                else:
                    await callback_query.answer("No files pending manual rename.", show_alert=True)
                return
            waiting_info = manual_rename_waiting[target_uid]
            failed_item = waiting_info['failed_item']
            del manual_rename_waiting[target_uid]
            failed_list = _get_failed_queue(target_uid)
            if failed_item in failed_list:
                failed_list.remove(failed_item)
            original_name = failed_item.get('original_file_name', 'Unknown')
            remaining = len(_get_failed_queue(target_uid))
            try:
                await callback_query.message.edit_text(
                    f"⏭️ **Skipped:** `{original_name}`\n"
                    f"**Remaining:** `{remaining}`"
                )
            except:
                pass
            await callback_query.answer("Skipped!")
            if remaining > 0:
                await asyncio.sleep(0.5)
                await _prompt_next_manual_rename(target_uid)
            else:
                try:
                    await app.send_message(target_uid, "✅ **Failed queue is now empty!**")
                except:
                    pass
        elif data.startswith("cancel_manual_"):
            parts = data.split("_")
            target_uid = int(parts[2])
            if user_id != target_uid:
                await callback_query.answer("This is not for you.", show_alert=True)
                return
            if target_uid in manual_rename_waiting:
                del manual_rename_waiting[target_uid]
            count = len(_get_failed_queue(target_uid))
            if target_uid in failed_rename_queue:
                del failed_rename_queue[target_uid]
            try:
                await callback_query.message.edit_text(
                    f"❌ **Cancelled all {count} manual rename(s).**\n\n"
                    "Files have been removed from the failed queue."
                )
            except:
                pass
            await callback_query.answer("All manual renames cancelled.")

        elif data.startswith("sort_seq_"):
            parts = data.split("_")
            if len(parts) != 4:
                await callback_query.answer("Invalid data.")
                return
            mode = int(parts[2])
            session_user_id = int(parts[3])
            if user_id != session_user_id:
                await callback_query.answer("This is not for you.", show_alert=True)
                return
            if session_user_id not in sequence_sessions or not sequence_sessions[session_user_id].get('files'):
                await callback_query.answer("No files found.", show_alert=True)
                return

            files = sequence_sessions[session_user_id]['files']
            id_source = await db.get_id_source(user_id)
            id_patterns = await load_patterns(user_id)
            sorted_files = sorted(files, key=lambda m: sort_key_for_mode(m, mode, id_source, id_patterns))
            sequence_sessions[session_user_id]['sorted_files'] = sorted_files
            sequence_sessions[session_user_id]['mode'] = mode

            try:
                await callback_query.message.edit_text("🔀 **Sorting files... please wait.**")
            except:
                pass

            for msg in sorted_files:
                try:
                    await msg.copy(chat_id=user_id)
                    await asyncio.sleep(0.3)
                except Exception as e:
                    print(f"Failed to forward file during sorting: {e}")

            summary = generate_sort_summary(sorted_files, mode, id_source, id_patterns)
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Add to Queue (in order)", callback_data=f"enqueue_seq_{session_user_id}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_seq")]
            ])
            try:
                await client.send_message(
                    chat_id=user_id,
                    text=f"📁 **{len(sorted_files)} files sorted.**\n\n{summary}",
                    reply_markup=buttons,
                    disable_web_page_preview=True
                )
                await callback_query.message.delete()
            except Exception as e:
                print(f"Sort summary send error: {e}")
            await callback_query.answer("Sorted!")

        elif data.startswith("enqueue_seq_"):
            parts = data.split("_")
            if len(parts) < 3:
                return
            session_user_id = int(parts[2])
            if user_id != session_user_id:
                await callback_query.answer("This is not for you.", show_alert=True)
                return
            if session_user_id not in sequence_sessions or 'sorted_files' not in sequence_sessions[session_user_id]:
                await callback_query.answer("No sorted files to enqueue.", show_alert=True)
                return
            sorted_files = sequence_sessions[session_user_id]['sorted_files']
            count = len(sorted_files)
            # Enqueue in the exact order they appear in sorted_files.
            for position, msg in enumerate(sorted_files):
                processing_queue.add_to_queue(msg, session_user_id)
            del sequence_sessions[session_user_id]
            await callback_query.message.edit_text(
                f"✅ **{count} files added to the processing queue in sorted order!**\n\n"
                "They will be processed one by one.",
                reply_markup=None
            )
            await callback_query.answer("Added to queue!")
        elif data == "cancel_seq":
            if user_id in sequence_sessions:
                del sequence_sessions[user_id]
            await callback_query.message.edit_text("❌ Sequence session cancelled.")
            await callback_query.answer("Cancelled.")

        elif data.startswith("cancel_task_"):
            tid = data[len("cancel_task_"):]
            task = processing_queue.task_map.get(tid)
            if task is None and processing_queue.current_task_id == tid:
                task = processing_queue.current_task
            if task is None:
                await callback_query.answer("This task is already finished.", show_alert=True)
                try:
                    await callback_query.message.edit_reply_markup(None)
                except Exception:
                    pass
                return
            if user_id != task['user_id'] and user_id not in Config.ADMIN:
                await callback_query.answer("This is not your task.", show_alert=True)
                return
            result = processing_queue.cancel_task(tid)
            if result == 'running':
                await callback_query.answer("Cancelling…")
            elif result == 'queued':
                await callback_query.answer("Removed from queue.")
                try:
                    await callback_query.message.edit_text("🚫 **Removed from queue.**", reply_markup=None)
                except Exception:
                    pass
            else:
                await callback_query.answer("This task is already finished.", show_alert=True)
                try:
                    await callback_query.message.edit_reply_markup(None)
                except Exception:
                    pass

        elif data.startswith("set_mediatype_"):
            media = data.split("_")[2]
            if media not in ["document", "video", "audio"]:
                await callback_query.answer("Invalid type")
                return
            await db.set_media_preference(user_id, media)
            await callback_query.answer(f"Media type set to {media}", show_alert=True)
            current = await db.get_media_preference(user_id)
            buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(f"{'✅ ' if current == 'document' else ''}Document", callback_data="set_mediatype_document"),
                    InlineKeyboardButton(f"{'✅ ' if current == 'video' else ''}Video", callback_data="set_mediatype_video"),
                    InlineKeyboardButton(f"{'✅ ' if current == 'audio' else ''}Audio", callback_data="set_mediatype_audio")
                ],
                [InlineKeyboardButton("🏠 Cancel", callback_data="home")]
            ])
            await callback_query.message.edit_text(
                f"**🎬 Output media type updated!**\n\nCurrent: `{current}`",
                reply_markup=buttons
            )

        elif data.startswith("set_idsrc_"):
            src = data.split("_", 2)[2]
            if src not in ("filename", "caption"):
                await callback_query.answer("Invalid")
                return
            await db.set_id_source(user_id, src)
            await callback_query.answer(f"Identifying from {src}", show_alert=True)
            try:
                await callback_query.message.edit_text(
                    f"**🔍 Identify variables from**\n\nCurrent: `{src}`",
                    reply_markup=_idsrc_buttons(src)
                )
            except Exception:
                pass

        else:
            if callback_query.data == 'help':
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')],
                    [InlineKeyboardButton("⚙️ ᴍᴇᴛᴀᴅᴀᴛᴀ", callback_data='metadata'),
                     InlineKeyboardButton("📊 Queue", callback_data='queue_status')],
                    [InlineKeyboardButton("👑 Admin Priority", callback_data='admin_priority')]
                ])
                try:
                    await callback_query.message.edit_text(
                        Txt.HELP_TXT, reply_markup=buttons, disable_web_page_preview=True
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        Txt.HELP_TXT, reply_markup=buttons, disable_web_page_preview=True
                    )
            elif callback_query.data == 'home':
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📚 ʜᴇʟᴘ", callback_data='help'),
                     InlineKeyboardButton("⚙️ ᴍᴇᴛᴀᴅᴀᴛᴀ", callback_data='metadata')],
                    [
                        InlineKeyboardButton('📢 ᴜᴘᴅᴀᴛᴇs', url='https://t.me/AnimeMultiDub'),
                        InlineKeyboardButton('🆘 sᴜᴘᴘᴏʀᴛ', url='https://t.me/AnimeMultiDub')
                    ],
                    [
                        InlineKeyboardButton('📊 Queue Stats', callback_data='queue_status'),
                        InlineKeyboardButton('👑 Admin Priority', callback_data='admin_priority')
                    ]
                ])
                try:
                    await callback_query.message.edit_text(
                        Txt.START_TXT.format(callback_query.from_user.mention), reply_markup=buttons
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        Txt.START_TXT.format(callback_query.from_user.mention), reply_markup=buttons
                    )
            elif callback_query.data == 'metadata':
                metadata_enabled = await db.get_metadata(user_id)
                status = "✅ **ENABLED**" if metadata_enabled else "❌ **DISABLED**"
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔧 View Metadata", callback_data='view_metadata')],
                    [InlineKeyboardButton(f"{'❌ Disable' if metadata_enabled else '✅ Enable'} Metadata",
                                          callback_data='toggle_metadata')],
                    [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')]
                ])
                try:
                    await callback_query.message.edit_text(
                        f"**⚙️ Metadata Settings**\n\n**Current Status:** {status}\n\nMetadata adds title, author, artist, and other info to your files.\n"
                        f"How to set metadata : use /settitle /setauthor /setartist /setaudio /setsubtitle /setvideo /setallmeta\n"
                        f"Example: `/setaudio @AnimeMultiDub`",
                        reply_markup=buttons
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        f"**⚙️ Metadata Settings**\n\n**Current Status:** {status}\n\nMetadata adds title, author, artist, and other info to your files.\n"
                        f"How to set metadata : use /settitle /setauthor /setartist /setaudio /setsubtitle /setvideo /setallmeta\n"
                        f"Example: `/setaudio @AnimeMultiDub`",
                        reply_markup=buttons
                    )
            elif callback_query.data == 'toggle_metadata':
                current = await db.get_metadata(user_id)
                new_value = not current
                await db.set_metadata(user_id, new_value)
                status = "✅ **ENABLED**" if new_value else "❌ **DISABLED**"
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔧 View Metadata", callback_data='view_metadata')],
                    [InlineKeyboardButton(f"{'❌ Disable' if new_value else '✅ Enable'} Metadata",
                                          callback_data='toggle_metadata')],
                    [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')]
                ])
                try:
                    await callback_query.message.edit_text(
                        f"**✅ Metadata setting updated!**\n\n**New Status:** {status}",
                        reply_markup=buttons
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        f"**✅ Metadata setting updated!**\n\n**New Status:** {status}",
                        reply_markup=buttons
                    )
            elif callback_query.data == 'view_metadata':
                title = await db.get_title(user_id)
                author = await db.get_author(user_id)
                artist = await db.get_artist(user_id)
                audio = await db.get_audio(user_id)
                subtitle = await db.get_subtitle(user_id)
                video = await db.get_video(user_id)
                metadata_enabled = await db.get_metadata(user_id)
                status = "✅ **ENABLED**" if metadata_enabled else "❌ **DISABLED**"
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ Back to Settings", callback_data='metadata')],
                    [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')]
                ])
                try:
                    await callback_query.message.edit_text(
                        f"**📊 Your Metadata Settings**\n\n**Metadata Status:** {status}\n\n"
                        f"**Title:** `{title}`\n**Author:** `{author}`\n**Artist:** `{artist}`\n"
                        f"**Audio:** `{audio}`\n**Subtitle:** `{subtitle}`\n**Video:** `{video}`",
                        reply_markup=buttons
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        f"**📊 Your Metadata Settings**\n\n**Metadata Status:** {status}\n\n"
                        f"**Title:** `{title}`\n**Author:** `{author}`\n**Artist:** `{artist}`\n"
                        f"**Audio:** `{audio}`\n**Subtitle:** `{subtitle}`\n**Video:** `{video}`",
                        reply_markup=buttons
                    )
            elif callback_query.data == 'queue_status':
                await callback_query.answer()
                await queue_stats_handler(client, callback_query.message)
            elif callback_query.data == 'admin_priority':
                if user_id not in Config.ADMIN:
                    await callback_query.answer("❌ Admin only feature!", show_alert=True)
                    return
                status = "✅ **ENABLED**" if processing_queue.admin_priority_mode else "❌ **DISABLED**"
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{'❌ Disable' if processing_queue.admin_priority_mode else '✅ Enable'} Priority",
                                          callback_data='toggle_admin_priority')],
                    [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')]
                ])
                try:
                    await callback_query.message.edit_text(
                        f"**👑 Admin Priority Settings**\n\n**Current Status:** {status}",
                        reply_markup=buttons
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        f"**👑 Admin Priority Settings**\n\n**Current Status:** {status}",
                        reply_markup=buttons
                    )
            elif callback_query.data == 'toggle_admin_priority':
                if user_id not in Config.ADMIN:
                    await callback_query.answer("❌ Admin only feature!", show_alert=True)
                    return
                processing_queue.admin_priority_mode = not processing_queue.admin_priority_mode
                status = "✅ **ENABLED**" if processing_queue.admin_priority_mode else "❌ **DISABLED**"
                buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{'❌ Disable' if processing_queue.admin_priority_mode else '✅ Enable'} Priority",
                                          callback_data='toggle_admin_priority')],
                    [InlineKeyboardButton("🏠 ʜᴏᴍᴇ", callback_data='home')]
                ])
                try:
                    await callback_query.message.edit_text(
                        f"**✅ Admin Priority updated!**\n\n**New Status:** {status}",
                        reply_markup=buttons
                    )
                except Exception:
                    await callback_query.message.delete()
                    await callback_query.message.reply_text(
                        f"**✅ Admin Priority updated!**\n\n**New Status:** {status}",
                        reply_markup=buttons
                    )
            await callback_query.answer()
    except Exception as e:
        print(f"Error in callback handler: {e}")

async def main():
    await app.start()
    await db.init_db()
    asyncio.create_task(queue_worker())
    me = await app.get_me()
    print(f"✅ Bot started as @{me.username}")
    print(f"✅ Bot ID: {me.id}")
    print("✅ Bot is ready. Files can be sent in private chat.")
    print("✅ Simplified build: no group-share, no encode/compress, no dump channel, no torrent downloader.")
    try:
        await app.send_message(
            Config.LOG_CHANNEL,
            f"🤖 **Bot Started Successfully!**\n\n"
            f"**Name:** {me.first_name}\n"
            f"**Username:** @{me.username}\n"
            f"**ID:** `{me.id}`\n"
            f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"**Build:** Simplified (rename + metadata + thumbnail + sequence sort + custom patterns)."
        )
    except:
        pass
    await idle()
    await app.stop()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True, check=True)
        print("✅ FFmpeg is installed and working")
    except:
        print("⚠️ WARNING: FFmpeg not found! Metadata embedding will not work.")
    print("\n" + "="*60)
    print("🚀 Starting Simplified Auto Rename Bot...")
    print("="*60)
    print(f"👑 Admins: {Config.ADMIN}")
    print(f"👷 Queue System: ACTIVE")
    print(f"📦 Max File Size: {humanbytes(Config.MAX_FILE_SIZE)}")
    print("👑 Admin Priority: DISABLED by default")
    print("⏸️ Renaming control: /stop_renaming, /start_renaming")
    print("🔄 Sequence: 3 sort modes — /ssequence, /esequence, /sequence_mode [1|2|3]")
    print("🔢 Counter: /count [number] to view/set {count} starting value")
    print("🧩 Patterns: /add_pattern, /patterns, /del_pattern, /identify_from")
    print("🤖 Bot is running. Press Ctrl+C to stop.")
    print("="*60 + "\n")
    try:
        app.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
