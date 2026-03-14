#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minecraft 地图玩家数据迁移器（单文件版）

特性
- 扫描存档中所有 UUID 命名的玩家相关文件
- 识别 online / offline UUID
- 对话式终端菜单
- 彩色 UI + 进度条
- 未匹配玩家列表与手动补全
- 联网再次尝试获取玩家 UUID
- 每次切换前自动备份
- 备份保存在 ./switch_data/<时间戳_操作名>/...
- 一键切换所有玩家在线 / 离线
- 一键恢复指定备份

默认假设：脚本运行目录就是 Minecraft 地图根目录。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from urllib.request import urlopen, Request, quote
    from urllib.error import HTTPError, URLError
except Exception:  # pragma: no cover
    urlopen = None
    Request = None
    quote = None
    HTTPError = Exception
    URLError = Exception

ROOT = Path.cwd()
DATA_DIR = ROOT / "switch_data"
SWITCH_DIR = ROOT / "switch_data"
PLAYER_JSON = SWITCH_DIR / "player.json"

SUPPORTED_EXTS = {".nbt", ".json", ".dat", ".dat_old", ".snbt", ".toml"}
COMMON_PLAYER_DIRS = {"playerdata", "advancements", "stats", "poi", "region"}
NAME_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")


class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"


def supports_color() -> bool:
    return sys.stdout.isatty() and not os.environ.get("NO_COLOR")


USE_COLOR = supports_color()


def col(text: str, code: str, bold: bool = False) -> str:
    if not USE_COLOR:
        return text
    return f"{C.BOLD if bold else ''}{code}{text}{C.RESET}"


def hr(ch: str = "═", width: int = 72) -> str:
    return ch * width


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def ask(prompt: str) -> str:
    return input(col(prompt, C.BRIGHT_CYAN)).strip()


def ask_yes_no(prompt: str, default: bool = False) -> bool:
    tip = " [Y/n]: " if default else " [y/N]: "
    s = ask(prompt + tip).lower()
    if not s:
        return default
    return s in {"y", "yes", "1", "true", "t", "是"}


def pause():
    input(col("按回车继续...", C.BRIGHT_BLACK))


def log_info(msg: str):
    print(col(f"[信息] {msg}", C.BRIGHT_CYAN))


def log_ok(msg: str):
    print(col(f"[完成] {msg}", C.BRIGHT_GREEN))


def log_warn(msg: str):
    print(col(f"[警告] {msg}", C.BRIGHT_YELLOW))


def log_err(msg: str):
    print(col(f"[错误] {msg}", C.BRIGHT_RED))


def banner():
    clear()
    print(col(hr(), C.BRIGHT_BLUE))
    print(col("Minecraft 地图玩家数据迁移器".center(72), C.BRIGHT_CYAN, bold=True))
    print(col("单文件 · 彩色终端 · 扫描 / 备份 / 切换 / 恢复".center(72), C.BRIGHT_BLACK))
    print(col(hr(), C.BRIGHT_BLUE))


def normalize_uuid(u: str) -> str:
    u = u.strip().lower().replace("-", "")
    if len(u) != 32 or any(ch not in "0123456789abcdef" for ch in u):
        raise ValueError(f"非法 UUID: {u}")
    return u


def uuid_hyphen(u: str) -> str:
    u = normalize_uuid(u)
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"


def is_uuid_like(s: str) -> bool:
    try:
        normalize_uuid(s)
        return True
    except Exception:
        return False


def offline_uuid_from_name(name: str) -> str:
    # Minecraft 离线 UUID 算法：UUID.nameUUIDFromBytes("OfflinePlayer:"+name)
    raw = ("OfflinePlayer:" + name).encode("utf-8")
    digest = hashlib.md5(raw).digest()
    b = bytearray(digest)
    b[6] = (b[6] & 0x0F) | 0x30
    b[8] = (b[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(b)).hex


def file_uuid_and_ext(p: Path) -> Optional[Tuple[str, str]]:
    name = p.name
    low = name.lower()
    for ext in sorted(SUPPORTED_EXTS, key=len, reverse=True):
        if low.endswith(ext):
            base = name[:-len(ext)]
            if is_uuid_like(base):
                return normalize_uuid_to_write(base), ext
    return None


def relpath_str(p: Path) -> str:
    return p.relative_to(ROOT).as_posix()


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SWITCH_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class PlayerRecord:
    name: str
    uuid: str
    online_uuid: Optional[str] = None
    offline_uuid: Optional[str] = None
    matched: bool = False
    last_scan: Optional[float] = None
    size: int = 0
    file_name: str = ""
    source: str = "scanned"


@dataclass
class FoundFile:
    name: str
    path: str
    uuid: str
    ext: str
    dir_kind: str
    size: int


def load_player_db() -> Dict[str, PlayerRecord]:
    ensure_dirs()
    if not PLAYER_JSON.exists():
        return {}
    try:
        raw = json.loads(PLAYER_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[str, PlayerRecord] = {}
    items = raw.get("players") if isinstance(raw, dict) else None
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                try:
                    rec = PlayerRecord(**item)
                    out[rec.name] = rec
                except Exception:
                    continue
    elif isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                try:
                    rec = PlayerRecord(**v)
                    out[rec.name] = rec
                except Exception:
                    continue
    return out


def save_player_db(records: Dict[str, PlayerRecord]) -> None:
    ensure_dirs()
    payload = {
        "version": 1,
        "updated_at": time.time(),
        "players": [asdict(v) for v in sorted(records.values(), key=lambda x: x.name.lower())],
    }
    PLAYER_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def progress_bar(cur: int, total: int, width: int = 28) -> str:
    total = max(total, 1)
    filled = int(width * cur / total)
    return "█" * filled + "░" * (width - filled)


def show_progress(cur: int, total: int, label: str):
    bar = progress_bar(cur, total)
    pct = cur * 100 / max(total, 1)
    print(f"\r{col(label, C.BRIGHT_CYAN)} {col(bar, C.BRIGHT_GREEN)} {pct:6.2f}% ({cur}/{total})", end="", flush=True)


def UUID(ID, isFull=False):
    """
    保留你提供的接口形式。
    返回 [username, uuid]，uuid 为 raw_id 或 full id。
    """
    if urlopen is None:
        return [ID, 'The name does not exist. ']

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    }
    req = Request('https://playerdb.co/api/player/minecraft/' + quote(ID), headers=headers)

    try:
        resp = urlopen(req, timeout=10)
    except (HTTPError, URLError):
        return [ID, 'The name does not exist. ']

    try:
        result = json.loads(resp.read().decode("utf-8"))
        if not result.get("success"):
            return [ID, 'The name does not exist. ']
        player = result["data"]["player"]
        return [player["username"], player["id"] if isFull else player["raw_id"]]
    except Exception:
        return [ID, 'The name does not exist. ']


def fetch_player_uuid(name: str) -> Optional[Tuple[str, str]]:
    got = UUID(name, isFull=False)
    if not isinstance(got, list) or len(got) < 2:
        return None
    uname, uid = got[0], got[1]
    if uid == 'The name does not exist. ':
        return None
    try:
        return uname, normalize_uuid(str(uid))
    except Exception:
        return None


def scan_uuid_files() -> List[FoundFile]:
    """
    扫描当前目录中所有文件名包含 UUID 且扩展名在支持列表中的文件。
    排除 switch_data 与 data 自身。
    """
    all_files = [p for p in ROOT.rglob("*") if p.is_file()]
    filtered = []
    for p in all_files:
        rel = relpath_str(p)
        if rel.startswith("switch_data/") or rel.startswith("data/"):
            continue
        filtered.append(p)

    result: List[FoundFile] = []
    total = len(filtered)
    for i, p in enumerate(filtered, 1):
        show_progress(i, total, "扫描 UUID 文件")
        parsed = file_uuid_and_ext(p)
        if not parsed:
            continue
        uid, ext = parsed
        kind = "other"
        size = p.stat().st_size
        for part in p.relative_to(ROOT).parts:
            if part in COMMON_PLAYER_DIRS:
                kind = part
                break
        result.append(FoundFile(name=p.name, path=relpath_str(p), uuid=uid, ext=ext, dir_kind=kind, size=size))
    if total:
        print()
    return result


def group_by_uuid(files: List[FoundFile]) -> Dict[str, List[FoundFile]]:
    m: Dict[str, List[FoundFile]] = {}
    for f in files:
        m.setdefault(f.uuid, []).append(f)
    return m

def scan_and_build_index(records: Dict[str, PlayerRecord]) -> Tuple[Dict[str, PlayerRecord], List[FoundFile]]:
    banner()
    log_info("开始扫描存档目录。")
    files = scan_uuid_files()
    grouped = group_by_uuid(files)

    merged = dict(records)

    # 对扫描到的 UUID 建立占位记录
    for uid, fset in grouped.items():
        existing = None
        size = 0
        for r in fset:
           if r.name.lower().endswith(".nbt"):
               size = r.size
        for r in merged.values():
            if is_uuid_like(r.uuid) and normalize_uuid(r.uuid) == uid:
                existing = r
                break
        if existing:
            existing.last_scan = time.time()
            continue

        merged[uid] = PlayerRecord(
            name=uid,
            uuid=uid,
            matched=False,
            last_scan=time.time(),
            source="scanned",
            size=size,
            offline_uuid=uid
        )

    # 尝试刷新在线/离线标识
    for r in merged.values():

        # 如果当前记录的 uuid 看起来合法
        if is_uuid_like(r.uuid):

            # ---------- 新增：先查询 playerdb ----------
            try:
                name, raw_uuid = UUID(r.uuid)
            except Exception:
                name, raw_uuid = None, None

            # 如果查询成功
            if raw_uuid and is_uuid_like(raw_uuid):
                r.name = name
                r.online_uuid = normalize_uuid(raw_uuid)
                r.offline_uuid = offline_uuid_from_name(r.name)
                r.matched = True

    save_player_db(merged)
    return merged, files


def list_players(records: Dict[str, PlayerRecord]) -> None:
    print()
    print(col("玩家列表", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))

    players = sorted(records.values(), key=lambda x: x.name.lower())
    print_player_table(players)


def list_unmatched(records: Dict[str, PlayerRecord]) -> List[PlayerRecord]:
    unmatched = [
        r for r in records.values()
        if not r.matched or not r.name or r.name == r.uuid
    ]

    items = sorted(unmatched, key=lambda x: x.uuid.lower())

    print_player_table(items)

    return items


def manual_link_player(records: Dict[str, PlayerRecord]) -> Dict[str, PlayerRecord]:

    items = list_unmatched(records)
    if not items:
        pause()
        return records
    print()
    print(col("手动补全玩家名", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))
    print(col("键入0返回", C.BRIGHT_YELLOW))

    sel = ask("选择玩家: ")
    if sel == "0":
        return records

    if not sel.isdigit() or not (1 <= int(sel) <= len(items)):
        log_warn("输入无效。")
        pause()
        return records

    r = items[int(sel) - 1]

    print(col(hr(), C.BRIGHT_BLUE))
    print(col(f"UUID: {r.uuid}", C.BRIGHT_CYAN, bold=True))

    name = ask("输入玩家名（留空取消）: ")
    if not name:
        return records

    if not NAME_RE.match(name):
        log_warn("玩家名格式不合法，应为 3~16 位字母、数字或下划线。")
        pause()
        return records

    fetched = fetch_player_uuid(name)

    r.name = name
    r.matched = True
    r.source = "manual"
    r.last_scan = time.time()

    if fetched:
        uname, uid = fetched

        old_uuid = r.uuid
        new_uuid = normalize_uuid(uid)

        r.name = uname
        r.online_uuid = new_uuid
        r.offline_uuid = old_uuid
        r.mode = "online"

        if old_uuid in records:
            del records[old_uuid]

        records[new_uuid] = r

        merge_player_records(records, r)

        log_ok(f"已联网获取：{uname} -> {new_uuid}")
    else:
        r.offline_uuid = offline_uuid_from_name(name)

        if normalize_uuid(r.uuid) == r.offline_uuid:
            r.mode = "offline"
        else:
            r.mode = "unknown"

        log_warn("网络未获取到 UUID，已先写入离线 UUID 推测值。")

    save_player_db(records)

    return records


def create_backup_session(action: str) -> Path:
    ensure_dirs()
    stamp = time.strftime("%Y%m%d_%H%M%S")
    base = SWITCH_DIR / f"{stamp}_{action}"
    path = base
    idx = 1
    while path.exists():
        idx += 1
        path = SWITCH_DIR / f"{stamp}_{action}_{idx}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def copy_to_backup(src: Path, backup_root: Path) -> Path:
    rel = src.relative_to(ROOT)
    dst = backup_root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return dst


def backup_files(files: Iterable[Path], action: str) -> Optional[Path]:
    files = [p for p in files if p.exists() and p.is_file()]
    if not files:
        return None
    backup_root = create_backup_session(action)
    total = len(files)
    for i, src in enumerate(files, 1):
        show_progress(i, total, "备份中")
        copy_to_backup(src, backup_root)
    print()
    return backup_root


def collect_related_files_by_uuid(uuids: Iterable[str]) -> List[Path]:
    target = {normalize_uuid_to_write(u) for u in uuids if is_uuid_like(u)}
    if not target:
        return []

    result: List[Path] = []
    for p in ROOT.rglob("*"):
        if not p.is_file():
            continue
        rel = relpath_str(p)
        if rel.startswith("switch_data/"):
            continue
        parsed = file_uuid_and_ext(p)
        if not parsed:
            continue
        uid, _ = parsed
        if uid in target:
            result.append(p)
    return result

def normalize_uuid_to_write(uuid_str: str) -> str:
    # 移除所有空白字符，以防万一
    clean_uuid = uuid_str.strip()
    try:
        # uuid.UUID(clean_uuid) 会自动处理有无横杠的情况
        # str(...) 会返回标准的带横杠格式：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        return str(uuid.UUID(clean_uuid))
    except ValueError:
        # 如果输入的字符串不是合法的 UUID，保持原样或报错
        return clean_uuid
        
def rename_uuid_related_files(old_uuid: str, new_uuid: str) -> Tuple[int, int]:
    old_uuid = normalize_uuid_to_write(old_uuid)
    new_uuid = normalize_uuid_to_write(new_uuid)

    affected = collect_related_files_by_uuid([old_uuid])
    if not affected:
        return 0, 0

    success = 0
    skipped = 0
    for src in affected:
        parsed = file_uuid_and_ext(src)
        if not parsed:
            skipped += 1
            continue
        uid, ext = parsed
        if uid != old_uuid:
            skipped += 1
            continue

        dst = src.with_name(f"{new_uuid}{ext}")
        if dst.exists():
            skipped += 1
            continue
        src.rename(dst)
        success += 1
    return success, skipped


def resolve_target_uuid(rec: PlayerRecord, target_mode: str) -> Optional[str]:
    """
    目标模式：
    - online：优先 online_uuid，其次联网获取
    - offline：优先 offline_uuid，其次按名字计算
    """
    if target_mode == "online":
        if rec.online_uuid and is_uuid_like(rec.online_uuid):
            return normalize_uuid(rec.online_uuid)
        if NAME_RE.match(rec.name):
            fetched = fetch_player_uuid(rec.name)
            if fetched:
                _, uid = fetched
                rec.online_uuid = uid
                return uid
        return None

    if target_mode == "offline":
        if rec.offline_uuid and is_uuid_like(rec.offline_uuid):
            return normalize_uuid(rec.offline_uuid)
        if NAME_RE.match(rec.name):
            uid = offline_uuid_from_name(rec.name)
            rec.offline_uuid = uid
            return uid
        return None

    return None


def switch_one(records: Dict[str, PlayerRecord]) -> None:
    if not records:
        log_warn("没有任何玩家记录，请先扫描。")
        pause()
        return

    items = sorted(records.values(), key=lambda x: x.name.lower())

    print()
    print(col("单个玩家切换", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))

    print_player_table(items)

    sel = ask("选择玩家: ")
    if sel == "0":
        return
    if not sel.isdigit() or not (1 <= int(sel) <= len(items)):
        log_warn("输入无效。")
        pause()
        return

    rec = items[int(sel) - 1]

    print(col(hr(), C.BRIGHT_BLUE))
    print(col("1. 切换为 online", C.BRIGHT_GREEN))
    print(col("2. 切换为 offline", C.BRIGHT_YELLOW))
    print(col("0. 返回", C.BRIGHT_BLACK))
    t = ask("选择目标状态: ")
    if t == "0":
        return
    if t not in {"1", "2"}:
        log_warn("输入无效。")
        pause()
        return

    target_mode = "online" if t == "1" else "offline"
    target_uuid = resolve_target_uuid(rec, target_mode)
    if not target_uuid:
        log_err("无法解析目标 UUID。")
        pause()
        return

    related = collect_related_files_by_uuid([rec.uuid, rec.online_uuid or "", rec.offline_uuid or ""])
    if not related:
        log_warn("没有找到该玩家的相关文件。")
        pause()
        return

    log_info("切换前正在备份……")
    backup_root = backup_files(related + ([PLAYER_JSON] if PLAYER_JSON.exists() else []), f"switch_{rec.name}_{target_mode}")
    if backup_root:
        log_ok(f"备份完成：{backup_root.relative_to(ROOT)}")

    succ, skip = rename_uuid_related_files(rec.uuid, target_uuid)
    if succ == 0:
        log_warn("未找到可重命名文件，可能当前 UUID 没有对应数据。")
    else:
        rec.uuid = target_uuid
        rec.mode = target_mode
        rec.matched = True
        rec.last_scan = time.time()
        save_player_db(records)
        log_ok(f"切换完成：重命名 {succ} 个文件，跳过 {skip} 个文件。")
    pause()


def batch_switch(records: Dict[str, PlayerRecord], target_mode: str) -> None:
    if not records:
        log_warn("没有任何玩家记录，请先扫描。")
        pause()
        return

    items = sorted(records.values(), key=lambda x: x.name.lower())
    print()
    print(col(f"批量切换为 {target_mode.upper()}", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))
    valid_items = []
    for r in items:
        target = resolve_target_uuid(r, target_mode)
        mode_txt = "offline" if r.mode == "offline" else "online" if r.mode == "online" else "unknown"
        print(
            col(" • ", C.BRIGHT_GREEN)
            + col(r.name, C.BRIGHT_CYAN, bold=True)
            + col(f" | 当前={mode_txt} | 目标UUID={target or '无法解析'}", C.WHITE)
        )
        if target:
            valid_items.append((r, target))

    if not valid_items:
        log_warn("没有可执行切换的玩家。")
        pause()
        return

    if not ask_yes_no("确认执行批量切换？", default=False):
        log_info("已取消。")
        pause()
        return

    all_related = []
    for r, _ in valid_items:
        all_related.extend(collect_related_files_by_uuid([r.uuid, r.online_uuid or "", r.offline_uuid or ""]))
    if PLAYER_JSON.exists():
        all_related.append(PLAYER_JSON)

    log_info("正在执行切换前备份……")
    backup_root = backup_files(all_related, f"batch_{target_mode}")
    if backup_root:
        log_ok(f"备份完成：{backup_root.relative_to(ROOT)}")

    total = len(valid_items)
    done = 0
    for i, (r, target) in enumerate(valid_items, 1):
        show_progress(i, total, "切换中")
        if is_uuid_like(r.uuid) and normalize_uuid(r.uuid) != normalize_uuid(target):
            succ, _ = rename_uuid_related_files(r.uuid, target)
            if succ > 0:
                r.uuid = target
                done += 1
        r.mode = target_mode
        r.matched = True
        r.last_scan = time.time()
    print()
    save_player_db(records)
    log_ok(f"批量切换完成：{done}/{total} 个玩家已处理。")
    pause()


def restore_backup() -> None:
    ensure_dirs()
    sessions = [p for p in SWITCH_DIR.iterdir() if p.is_dir()]
    if not sessions:
        log_warn("没有可恢复的备份。")
        pause()
        return

    sessions = sorted(sessions, key=lambda p: p.name, reverse=True)
    print()
    print(col("恢复备份", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))
    for i, s in enumerate(sessions, 1):
        print(col(f"{i:>3}. ", C.BRIGHT_GREEN) + s.name)
    print(col("  0. 返回", C.BRIGHT_YELLOW))

    sel = ask("选择备份集: ")
    if sel == "0":
        return
    if not sel.isdigit() or not (1 <= int(sel) <= len(sessions)):
        log_warn("输入无效。")
        pause()
        return

    src_root = sessions[int(sel) - 1]
    if not ask_yes_no(f"确认从 {src_root.name} 恢复并覆盖当前文件？", default=False):
        log_info("已取消。")
        pause()
        return

    files = [p for p in src_root.rglob("*") if p.is_file()]
    if not files:
        log_warn("该备份集为空。")
        pause()
        return

    total = len(files)
    for i, src in enumerate(files, 1):
        show_progress(i, total, "恢复中")
        rel = src.relative_to(src_root)
        dst = ROOT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    print()
    log_ok("恢复完成。")
    pause()


def refresh_online_uuids(records: Dict[str, PlayerRecord]) -> None:
    targets = [r for r in records.values() if r.matched and NAME_RE.match(r.name)]
    if not targets:
        log_warn("没有可刷新在线 UUID 的玩家。")
        pause()
        return

    print()
    print(col("刷新在线 UUID", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))
    total = len(targets)
    changed = 0
    for i, r in enumerate(targets, 1):
        show_progress(i, total, "刷新中")
        fetched = fetch_player_uuid(r.name)
        if fetched:
            _, uid = fetched
            uid = normalize_uuid(uid)
            if r.online_uuid != uid:
                r.online_uuid = uid
                changed += 1
                r.source = "api"
    print()
    save_player_db(records)
    log_ok(f"刷新完成，更新 {changed} 个在线 UUID。")
    pause()


def show_sessions():
    ensure_dirs()
    sessions = [p for p in SWITCH_DIR.iterdir() if p.is_dir()]
    print()
    print(col("备份列表", C.BRIGHT_WHITE, bold=True))
    print(col(hr(), C.BRIGHT_BLUE))
    if not sessions:
        log_warn("暂无备份。")
        return
    for i, s in enumerate(sorted(sessions, key=lambda p: p.name, reverse=True), 1):
        count = sum(1 for _ in s.rglob("*") if _.is_file())
        print(col(f"{i:>3}. ", C.BRIGHT_GREEN) + f"{s.name}  | 文件数={count}")


def print_player_table(players: List[PlayerRecord]) -> None:
    if not players:
        log_warn("没有玩家记录。")
        return

    items = list(players)

    # 计算列宽
    idx_w = max(2, len(str(len(items))))
    name_w = max(len("玩家"), max(len(r.name or "") for r in items))
    uuid_w = max(len("Online UUID"), max(len(r.online_uuid or "") for r in items))
    state_w = len("状态")

    # 顶部
    print(
        "┌" + "─"*(idx_w+2) +
        "┬" + "─"*(name_w+2) +
        "┬" + "─"*(uuid_w+2) +
        "┬" + "─"*(state_w+2) + "┐"
    )

    # 表头
    print(
        "│ "
        + col("#".ljust(idx_w), C.BRIGHT_WHITE, bold=True)
        + " │ "
        + col("玩家".ljust(name_w), C.BRIGHT_WHITE, bold=True)
        + " │ "
        + col("Online UUID".ljust(uuid_w), C.BRIGHT_WHITE, bold=True)
        + " │ "
        + col("状态".ljust(state_w), C.BRIGHT_WHITE, bold=True)
        + " │"
    )

    # 分隔
    print(
        "├" + "─"*(idx_w+2) +
        "┼" + "─"*(name_w+2) +
        "┼" + "─"*(uuid_w+2) +
        "┼" + "─"*(state_w+2) + "┤"
    )

    # 数据
    for i, r in enumerate(items, 1):

        if r.uuid == r.online_uuid:
            state = col("ONLINE", C.GREEN, bold=True)

        else:
            state = col("OFFLINE", C.RED, bold=True)


        print(
            "│ "
            + col(str(i).ljust(idx_w), C.BRIGHT_GREEN)
            + " │ "
            + col((r.name or "").ljust(name_w), C.BRIGHT_CYAN, bold=True)
            + " │ "
            + (r.online_uuid or "").ljust(uuid_w)
            + " │ "
            + state.ljust(state_w)
            + " │"
        )

    # 底部
    print(
        "└" + "─"*(idx_w+2) +
        "┴" + "─"*(name_w+2) +
        "┴" + "─"*(uuid_w+2) +
        "┴" + "─"*(state_w+2) + "┘"
    )

def merge_player_records(records: Dict[str, PlayerRecord], rec: PlayerRecord) -> None:
    """
    自动合并同一玩家的多条记录
    """

    keys = set()

    if rec.uuid:
        keys.add(normalize_uuid(rec.uuid))

    if rec.online_uuid:
        keys.add(normalize_uuid(rec.online_uuid))

    if rec.offline_uuid:
        keys.add(normalize_uuid(rec.offline_uuid))

    found = []

    for k in keys:
        if k in records and records[k] not in found:
            found.append(records[k])

    if len(found) <= 1:
        return

    master = found[0]

    for r in found[1:]:

        if r.name and (not master.name or master.name == master.uuid):
            master.name = r.name

        if r.online_uuid:
            master.online_uuid = r.online_uuid

        if r.offline_uuid:
            master.offline_uuid = r.offline_uuid

        if r.mode == "online":
            master.mode = "online"

        # 删除旧key
        for k, v in list(records.items()):
            if v is r:
                del records[k]

    records[master.uuid] = master


def main():
    ensure_dirs()
    records = load_player_db()

    while True:
        banner()
        print(col(f"地图根目录：{ROOT}", C.BRIGHT_WHITE))
        print(col(f"索引文件：{PLAYER_JSON}", C.BRIGHT_WHITE))
        print(col(f"备份目录：{SWITCH_DIR}", C.BRIGHT_WHITE))
        print(col(hr(), C.BRIGHT_BLUE))
        print(col("  1. 扫描 UUID 文件", C.BRIGHT_GREEN))
        print(col("  2. 查看玩家列表", C.BRIGHT_GREEN))
        print(col("  3. 手动补全", C.BRIGHT_GREEN))
        print(col("  4. 刷新在线 UUID", C.BRIGHT_GREEN))
        print(col("  5. 单个玩家切换 online / offline", C.BRIGHT_GREEN))
        print(col("  6. 批量切换所有玩家为 online", C.BRIGHT_GREEN))
        print(col("  7. 批量切换所有玩家为 offline", C.BRIGHT_GREEN))
        print(col("  8. 查看备份列表", C.BRIGHT_GREEN))
        print(col("  9. 恢复备份", C.BRIGHT_GREEN))
        print(col("  0. 退出", C.BRIGHT_RED))
        print(col(hr(), C.BRIGHT_BLUE))

        choice = ask("请选择功能: ")

        if choice == "1":
            records, files = scan_and_build_index(records)
            log_ok(f"扫描完成，共发现 {len(files)} 个 UUID 文件。")
            pause()
        elif choice == "2":
            list_players(records)
            pause()
        elif choice == "3":
            records = manual_link_player(records)
            pause()
        elif choice == "4":
            refresh_online_uuids(records)
        elif choice == "5":
            list_players(records)
            switch_one(records)
        elif choice == "6":
            batch_switch(records, "online")
        elif choice == "7":
            batch_switch(records, "offline")
        elif choice == "8":
            show_sessions()
            pause()
        elif choice == "9":
            restore_backup()
        elif choice == "0":
            log_info("已退出。")
            break
        else:
            log_warn("无效输入。")
            time.sleep(0.7)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        log_warn("已中断。")
    except Exception as e:
        print()
        log_err(f"程序出错：{e}")
        raise
