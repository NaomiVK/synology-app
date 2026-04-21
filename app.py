from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
import sqlite3
import shutil
import struct
import tempfile
import threading
import time
import base64
import binascii
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from hashlib import sha256
from datetime import datetime
from pathlib import Path
from collections import OrderedDict, deque
from typing import Optional

from flask import Flask, Response, abort, redirect, render_template, request, send_file, session, url_for, has_request_context
from PIL import Image, ImageDraw, ImageFilter, ImageOps
from werkzeug.security import check_password_hash, generate_password_hash

try:
    import cv2
    import numpy as np
except ImportError:
    cv2 = None
    np = None

import ai_analysis
import metadata_index
from png_metadata_parser import PNGMetadataParser


APP_TITLE = "Minx PNG Metadata Viewer"
APP_TITLE_MOBILE = "Minx Metadata Viewer"
BROWSE_ROOT = Path(os.getenv("BROWSE_ROOT", "/data/output")).resolve()
SHOW_HIDDEN = os.getenv("SHOW_HIDDEN", "0") == "1"
THUMB_CACHE_DIR = Path(os.getenv("THUMB_CACHE_DIR", "/tmp/thumbs")).resolve()
THUMB_CACHE_MAX_AGE = int(os.getenv("THUMB_CACHE_MAX_AGE", str(60 * 60 * 24 * 30)))
THUMB_SIZE_SQUARE = int(os.getenv("THUMB_SIZE_SQUARE", "320"))
THUMB_SIZE_FULL = int(os.getenv("THUMB_SIZE_FULL", "640"))
THUMB_SIZE_PREVIEW = int(os.getenv("THUMB_SIZE_PREVIEW", "1600"))
APP_LOG_PATH = Path(os.getenv("APP_LOG_PATH", "/tmp/viewer.log")).resolve()
THUMB_READY_STATE_PATH = Path(os.getenv("THUMB_READY_STATE_PATH", "/tmp/thumb-ready-state.json")).resolve()
REBUILD_STATUS_PATH = Path(os.getenv("REBUILD_STATUS_PATH", "/tmp/rebuild-status.json")).resolve()
METADATA_DB_PATH = Path(os.getenv("METADATA_DB_PATH", "/tmp/metadata-index.sqlite")).resolve()
METADATA_INDEX_STATUS_PATH = Path(os.getenv("METADATA_INDEX_STATUS_PATH", "/tmp/metadata-index-status.json")).resolve()
AI_ANALYSIS_STATUS_PATH = Path(os.getenv("AI_ANALYSIS_STATUS_PATH", "/tmp/ai-analysis-status.json")).resolve()
FAVORITES_STATE_PATH = Path(os.getenv("FAVORITES_STATE_PATH", "/cache/favorites.json")).resolve()
FOLDER_COVERS_STATE_PATH = Path(os.getenv("FOLDER_COVERS_STATE_PATH", "/cache/folder-covers.json")).resolve()
EDITS_DIR = Path(os.getenv("EDITS_DIR", "/cache/edits")).resolve()
THUMB_DIMENSIONS_DB_PATH = Path(os.getenv("THUMB_DIMENSIONS_DB_PATH", "/cache/thumb-dimensions.sqlite")).resolve()
TAG_ALIASES_CSV_PATH = Path(os.getenv("TAG_ALIASES_CSV_PATH", "/cache/tag_aliases.csv"))
GENERIC_TAGS_CSV_PATH = Path(os.getenv("GENERIC_TAGS_CSV_PATH", "/cache/generic_tags.csv"))
METADATA_CACHE_MAX_ITEMS = int(os.getenv("METADATA_CACHE_MAX_ITEMS", "128"))
REBUILD_PREVIEW_WORKERS = 4
FAVORITES_DIR_KEY = "__favorites__"
EDITS_DIR_KEY = "__edits__"
AI_ANALYSIS_ENABLED = os.getenv("AI_ANALYSIS_ENABLED", "0") == "1"
AI_ANALYSIS_MODEL = os.getenv("AI_ANALYSIS_MODEL", ai_analysis.DEFAULT_MODEL)
AI_ANALYSIS_DETAIL = os.getenv("AI_ANALYSIS_DETAIL", ai_analysis.DEFAULT_DETAIL)
AI_ANALYSIS_PROMPT_VERSION = os.getenv("AI_ANALYSIS_PROMPT_VERSION", ai_analysis.DEFAULT_PROMPT_VERSION)
AI_ANALYSIS_MAX_WORKERS = max(1, int(os.getenv("AI_ANALYSIS_MAX_WORKERS", str(REBUILD_PREVIEW_WORKERS))))
SECRET_KEY = os.getenv("SECRET_KEY", "")
APP_PASSWORD_HASH = os.getenv("APP_PASSWORD_HASH", "").strip()
AUTH_ENV_FILE_PATH = Path(os.getenv("AUTH_ENV_FILE_PATH", "/cache/.env")).resolve()
REMEMBER_ME_DAYS = max(1, int(os.getenv("REMEMBER_ME_DAYS", "30")))
LOGIN_RATE_LIMIT_WINDOW_SECONDS = max(60, int(os.getenv("LOGIN_RATE_LIMIT_WINDOW_SECONDS", "900")))
LOGIN_RATE_LIMIT_MAX_ATTEMPTS = max(1, int(os.getenv("LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "10")))
AUTH_SESSION_KEY = "auth_ok"
AUTH_FINGERPRINT_KEY = "auth_fp"
LOCAL_REPAIR_RADIUS = max(1, min(16, int(os.getenv("LOCAL_REPAIR_RADIUS", "2"))))
LOCAL_REPAIR_METHOD = str(os.getenv("LOCAL_REPAIR_METHOD", "telea")).strip().lower()

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.permanent_session_lifetime = timedelta(days=REMEMBER_ME_DAYS)
parser = PNGMetadataParser()
app_logger = logging.getLogger("png_viewer")
metadata_cache_lock = threading.Lock()
metadata_cache = OrderedDict()
thumb_ready_state_lock = threading.Lock()
directory_thumbnail_progress_lock = threading.Lock()
directory_thumbnail_progress = {}
folder_cover_cache_lock = threading.Lock()
folder_cover_cache = {}
rebuild_previews_lock = threading.Lock()
rebuild_status_lock = threading.Lock()
metadata_index_lock = threading.Lock()
metadata_index_status_lock = threading.Lock()
ai_analysis_lock = threading.Lock()
ai_analysis_status_lock = threading.Lock()
favorites_state_lock = threading.Lock()
folder_covers_state_lock = threading.Lock()
thumb_dimensions_db_lock = threading.Lock()
tag_aliases_lock = threading.Lock()
tag_aliases_cache = {
    "path": None,
    "mtime_ns": None,
    "config": {
        "canonical_by_normalized": {},
        "search_terms_by_normalized": {},
        "source_path": None,
    },
}
generic_tags_lock = threading.Lock()
generic_tags_cache = {
    "path": None,
    "mtime_ns": None,
    "config": {
        "hidden_generic_tags": set(),
        "source_path": None,
    },
}
login_attempts_lock = threading.Lock()
login_attempts = {}
auth_config_lock = threading.Lock()
auth_config_cache = {
    "path": None,
    "mtime_ns": None,
    "password_hash": APP_PASSWORD_HASH,
}
rebuild_status = {
    "state": "idle",
    "phase": "idle",
    "cancel_requested": False,
    "progress_pct": 0,
    "completed_tasks": 0,
    "total_tasks": 0,
    "folders_scanned": 0,
    "folders_with_pngs": 0,
    "image_count": 0,
    "current_directory": None,
    "current_file": None,
    "scope_dir": "",
    "scope_label": "root",
    "force_rebuild": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "summary": None,
}


def current_auth_fingerprint():
    if not get_app_password_hash():
        return ""
    return sha256(get_app_password_hash().encode("utf-8")).hexdigest()


def decode_env_value_from_compose(value: str) -> str:
    return str(value or "").replace("$$", "$")


def load_password_hash_from_env_file():
    if not AUTH_ENV_FILE_PATH.exists() or not AUTH_ENV_FILE_PATH.is_file():
        return None, None
    try:
        stat = AUTH_ENV_FILE_PATH.stat()
        mtime_ns = int(stat.st_mtime_ns)
    except OSError:
        return None, None
    try:
        lines = AUTH_ENV_FILE_PATH.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None, None

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not stripped.startswith("APP_PASSWORD_HASH="):
            continue
        _, _, raw_value = stripped.partition("=")
        return decode_env_value_from_compose(raw_value.strip()), mtime_ns
    return None, mtime_ns


def get_app_password_hash():
    with auth_config_lock:
        cache_path = auth_config_cache.get("path")
        cache_mtime = auth_config_cache.get("mtime_ns")
        password_hash, mtime_ns = load_password_hash_from_env_file()
        if mtime_ns is not None:
            if cache_path != str(AUTH_ENV_FILE_PATH) or cache_mtime != mtime_ns:
                auth_config_cache.update({
                    "path": str(AUTH_ENV_FILE_PATH),
                    "mtime_ns": mtime_ns,
                    "password_hash": password_hash or APP_PASSWORD_HASH,
                })
            return auth_config_cache.get("password_hash") or APP_PASSWORD_HASH
        return auth_config_cache.get("password_hash") or APP_PASSWORD_HASH


def set_app_password_hash(password_hash: str):
    global APP_PASSWORD_HASH
    with auth_config_lock:
        APP_PASSWORD_HASH = (password_hash or "").strip()
        mtime_ns = None
        try:
            if AUTH_ENV_FILE_PATH.exists() and AUTH_ENV_FILE_PATH.is_file():
                mtime_ns = int(AUTH_ENV_FILE_PATH.stat().st_mtime_ns)
        except OSError:
            mtime_ns = None
        auth_config_cache.update({
            "path": str(AUTH_ENV_FILE_PATH),
            "mtime_ns": mtime_ns,
            "password_hash": APP_PASSWORD_HASH,
        })


def escape_env_value_for_compose(value: str) -> str:
    return str(value or "").replace("$", "$$")


def persist_password_hash_to_env_file(password_hash: str):
    escaped_hash = escape_env_value_for_compose(password_hash)
    AUTH_ENV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    if AUTH_ENV_FILE_PATH.exists() and AUTH_ENV_FILE_PATH.is_file():
        lines = AUTH_ENV_FILE_PATH.read_text(encoding="utf-8").splitlines()

    updated_lines = []
    replaced = False
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("APP_PASSWORD_HASH="):
            indent = line[: len(line) - len(stripped)]
            updated_lines.append(f"{indent}APP_PASSWORD_HASH={escaped_hash}")
            replaced = True
            continue
        updated_lines.append(line)

    if not replaced:
        if updated_lines and updated_lines[-1] != "":
            updated_lines.append("")
        updated_lines.append(f"APP_PASSWORD_HASH={escaped_hash}")

    AUTH_ENV_FILE_PATH.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")


def is_login_configured():
    return bool(SECRET_KEY and get_app_password_hash())


def is_authenticated_session():
    if not is_login_configured():
        return False
    if not session.get(AUTH_SESSION_KEY):
        return False
    return session.get(AUTH_FINGERPRINT_KEY) == current_auth_fingerprint()


def clear_auth_session():
    session.pop(AUTH_SESSION_KEY, None)
    session.pop(AUTH_FINGERPRINT_KEY, None)
    session.pop("_permanent", None)


def get_request_client_key():
    forwarded_for = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return forwarded_for or request.remote_addr or "unknown"


def prune_login_attempts(now_ts):
    cutoff = now_ts - LOGIN_RATE_LIMIT_WINDOW_SECONDS
    stale_keys = []
    for key, attempts in login_attempts.items():
        filtered = [ts for ts in attempts if ts >= cutoff]
        if filtered:
            login_attempts[key] = filtered
        else:
            stale_keys.append(key)
    for key in stale_keys:
        login_attempts.pop(key, None)


def is_login_rate_limited(client_key):
    now_ts = datetime.utcnow().timestamp()
    with login_attempts_lock:
        prune_login_attempts(now_ts)
        attempts = login_attempts.get(client_key, [])
        return len(attempts) >= LOGIN_RATE_LIMIT_MAX_ATTEMPTS


def record_failed_login_attempt(client_key):
    now_ts = datetime.utcnow().timestamp()
    with login_attempts_lock:
        prune_login_attempts(now_ts)
        attempts = login_attempts.setdefault(client_key, [])
        attempts.append(now_ts)


def clear_failed_login_attempts(client_key):
    with login_attempts_lock:
        login_attempts.pop(client_key, None)


def normalize_alias_tag_text(value):
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().lower().split())
    return normalized or None


def get_tag_aliases_source_path():
    try:
        resolved = TAG_ALIASES_CSV_PATH.resolve()
    except Exception:
        resolved = TAG_ALIASES_CSV_PATH
    if resolved.exists() and resolved.is_file():
        return resolved
    return None


def get_generic_tags_source_path():
    try:
        resolved = GENERIC_TAGS_CSV_PATH.resolve()
    except Exception:
        resolved = GENERIC_TAGS_CSV_PATH
    if resolved.exists() and resolved.is_file():
        return resolved
    return None


def load_tag_aliases_config():
    source_path = get_tag_aliases_source_path()
    if source_path is None:
        return {
            "canonical_by_normalized": {},
            "search_terms_by_normalized": {},
            "source_path": None,
        }

    try:
        stat = source_path.stat()
        mtime_ns = int(stat.st_mtime_ns)
    except OSError:
        return {
            "canonical_by_normalized": {},
            "search_terms_by_normalized": {},
            "source_path": None,
        }

    with tag_aliases_lock:
        cached_path = tag_aliases_cache.get("path")
        cached_mtime = tag_aliases_cache.get("mtime_ns")
        if cached_path == str(source_path) and cached_mtime == mtime_ns:
            return dict(tag_aliases_cache["config"])

        canonical_by_normalized = {}
        family_terms = {}
        try:
            with source_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    alias_tag = (row.get("alias_tag") or "").strip()
                    canonical_tag = (row.get("canonical_tag") or "").strip()
                    status = (row.get("status") or "active").strip().lower()
                    if not alias_tag or not canonical_tag or status not in {"", "active", "enabled", "1", "true", "yes"}:
                        continue
                    alias_key = normalize_alias_tag_text(alias_tag)
                    canonical_key = normalize_alias_tag_text(canonical_tag)
                    if not alias_key or not canonical_key:
                        continue
                    canonical_by_normalized[alias_key] = canonical_tag
                    canonical_by_normalized[canonical_key] = canonical_tag
                    family_terms.setdefault(canonical_key, set()).update([alias_tag, canonical_tag])
        except Exception as exc:
            log_event("tag_aliases_load_failure", "Failed to load tag aliases", file=str(source_path), error=str(exc))
            config = {
                "canonical_by_normalized": {},
                "search_terms_by_normalized": {},
                "source_path": str(source_path),
            }
            tag_aliases_cache.update({"path": str(source_path), "mtime_ns": mtime_ns, "config": config})
            return dict(config)

        search_terms_by_normalized = {}
        for canonical_key, terms in family_terms.items():
            canonical_label = canonical_by_normalized.get(canonical_key) or next(iter(sorted(terms)))
            normalized_terms = []
            seen = set()
            for term in sorted(terms):
                norm = normalize_alias_tag_text(term)
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                normalized_terms.append(term)
            if canonical_key not in canonical_by_normalized:
                canonical_by_normalized[canonical_key] = canonical_label
            search_terms_by_normalized[canonical_key] = normalized_terms
            for term in normalized_terms:
                norm = normalize_alias_tag_text(term)
                if not norm:
                    continue
                canonical_by_normalized[norm] = canonical_label
                search_terms_by_normalized[norm] = list(normalized_terms)

        config = {
            "canonical_by_normalized": canonical_by_normalized,
            "search_terms_by_normalized": search_terms_by_normalized,
            "source_path": str(source_path),
        }
        tag_aliases_cache.update({"path": str(source_path), "mtime_ns": mtime_ns, "config": config})
        return dict(config)


def load_generic_tags_config():
    source_path = get_generic_tags_source_path()
    if source_path is None:
        return {
            "hidden_generic_tags": set(),
            "source_path": None,
        }

    try:
        stat = source_path.stat()
        mtime_ns = int(stat.st_mtime_ns)
    except OSError:
        return {
            "hidden_generic_tags": set(),
            "source_path": None,
        }

    with generic_tags_lock:
        cached_path = generic_tags_cache.get("path")
        cached_mtime = generic_tags_cache.get("mtime_ns")
        if cached_path == str(source_path) and cached_mtime == mtime_ns:
            return {
                "hidden_generic_tags": set(generic_tags_cache["config"]["hidden_generic_tags"]),
                "source_path": generic_tags_cache["config"]["source_path"],
            }

        hidden_generic_tags = set()
        try:
            with source_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    tag = normalize_alias_tag_text((row.get("tag") or ""))
                    if not tag:
                        continue
                    hide_value = str(row.get("hide_in_explorer") or row.get("is_generic") or "").strip().lower()
                    if hide_value not in {"1", "true", "yes", "on", "active"}:
                        continue
                    hidden_generic_tags.add(tag)
        except Exception as exc:
            log_event("generic_tags_load_failure", "Failed to load generic tags", file=str(source_path), error=str(exc))
            config = {
                "hidden_generic_tags": set(),
                "source_path": str(source_path),
            }
            generic_tags_cache.update({"path": str(source_path), "mtime_ns": mtime_ns, "config": config})
            return {
                "hidden_generic_tags": set(),
                "source_path": str(source_path),
            }

        config = {
            "hidden_generic_tags": hidden_generic_tags,
            "source_path": str(source_path),
        }
        generic_tags_cache.update({"path": str(source_path), "mtime_ns": mtime_ns, "config": config})
        return {
            "hidden_generic_tags": set(hidden_generic_tags),
            "source_path": str(source_path),
        }
metadata_index_status = {
    "state": "idle",
    "phase": "idle",
    "cancel_requested": False,
    "progress_pct": 0,
    "completed_tasks": 0,
    "total_tasks": 0,
    "folders_scanned": 0,
    "folders_with_pngs": 0,
    "image_count": 0,
    "current_directory": None,
    "current_file": None,
    "scope_dir": "",
    "scope_label": "root",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "summary": None,
}
ai_analysis_status = {
    "state": "idle",
    "phase": "idle",
    "cancel_requested": False,
    "progress_pct": 0,
    "completed_tasks": 0,
    "total_tasks": 0,
    "folders_scanned": 0,
    "folders_with_pngs": 0,
    "image_count": 0,
    "current_directory": None,
    "current_file": None,
    "scope_dir": "",
    "scope_label": "root",
    "force_rebuild": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "summary": None,
}


def ensure_favorites_state_dir():
    FAVORITES_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def normalize_favorites_state(payload):
    if not isinstance(payload, dict):
        payload = {}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    normalized_entries = {}
    for rel_path, item in entries.items():
        if not isinstance(rel_path, str) or not isinstance(item, dict):
            continue
        normalized_entries[rel_path] = {
            "mtime": int(item.get("mtime") or 0),
            "size": int(item.get("size") or 0),
            "added_at": str(item.get("added_at") or ""),
        }
    return {"entries": normalized_entries}


def load_favorites_state():
    if not FAVORITES_STATE_PATH.exists() or not FAVORITES_STATE_PATH.is_file():
        return {"entries": {}}
    try:
        payload = json.loads(FAVORITES_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"entries": {}}
    return normalize_favorites_state(payload)


def save_favorites_state(state):
    ensure_favorites_state_dir()
    normalized = normalize_favorites_state(state)
    atomic_write_json(FAVORITES_STATE_PATH, normalized)


def normalize_folder_covers_state(payload):
    if not isinstance(payload, dict):
        payload = {}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    normalized_entries = {}
    for folder_rel, item in entries.items():
        if not isinstance(folder_rel, str) or not isinstance(item, dict):
            continue
        folder_key = folder_rel.strip().replace("\\", "/").strip("/")
        rel_path = str(item.get("rel_path") or "").strip().replace("\\", "/")
        if not folder_key or not rel_path:
            continue
        normalized_entries[folder_key] = {
            "rel_path": rel_path,
            "set_at": str(item.get("set_at") or ""),
        }
    return {"entries": normalized_entries}


def load_folder_covers_state():
    if not FOLDER_COVERS_STATE_PATH.exists() or not FOLDER_COVERS_STATE_PATH.is_file():
        return {"entries": {}}
    try:
        payload = json.loads(FOLDER_COVERS_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"entries": {}}
    return normalize_folder_covers_state(payload)


def save_folder_covers_state(state):
    normalized = normalize_folder_covers_state(state)
    atomic_write_json(FOLDER_COVERS_STATE_PATH, normalized)


def ensure_edits_dir():
    EDITS_DIR.mkdir(parents=True, exist_ok=True)


def build_source_signature(st) -> str:
    return f"{int(getattr(st, 'st_mtime_ns', int(st.st_mtime * 1_000_000_000)))}:{int(st.st_size)}"


def build_edit_cache_key(rel_path: str) -> str:
    return sha256(rel_path.encode("utf-8")).hexdigest()


def get_edit_paths(rel_path: str) -> dict:
    digest = build_edit_cache_key(rel_path)
    edit_dir = EDITS_DIR / digest[:2] / digest
    return {
        "dir": edit_dir,
        "meta": edit_dir / "meta.json",
        "current": edit_dir / "current.png",
        "editor_base": edit_dir / "editor_base.png",
    }


def normalize_image_edit_meta(payload):
    if not isinstance(payload, dict):
        payload = {}
    history = payload.get("history")
    if not isinstance(history, list):
        history = []
    normalized_history = []
    for item in history:
        if not isinstance(item, dict):
            continue
        normalized_history.append({
            "type": str(item.get("type") or "").strip(),
            "ts": str(item.get("ts") or "").strip(),
            "prompt": str(item.get("prompt") or "").strip(),
            "preset_name": str(item.get("preset_name") or "").strip(),
            "selection": item.get("selection") if isinstance(item.get("selection"), dict) else None,
            "patch_rect": item.get("patch_rect") if isinstance(item.get("patch_rect"), dict) else None,
            "look_modules": item.get("look_modules") if isinstance(item.get("look_modules"), dict) else None,
            "look_steps": item.get("look_steps") if isinstance(item.get("look_steps"), list) else None,
            "model": str(item.get("model") or "").strip(),
            "temperature": float(item.get("temperature") or 0),
            "tint": float(item.get("tint") or 0),
            "saturation": float(item.get("saturation") or 0),
            "vibrance": float(item.get("vibrance") or 0),
            "brightness": float(item.get("brightness") or 0),
            "contrast": float(item.get("contrast") or 0),
            "matte": float(item.get("matte") or 0),
            "curve": float(item.get("curve") or 0),
            "whites": float(item.get("whites") or 0),
            "vignette_center_x": float(item.get("vignette_center_x") or 0),
            "vignette_center_y": float(item.get("vignette_center_y") or 0),
            "vignette_size": float(item.get("vignette_size") or 0),
            "vignette_feather": float(item.get("vignette_feather") or 0),
            "vignette_inner_brightness": float(item.get("vignette_inner_brightness") or 0),
            "vignette_outer_brightness": float(item.get("vignette_outer_brightness") or 0),
            "vignette_highlight_protect": float(item.get("vignette_highlight_protect") or 0),
        })
    return {
        "rel_path": str(payload.get("rel_path") or "").strip(),
        "source_signature": str(payload.get("source_signature") or "").strip(),
        "source_mtime_ns": int(payload.get("source_mtime_ns") or 0),
        "source_size_bytes": int(payload.get("source_size_bytes") or 0),
        "created_at": str(payload.get("created_at") or "").strip(),
        "updated_at": str(payload.get("updated_at") or "").strip(),
        "current_image": str(payload.get("current_image") or "current.png").strip() or "current.png",
        "editor_base_image": str(payload.get("editor_base_image") or "").strip(),
        "history": normalized_history,
    }


def load_image_edit_meta(rel_path: str) -> Optional[dict]:
    paths = get_edit_paths(rel_path)
    meta_path = paths["meta"]
    if not meta_path.exists() or not meta_path.is_file():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    normalized = normalize_image_edit_meta(payload)
    if normalized["rel_path"] != rel_path:
        return None
    return normalized


def load_image_edit_meta_summary(rel_path: str) -> Optional[dict]:
    paths = get_edit_paths(rel_path)
    meta_path = paths["meta"]
    if not meta_path.exists() or not meta_path.is_file():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("rel_path") or "").strip() != rel_path:
        return None
    return {
        "rel_path": rel_path,
        "source_signature": str(payload.get("source_signature") or "").strip(),
        "updated_at": str(payload.get("updated_at") or "").strip(),
        "current_image": str(payload.get("current_image") or "current.png").strip() or "current.png",
        "editor_base_image": str(payload.get("editor_base_image") or "").strip(),
    }


def save_image_edit_meta(rel_path: str, payload: dict):
    paths = get_edit_paths(rel_path)
    paths["dir"].mkdir(parents=True, exist_ok=True)
    normalized = normalize_image_edit_meta(payload)
    atomic_write_json(paths["meta"], normalized)


def clear_image_edit(rel_path: str):
    paths = get_edit_paths(rel_path)
    shutil.rmtree(paths["dir"], ignore_errors=True)


def get_current_image_edit(rel_path: str, st) -> Optional[dict]:
    meta = load_image_edit_meta(rel_path)
    if not meta:
        return None
    if meta.get("source_signature") != build_source_signature(st):
        return None
    current_path = get_edit_paths(rel_path)["current"]
    if not current_path.exists() or not current_path.is_file():
        return None
    return {
        "meta": meta,
        "current_path": current_path,
    }


def get_current_image_edit_summary(rel_path: str, st) -> Optional[dict]:
    meta = load_image_edit_meta_summary(rel_path)
    if not meta:
        return None
    if meta.get("source_signature") != build_source_signature(st):
        return None
    current_path = get_edit_paths(rel_path)["dir"] / str(meta.get("current_image") or "current.png")
    if not current_path.exists() or not current_path.is_file():
        return None
    return {
        "meta": meta,
        "current_path": current_path,
    }


def extract_look_modules_from_history(history) -> dict:
    modules = {}
    for entry in history if isinstance(history, list) else []:
        if not isinstance(entry, dict):
            continue
        if entry.get("type") in {"look_preset", "editor_session"} and isinstance(entry.get("look_modules"), dict):
            preset_modules = sanitize_look_modules(entry.get("look_modules"))
            if preset_modules.get("color"):
                modules["color"] = preset_modules["color"]
            if preset_modules.get("brightness_contrast"):
                modules["brightness_contrast"] = preset_modules["brightness_contrast"]
            if preset_modules.get("matte"):
                modules["matte"] = preset_modules["matte"]
            if preset_modules.get("vignette"):
                modules["vignette"] = preset_modules["vignette"]
            continue
        if entry.get("type") == "color_balance":
            color_modules = sanitize_look_modules({
                "color": {
                    "temperature": entry.get("temperature"),
                    "tint": entry.get("tint"),
                    "saturation": entry.get("saturation"),
                    "vibrance": entry.get("vibrance"),
                }
            })
            if color_modules.get("color"):
                modules["color"] = color_modules["color"]
            continue
        if entry.get("type") == "brightness_contrast":
            brightness_contrast_modules = sanitize_look_modules({
                "brightness_contrast": {
                    "brightness": entry.get("brightness"),
                    "contrast": entry.get("contrast"),
                }
            })
            if brightness_contrast_modules.get("brightness_contrast"):
                modules["brightness_contrast"] = brightness_contrast_modules["brightness_contrast"]
            continue
        if entry.get("type") == "matte_black":
            matte_modules = sanitize_look_modules({
                "matte": {
                    "matte": entry.get("matte"),
                    "curve": entry.get("curve"),
                    "whites": entry.get("whites"),
                }
            })
            if matte_modules.get("matte"):
                modules["matte"] = matte_modules["matte"]
            continue
        if entry.get("type") == "vignette":
            vignette_modules = sanitize_look_modules({
                "vignette": {
                    "center_x": entry.get("vignette_center_x"),
                    "center_y": entry.get("vignette_center_y"),
                    "size": entry.get("vignette_size"),
                    "feather": entry.get("vignette_feather"),
                    "inner_brightness": entry.get("vignette_inner_brightness"),
                    "outer_brightness": entry.get("vignette_outer_brightness"),
                    "highlight_protect": entry.get("vignette_highlight_protect"),
                }
            })
            if vignette_modules.get("vignette"):
                modules["vignette"] = vignette_modules["vignette"]
    return sanitize_look_modules(modules)


def extract_look_steps_from_history(history) -> list[dict]:
    steps = []
    for entry in history if isinstance(history, list) else []:
        if not isinstance(entry, dict):
            continue
        if entry.get("type") in {"look_preset", "editor_session"} and isinstance(entry.get("look_steps"), list):
            nested_steps = sanitize_look_steps(entry.get("look_steps"))
            steps = nested_steps
            continue
        if entry.get("type") in {"look_preset", "editor_session"} and isinstance(entry.get("look_modules"), dict):
            nested_modules = sanitize_look_modules(entry.get("look_modules"))
            nested_steps = []
            if nested_modules.get("color"):
                nested_steps.append({"type": "color", "values": nested_modules["color"]})
            if nested_modules.get("brightness_contrast"):
                nested_steps.append({"type": "brightness_contrast", "values": nested_modules["brightness_contrast"]})
            if nested_modules.get("matte"):
                nested_steps.append({"type": "matte", "values": nested_modules["matte"]})
            if nested_modules.get("vignette"):
                nested_steps.append({"type": "vignette", "values": nested_modules["vignette"]})
            steps = nested_steps
            continue
        if entry.get("type") == "color_balance":
            color_modules = sanitize_look_modules({
                "color": {
                    "temperature": entry.get("temperature"),
                    "tint": entry.get("tint"),
                    "saturation": entry.get("saturation"),
                    "vibrance": entry.get("vibrance"),
                }
            })
            if color_modules.get("color"):
                steps = [step for step in steps if step.get("type") != "color"]
                steps.append({"type": "color", "values": color_modules["color"]})
            continue
        if entry.get("type") == "brightness_contrast":
            brightness_contrast_modules = sanitize_look_modules({
                "brightness_contrast": {
                    "brightness": entry.get("brightness"),
                    "contrast": entry.get("contrast"),
                }
            })
            if brightness_contrast_modules.get("brightness_contrast"):
                steps = [step for step in steps if step.get("type") != "brightness_contrast"]
                steps.append({"type": "brightness_contrast", "values": brightness_contrast_modules["brightness_contrast"]})
            continue
        if entry.get("type") == "matte_black":
            matte_modules = sanitize_look_modules({
                "matte": {
                    "matte": entry.get("matte"),
                    "curve": entry.get("curve"),
                    "whites": entry.get("whites"),
                }
            })
            if matte_modules.get("matte"):
                steps = [step for step in steps if step.get("type") != "matte"]
                steps.append({"type": "matte", "values": matte_modules["matte"]})
            continue
        if entry.get("type") == "vignette":
            vignette_modules = sanitize_look_modules({
                "vignette": {
                    "center_x": entry.get("vignette_center_x"),
                    "center_y": entry.get("vignette_center_y"),
                    "size": entry.get("vignette_size"),
                    "feather": entry.get("vignette_feather"),
                    "inner_brightness": entry.get("vignette_inner_brightness"),
                    "outer_brightness": entry.get("vignette_outer_brightness"),
                    "highlight_protect": entry.get("vignette_highlight_protect"),
                }
            })
            if vignette_modules.get("vignette"):
                steps = [step for step in steps if step.get("type") != "vignette"]
                steps.append({"type": "vignette", "values": vignette_modules["vignette"]})
    return sanitize_look_steps(steps)


def get_effective_image_path(rel_path: str, source_path: Path, st=None) -> Path:
    if st is None:
        st = source_path.stat()
    current_edit = get_current_image_edit(rel_path, st)
    return current_edit["current_path"] if current_edit else source_path


def get_editor_base_image_path(rel_path: str, source_path: Path, st=None) -> Path:
    if st is None:
        st = source_path.stat()
    current_edit = get_current_image_edit(rel_path, st)
    if not current_edit:
        return source_path
    meta = current_edit.get("meta") or {}
    editor_base_name = str(meta.get("editor_base_image") or "").strip()
    if not editor_base_name:
        return current_edit["current_path"]
    editor_base_path = get_edit_paths(rel_path)["dir"] / editor_base_name
    if editor_base_path.exists() and editor_base_path.is_file():
        return editor_base_path
    return current_edit["current_path"]


def build_thumb_signature_from_current_edit(rel_path: str, st, current_edit: Optional[dict] = None) -> str:
    edit_version = ""
    if current_edit:
        meta = current_edit.get("meta") or {}
        edit_version = f":edit:{meta.get('updated_at') or '1'}"
    return f"{rel_path}:{int(st.st_mtime)}:{st.st_size}{edit_version}"


def build_thumb_signature(rel_path: str, st) -> str:
    current_edit = get_current_image_edit(rel_path, st)
    return build_thumb_signature_from_current_edit(rel_path, st, current_edit=current_edit)


def build_thumb_cache_path_from_signature(thumb_sig: str, mode: str) -> Path:
    key = f"{thumb_sig}:{mode}"
    digest = sha256(key.encode("utf-8")).hexdigest()
    return THUMB_CACHE_DIR / digest[:2] / f"{digest}.webp"


def build_gallery_image_edit_payload(current_edit: Optional[dict]) -> Optional[dict]:
    if not current_edit:
        return None
    meta = current_edit.get("meta") or {}
    return {
        "has_edit": True,
        "updated_at": meta.get("updated_at"),
    }


def build_image_edit_payload(rel_path: str, st) -> Optional[dict]:
    current_edit = get_current_image_edit(rel_path, st)
    if not current_edit:
        return None
    meta = current_edit["meta"]
    history = list(meta.get("history") or [])
    return {
        "has_edit": True,
        "updated_at": meta.get("updated_at"),
        "created_at": meta.get("created_at"),
        "history_count": len(history),
        "history": history[-5:],
        "look_modules": extract_look_modules_from_history(history),
        "look_steps": extract_look_steps_from_history(history),
        "has_editor_base": bool(str(meta.get("editor_base_image") or "").strip()),
    }


def save_image_edit_variant(
    rel_path: str,
    st,
    image: Image.Image,
    history_entry: Optional[dict] = None,
    *,
    history_entries: Optional[list[dict]] = None,
    extra_meta: Optional[dict] = None,
) -> dict:
    paths = get_edit_paths(rel_path)
    paths["dir"].mkdir(parents=True, exist_ok=True)
    image.save(paths["current"], format="PNG")

    timestamp = utc_now_iso()
    meta = load_image_edit_meta(rel_path) or {
        "rel_path": rel_path,
        "source_signature": build_source_signature(st),
        "source_mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
        "source_size_bytes": int(st.st_size),
        "created_at": timestamp,
        "updated_at": timestamp,
        "current_image": "current.png",
        "history": [],
    }
    meta["source_signature"] = build_source_signature(st)
    meta["source_mtime_ns"] = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
    meta["source_size_bytes"] = int(st.st_size)
    meta["updated_at"] = timestamp
    meta["current_image"] = "current.png"
    history = list(meta.get("history") or [])
    if isinstance(history_entry, dict):
        entry = dict(history_entry)
        entry["ts"] = timestamp
        history.append(entry)
    for item in history_entries if isinstance(history_entries, list) else []:
        if not isinstance(item, dict):
            continue
        entry = dict(item)
        entry["ts"] = timestamp
        history.append(entry)
    meta["history"] = history[-20:]
    if isinstance(extra_meta, dict):
        for key, value in extra_meta.items():
            meta[key] = value
    save_image_edit_meta(rel_path, meta)
    return {
        "meta": meta,
        "edited_path": paths["current"],
    }


def _clamp_adjustment(value, min_value=-100.0, max_value=100.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return max(min_value, min(max_value, numeric))


def connect_look_presets_db() -> sqlite3.Connection:
    METADATA_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(METADATA_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS look_presets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            modules_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_used_at TEXT,
            use_count INTEGER NOT NULL DEFAULT 0
        )
    """)
    return conn


def sanitize_look_modules(payload) -> dict:
    modules = payload if isinstance(payload, dict) else {}
    color_payload = modules.get("color") if isinstance(modules.get("color"), dict) else None
    brightness_contrast_payload = modules.get("brightness_contrast") if isinstance(modules.get("brightness_contrast"), dict) else None
    matte_payload = modules.get("matte") if isinstance(modules.get("matte"), dict) else None
    vignette_payload = modules.get("vignette") if isinstance(modules.get("vignette"), dict) else None
    sanitized = {}

    if color_payload:
        color = {
            "temperature": _clamp_adjustment(color_payload.get("temperature")),
            "tint": _clamp_adjustment(color_payload.get("tint")),
            "saturation": _clamp_adjustment(color_payload.get("saturation")),
            "vibrance": _clamp_adjustment(color_payload.get("vibrance")),
        }
        if any(abs(float(v)) > 1e-6 for v in color.values()):
            sanitized["color"] = color

    if brightness_contrast_payload:
        brightness_contrast = {
            "brightness": _clamp_adjustment(brightness_contrast_payload.get("brightness"), -35.0, 35.0),
            "contrast": _clamp_adjustment(brightness_contrast_payload.get("contrast"), -35.0, 35.0),
        }
        if any(abs(float(v)) > 1e-6 for v in brightness_contrast.values()):
            sanitized["brightness_contrast"] = brightness_contrast

    if matte_payload:
        matte = {
            "matte": _clamp_adjustment(matte_payload.get("matte"), 0.0, 100.0),
            "curve": _clamp_adjustment(matte_payload.get("curve"), 0.0, 100.0),
            "whites": _clamp_adjustment(matte_payload.get("whites"), -100.0, 100.0),
        }
        if any(abs(float(v)) > 1e-6 for v in matte.values()):
            sanitized["matte"] = matte

    if vignette_payload:
        vignette = {
            "center_x": _clamp_adjustment(vignette_payload.get("center_x"), 0.0, 1.0),
            "center_y": _clamp_adjustment(vignette_payload.get("center_y"), 0.0, 1.0),
            "size": _clamp_adjustment(vignette_payload.get("size"), 0.0, 100.0),
            "feather": _clamp_adjustment(vignette_payload.get("feather"), 0.0, 100.0),
            "inner_brightness": _clamp_adjustment(vignette_payload.get("inner_brightness"), -100.0, 100.0),
            "outer_brightness": _clamp_adjustment(vignette_payload.get("outer_brightness"), -150.0, 100.0),
            "highlight_protect": _clamp_adjustment(vignette_payload.get("highlight_protect"), 0.0, 100.0),
        }
        if (
            abs(float(vignette["inner_brightness"])) > 1e-6
            or abs(float(vignette["outer_brightness"])) > 1e-6
            or abs(float(vignette["center_x"]) - 0.5) > 1e-6
            or abs(float(vignette["center_y"]) - 0.5) > 1e-6
            or abs(float(vignette["size"]) - 40.0) > 1e-6
            or abs(float(vignette["feather"]) - 45.0) > 1e-6
            or abs(float(vignette["highlight_protect"]) - 70.0) > 1e-6
        ):
            sanitized["vignette"] = vignette

    return sanitized


def sanitize_look_steps(payload) -> list[dict]:
    steps = payload if isinstance(payload, list) else []
    sanitized = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_type = str(step.get("type") or "").strip().lower()
        values = step.get("values") if isinstance(step.get("values"), dict) else {}
        if step_type == "color":
            color = sanitize_look_modules({"color": values}).get("color")
            if color:
                sanitized.append({"type": "color", "values": color})
        elif step_type == "brightness_contrast":
            brightness_contrast = sanitize_look_modules({"brightness_contrast": values}).get("brightness_contrast")
            if brightness_contrast:
                sanitized.append({"type": "brightness_contrast", "values": brightness_contrast})
        elif step_type == "matte":
            matte = sanitize_look_modules({"matte": values}).get("matte")
            if matte:
                sanitized.append({"type": "matte", "values": matte})
        elif step_type == "vignette":
            vignette = sanitize_look_modules({"vignette": values}).get("vignette")
            if vignette:
                sanitized.append({"type": "vignette", "values": vignette})
    return sanitized


def build_look_payload(steps_payload=None, modules_payload=None) -> dict:
    steps = sanitize_look_steps(steps_payload)
    if not steps:
        modules = sanitize_look_modules(modules_payload)
        if modules.get("color"):
            steps.append({"type": "color", "values": modules["color"]})
        if modules.get("brightness_contrast"):
            steps.append({"type": "brightness_contrast", "values": modules["brightness_contrast"]})
        if modules.get("matte"):
            steps.append({"type": "matte", "values": modules["matte"]})
        if modules.get("vignette"):
            steps.append({"type": "vignette", "values": modules["vignette"]})
    modules = {}
    for step in steps:
        if step["type"] == "color":
            modules["color"] = dict(step["values"])
        elif step["type"] == "brightness_contrast":
            modules["brightness_contrast"] = dict(step["values"])
        elif step["type"] == "matte":
            modules["matte"] = dict(step["values"])
        elif step["type"] == "vignette":
            modules["vignette"] = dict(step["values"])
    return {
        "steps": steps,
        "modules": sanitize_look_modules(modules),
    }


def apply_look_steps_to_image(image: Image.Image, steps_payload=None, modules_payload=None) -> tuple[Image.Image, dict]:
    look_payload = build_look_payload(steps_payload, modules_payload)
    output = image.convert("RGBA")
    for step in look_payload["steps"]:
        if step["type"] == "color":
            color = step["values"]
            output = apply_color_adjustments(
                output,
                temperature=color.get("temperature", 0.0),
                tint=color.get("tint", 0.0),
                saturation=color.get("saturation", 0.0),
                vibrance=color.get("vibrance", 0.0),
            )
            continue
        if step["type"] == "brightness_contrast":
            brightness_contrast = step["values"]
            output = apply_brightness_contrast_adjustments(
                output,
                brightness=brightness_contrast.get("brightness", 0.0),
                contrast=brightness_contrast.get("contrast", 0.0),
            )
            continue
        if step["type"] == "matte":
            matte = step["values"]
            output = apply_matte_adjustments(
                output,
                matte=matte.get("matte", 0.0),
                curve=matte.get("curve", 0.0),
                whites=matte.get("whites", 0.0),
            )
            continue
        if step["type"] == "vignette":
            vignette = step["values"]
            output = apply_vignette_adjustments(
                output,
                center_x=vignette.get("center_x", 0.5),
                center_y=vignette.get("center_y", 0.5),
                size=vignette.get("size", 40.0),
                feather=vignette.get("feather", 45.0),
                inner_brightness=vignette.get("inner_brightness", 0.0),
                outer_brightness=vignette.get("outer_brightness", 0.0),
                highlight_protect=vignette.get("highlight_protect", 70.0),
            )
    return output, look_payload


def serialize_look_preset(row: sqlite3.Row) -> dict:
    payload = {"steps": [], "modules": {}}
    try:
        raw_payload = json.loads(row["modules_json"] or "{}")
        if isinstance(raw_payload, dict) and ("steps" in raw_payload or "modules" in raw_payload):
            payload = build_look_payload(raw_payload.get("steps"), raw_payload.get("modules"))
        else:
            payload = build_look_payload(None, raw_payload)
    except Exception:
        payload = {"steps": [], "modules": {}}
    return {
        "id": int(row["id"]),
        "name": str(row["name"] or "").strip(),
        "steps": payload["steps"],
        "modules": payload["modules"],
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "last_used_at": str(row["last_used_at"] or ""),
        "use_count": int(row["use_count"] or 0),
    }


def apply_color_adjustments(image: Image.Image, *, temperature=0.0, tint=0.0, saturation=0.0, vibrance=0.0) -> Image.Image:
    if np is None:
        raise RuntimeError("Color adjustment dependencies are not installed")

    temperature = _clamp_adjustment(temperature)
    tint = _clamp_adjustment(tint)
    saturation = _clamp_adjustment(saturation)
    vibrance = _clamp_adjustment(vibrance)

    rgba = np.array(image.convert("RGBA"), dtype=np.float32) / 255.0
    rgb = rgba[:, :, :3]
    alpha = rgba[:, :, 3:4]

    temp_shift = temperature / 100.0
    tint_shift = tint / 100.0

    red_scale = 1.0 + (temp_shift * 0.18) + (tint_shift * 0.08)
    green_scale = 1.0 - (abs(temp_shift) * 0.05) - (tint_shift * 0.14)
    blue_scale = 1.0 - (temp_shift * 0.18) + (tint_shift * 0.08)
    rgb = np.clip(rgb * np.array([red_scale, green_scale, blue_scale], dtype=np.float32), 0.0, 1.0)

    luma = (
        rgb[:, :, 0:1] * 0.2126
        + rgb[:, :, 1:2] * 0.7152
        + rgb[:, :, 2:3] * 0.0722
    )

    saturation_factor = 1.0 + (saturation / 100.0)
    rgb = np.clip(luma + ((rgb - luma) * saturation_factor), 0.0, 1.0)

    current_sat = np.max(rgb, axis=2, keepdims=True) - np.min(rgb, axis=2, keepdims=True)
    vibrance_amount = vibrance / 100.0
    if vibrance_amount >= 0:
        vibrance_factor = 1.0 + (vibrance_amount * (1.0 - current_sat))
    else:
        vibrance_factor = 1.0 + (vibrance_amount * current_sat)
    rgb = np.clip(luma + ((rgb - luma) * vibrance_factor), 0.0, 1.0)

    output = np.concatenate((rgb, alpha), axis=2)
    return Image.fromarray((output * 255.0).astype(np.uint8), mode="RGBA")


def apply_brightness_contrast_adjustments(image: Image.Image, *, brightness=0.0, contrast=0.0) -> Image.Image:
    if np is None:
        raise RuntimeError("Brightness/contrast adjustment dependencies are not installed")

    brightness = _clamp_adjustment(brightness, -35.0, 35.0)
    contrast = _clamp_adjustment(contrast, -35.0, 35.0)

    rgba = np.array(image.convert("RGBA"), dtype=np.float32) / 255.0
    rgb = rgba[:, :, :3]
    alpha = rgba[:, :, 3:4]

    base_luma = (
        (rgb[:, :, 0:1] * 0.2126)
        + (rgb[:, :, 1:2] * 0.7152)
        + (rgb[:, :, 2:3] * 0.0722)
    )
    luma = base_luma.copy()

    brightness_amount = brightness / 100.0
    if brightness_amount > 0.0:
        headroom = np.power(np.clip(1.0 - luma, 0.0, 1.0), 1.08)
        lift_strength = np.power(brightness_amount, 1.18) * 0.72
        shadow_support = 0.72 + (0.28 * np.power(np.clip(1.0 - luma, 0.0, 1.0), 0.9))
        luma = np.clip(luma + (lift_strength * headroom * shadow_support), 0.0, 1.0)
    elif brightness_amount < 0.0:
        dark_strength = np.power(-brightness_amount, 1.08) * 0.82
        tonal_weight = 0.38 + (0.62 * np.power(np.clip(luma, 0.0, 1.0), 0.7))
        luma = np.clip(luma * (1.0 - (dark_strength * tonal_weight)), 0.0, 1.0)

    contrast_amount = contrast / 100.0
    if abs(contrast_amount) > 1e-6:
        pivot = 0.5
        contrast_factor = (1.0 + (contrast_amount * 1.2)) if contrast_amount > 0.0 else (1.0 / (1.0 + ((-contrast_amount) * 1.2)))
        luma = np.clip(pivot + ((luma - pivot) * contrast_factor), 0.0, 1.0)

    chroma_scale = np.clip(1.0 - (max(0.0, brightness_amount) * 0.08) - (max(0.0, contrast_amount) * 0.03), 0.9, 1.02)
    rgb = np.clip(luma + ((rgb - base_luma) * chroma_scale), 0.0, 1.0)

    output = np.concatenate((rgb, alpha), axis=2)
    return Image.fromarray((output * 255.0).astype(np.uint8), mode="RGBA")


def apply_matte_adjustments(image: Image.Image, *, matte=0.0, curve=0.0, whites=0.0) -> Image.Image:
    if np is None:
        raise RuntimeError("Matte adjustment dependencies are not installed")

    matte = _clamp_adjustment(matte, 0.0, 100.0)
    curve = _clamp_adjustment(curve, 0.0, 100.0)
    whites = _clamp_adjustment(whites, -100.0, 100.0)

    rgba = np.array(image.convert("RGBA"), dtype=np.float32) / 255.0
    rgb = rgba[:, :, :3]
    alpha = rgba[:, :, 3:4]
    base_luma = (
        (rgb[:, :, 0:1] * 0.2126)
        + (rgb[:, :, 1:2] * 0.7152)
        + (rgb[:, :, 2:3] * 0.0722)
    )
    luma = base_luma.copy()

    matte_amount = (matte / 100.0) * 0.22
    if matte_amount > 0.0:
        shadow_mask = np.power(np.clip(1.0 - luma, 0.0, 1.0), 1.65)
        luma = np.clip(luma + (matte_amount * shadow_mask * (1.0 - (luma * 0.35))), 0.0, 1.0)

    whites_amount = whites / 100.0
    highlight_mask = np.power(np.clip((luma - 0.45) / 0.55, 0.0, 1.0), 1.6)
    if whites_amount < 0.0:
        clamp_amount = -whites_amount
        luma = np.clip(
            luma - (clamp_amount * highlight_mask * np.maximum(0.0, luma - 0.45) * 0.55),
            0.0,
            1.0,
        )
    elif whites_amount > 0.0:
        luma = np.clip(luma + (whites_amount * highlight_mask * (1.0 - luma) * 0.24), 0.0, 1.0)

    curve_amount = curve / 100.0
    if curve_amount > 0.0:
        mid_mask = np.power(np.clip(1.0 - np.abs((luma - 0.5) / 0.5), 0.0, 1.0), 1.35)
        contrasted = np.clip(0.5 + ((luma - 0.5) * (1.0 + (curve_amount * 0.72))), 0.0, 1.0)
        curve_blend = curve_amount * mid_mask
        luma = np.clip((luma * (1.0 - curve_blend)) + (contrasted * curve_blend), 0.0, 1.0)

    chroma_scale = 1.0 - (matte_amount * 0.18 * np.power(np.clip(1.0 - base_luma, 0.0, 1.0), 1.2))
    rgb = np.clip(luma + ((rgb - base_luma) * chroma_scale), 0.0, 1.0)

    output = np.concatenate((rgb, alpha), axis=2)
    return Image.fromarray((output * 255.0).astype(np.uint8), mode="RGBA")


def apply_vignette_adjustments(
    image: Image.Image,
    *,
    center_x=0.5,
    center_y=0.5,
    size=40.0,
    feather=45.0,
    inner_brightness=0.0,
    outer_brightness=0.0,
    highlight_protect=70.0,
) -> Image.Image:
    if np is None:
        raise RuntimeError("Vignette adjustment dependencies are not installed")

    center_x = _clamp_adjustment(center_x, 0.0, 1.0)
    center_y = _clamp_adjustment(center_y, 0.0, 1.0)
    size = _clamp_adjustment(size, 0.0, 100.0)
    feather = _clamp_adjustment(feather, 0.0, 100.0)
    inner_brightness = _clamp_adjustment(inner_brightness, -100.0, 100.0)
    outer_brightness = _clamp_adjustment(outer_brightness, -150.0, 100.0)
    highlight_protect = _clamp_adjustment(highlight_protect, 0.0, 100.0)

    rgba = np.array(image.convert("RGBA"), dtype=np.float32) / 255.0
    rgb = rgba[:, :, :3]
    alpha = rgba[:, :, 3:4]
    height, width = rgb.shape[:2]

    base_luma = (
        (rgb[:, :, 0:1] * 0.2126)
        + (rgb[:, :, 1:2] * 0.7152)
        + (rgb[:, :, 2:3] * 0.0722)
    )

    x = np.linspace(0.0, 1.0, width, dtype=np.float32)
    y = np.linspace(0.0, 1.0, height, dtype=np.float32)
    grid_x, grid_y = np.meshgrid(x, y)
    distances = np.sqrt(((grid_x - center_x) ** 2) + ((grid_y - center_y) ** 2))
    max_distance = max(
        math.hypot(center_x, center_y),
        math.hypot(1.0 - center_x, center_y),
        math.hypot(center_x, 1.0 - center_y),
        math.hypot(1.0 - center_x, 1.0 - center_y),
        1e-6,
    )
    radial = np.clip((distances / max_distance)[:, :, None], 0.0, 1.0)

    clear_radius = 0.06 + ((size / 100.0) * 0.72)
    feather_width = 0.04 + ((feather / 100.0) * 0.52)
    vignette_mask = np.clip((radial - clear_radius) / max(feather_width, 1e-6), 0.0, 1.0)
    vignette_mask = vignette_mask * vignette_mask * (3.0 - (2.0 * vignette_mask))
    center_mask = 1.0 - vignette_mask

    shadow_allow = np.power(np.clip((base_luma - 0.04) / 0.62, 0.0, 1.0), 0.85)
    highlight_headroom = np.power(np.clip(1.0 - base_luma, 0.0, 1.0), 1.05)
    highlight_rolloff = np.power(np.clip((1.0 - base_luma) / 0.88, 0.0, 1.0), 0.82)
    peak_rgb = np.max(rgb, axis=2, keepdims=True)
    highlight_mask = np.power(np.clip((peak_rgb - 0.28) / 0.5, 0.0, 1.0), 0.82)
    highlight_preserve = highlight_protect / 100.0

    def apply_rgb_brightness_region(target_rgb, amount, region_mask):
        strength = float(amount) / 100.0
        if abs(strength) <= 1e-6:
            return target_rgb
        if strength > 0.0:
            current_luma = (
                (target_rgb[:, :, 0:1] * 0.2126)
                + (target_rgb[:, :, 1:2] * 0.7152)
                + (target_rgb[:, :, 2:3] * 0.0722)
            )
            lift_strength = np.power(strength, 1.75) * 0.34
            tonal_headroom = np.power(np.clip(1.0 - current_luma, 0.0, 1.0), 1.2)
            lift = lift_strength * region_mask * tonal_headroom * (0.7 + (0.3 * shadow_allow))
            lift *= (0.98 - (0.42 * highlight_preserve))
            lifted_luma = np.clip(current_luma + (lift * (0.2 + (0.8 * tonal_headroom))), 0.0, 1.0)
            chroma_retention = np.clip(1.0 - (lift * 0.16), 0.88, 1.0)
            lifted = lifted_luma + ((target_rgb - current_luma) * chroma_retention)
            return np.clip(lifted, 0.0, 1.0)
        darken_strength = np.power(-strength, 1.15)
        darken = darken_strength * region_mask * shadow_allow
        darken *= (0.52 + (0.48 * highlight_rolloff))
        darkened = np.clip(target_rgb * (1.0 - darken), 0.0, 1.0)
        preserve_mix = np.clip(region_mask * highlight_mask * highlight_preserve * 0.92, 0.0, 0.96)
        return np.clip((darkened * (1.0 - preserve_mix)) + (target_rgb * preserve_mix), 0.0, 1.0)

    rgb = apply_rgb_brightness_region(rgb, inner_brightness, center_mask)
    rgb = apply_rgb_brightness_region(rgb, outer_brightness, vignette_mask)

    current_luma = (
        (rgb[:, :, 0:1] * 0.2126)
        + (rgb[:, :, 1:2] * 0.7152)
        + (rgb[:, :, 2:3] * 0.0722)
    )
    chroma_scale = np.clip(
        1.0
        - (np.clip(-outer_brightness, 0.0, 150.0) / 150.0) * vignette_mask * 0.06
        - (np.clip(-inner_brightness, 0.0, 100.0) / 100.0) * center_mask * 0.04,
        0.9,
        1.06,
    )
    chroma_scale = np.clip(chroma_scale + (highlight_mask * vignette_mask * highlight_preserve * 0.24), 0.94, 1.18)
    rgb = np.clip(current_luma + ((rgb - current_luma) * chroma_scale), 0.0, 1.0)

    output = np.concatenate((rgb, alpha), axis=2)
    return Image.fromarray((output * 255.0).astype(np.uint8), mode="RGBA")


def get_effective_image_path_for_variant(rel_path: str, source_path: Path, st=None, variant: Optional[str] = None) -> Path:
    requested = str(variant or "").strip().lower()
    if requested == "original":
        return source_path
    if requested == "editor-base":
        return get_editor_base_image_path(rel_path, source_path, st=st)
    return get_effective_image_path(rel_path, source_path, st=st)


def build_png_item(rel_path: str, name: str, st, *, current_edit: Optional[dict] = None) -> dict:
    if current_edit is None:
        current_edit = get_current_image_edit_summary(rel_path, st)
    thumb_sig = build_thumb_signature_from_current_edit(rel_path, st, current_edit=current_edit)
    square_cache_path = build_thumb_cache_path_from_signature(thumb_sig, "square")
    full_cache_path = build_thumb_cache_path_from_signature(thumb_sig, "full")
    preview_cache_path = build_thumb_cache_path_from_signature(thumb_sig, "preview")
    return {
        "type": "png",
        "name": name,
        "rel_path": rel_path,
        "size_bytes": st.st_size,
        "mtime": int(st.st_mtime),
        "thumb_sig": thumb_sig,
        "image_edit": build_gallery_image_edit_payload(current_edit),
        "cache_paths": {
            "square": square_cache_path,
            "full": full_cache_path,
            "preview": preview_cache_path,
        },
    }


def get_favorite_entry(rel_path: str):
    with favorites_state_lock:
        state = load_favorites_state()
        return state["entries"].get(rel_path)


def is_favorited(rel_path: str, st=None) -> bool:
    entry = get_favorite_entry(rel_path)
    if not entry:
        return False
    if st is None:
        try:
            st = resolve_safe_path(rel_path).stat()
        except Exception:
            return False
    return int(entry.get("mtime") or 0) == int(st.st_mtime) and int(entry.get("size") or 0) == int(st.st_size)


def set_favorite(rel_path: str, st, enabled: bool) -> bool:
    with favorites_state_lock:
        state = load_favorites_state()
        entries = state["entries"]
        if enabled:
            entries[rel_path] = {
                "mtime": int(st.st_mtime),
                "size": int(st.st_size),
                "added_at": utc_now_iso(),
            }
            save_favorites_state(state)
            return True
        entries.pop(rel_path, None)
        save_favorites_state(state)
        return False


def get_favorites_listing(sort_key: str = "date", sort_dir: str = "desc"):
    with favorites_state_lock:
        state = load_favorites_state()

    valid_items = []
    changed = False
    for rel_path, entry in list(state["entries"].items()):
        try:
            path = resolve_safe_path(rel_path)
        except ValueError:
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        if should_exclude_png_path(path) or not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        try:
            st = path.stat()
        except OSError:
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        if int(entry.get("mtime") or 0) != int(st.st_mtime) or int(entry.get("size") or 0) != int(st.st_size):
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        valid_items.append(build_png_item(rel_path, path.name, st))

    if changed:
        with favorites_state_lock:
            save_favorites_state(state)

    if sort_key == "name":
        valid_items.sort(key=lambda item: item["name"].lower(), reverse=(sort_dir == "desc"))
    else:
        valid_items.sort(key=lambda item: (item["mtime"], item["name"].lower()), reverse=(sort_dir == "desc"))

    breadcrumb = [
        {"name": "root", "rel_path": ""},
        {"name": "Favorites", "rel_path": FAVORITES_DIR_KEY},
    ]
    return valid_items, breadcrumb


def build_favorites_folder_item():
    with favorites_state_lock:
        state = load_favorites_state()
    latest_mtime = 0
    count = 0
    newest_cover = None
    changed = False
    for rel_path, item in list(state["entries"].items()):
        try:
            path = resolve_safe_path(rel_path)
        except ValueError:
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        if should_exclude_png_path(path) or not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        try:
            st = path.stat()
        except OSError:
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        if int(item.get("mtime") or 0) != int(st.st_mtime) or int(item.get("size") or 0) != int(st.st_size):
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        count += 1
        item_mtime = int(st.st_mtime)
        latest_mtime = max(latest_mtime, item_mtime)
        cover_sort_key = (str(item.get("added_at") or ""), item_mtime, path.name.lower())
        if newest_cover is None or cover_sort_key > newest_cover["sort_key"]:
            newest_cover = {
                "sort_key": cover_sort_key,
                "cover": {
                    "name": path.name,
                    "rel_path": rel_path,
                    "thumb_sig": build_thumb_signature(rel_path, st),
                },
            }
    if changed:
        with favorites_state_lock:
            save_favorites_state(state)
    return {
        "type": "favorites",
        "name": "Favorites",
        "rel_path": FAVORITES_DIR_KEY,
        "mtime": latest_mtime,
        "count": count,
        "cover": newest_cover["cover"] if newest_cover else None,
    }


def iter_current_edited_items():
    if not EDITS_DIR.exists() or not EDITS_DIR.is_dir():
        return
    seen_rel_paths = set()
    for meta_path in EDITS_DIR.glob("*/*/meta.json"):
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        meta = normalize_image_edit_meta(payload)
        rel_path = meta.get("rel_path") or ""
        if not rel_path or rel_path in seen_rel_paths:
            continue
        seen_rel_paths.add(rel_path)
        try:
            source_path = resolve_safe_path(rel_path)
        except ValueError:
            continue
        if should_exclude_png_path(source_path) or not source_path.exists() or not source_path.is_file() or source_path.suffix.lower() != ".png":
            continue
        try:
            st = source_path.stat()
        except OSError:
            continue
        current_edit = get_current_image_edit(rel_path, st)
        if not current_edit:
            continue
        yield {
            "rel_path": rel_path,
            "path": source_path,
            "stat": st,
            "meta": current_edit.get("meta") or {},
            "current_path": current_edit.get("current_path"),
        }


def get_edits_listing(sort_key: str = "date", sort_dir: str = "desc"):
    valid_items = []
    for item in iter_current_edited_items():
        png_item = build_png_item(item["rel_path"], item["path"].name, item["stat"], current_edit=item)
        png_item["edit_updated_at"] = str((item.get("meta") or {}).get("updated_at") or "")
        valid_items.append(png_item)

    if sort_key == "name":
        valid_items.sort(key=lambda item: item["name"].lower(), reverse=(sort_dir == "desc"))
    else:
        valid_items.sort(
            key=lambda item: (
                item.get("edit_updated_at") or "",
                item["mtime"],
                item["name"].lower(),
            ),
            reverse=(sort_dir == "desc"),
        )

    breadcrumb = [
        {"name": "root", "rel_path": ""},
        {"name": "Edits", "rel_path": EDITS_DIR_KEY},
    ]
    return valid_items, breadcrumb


def build_edits_folder_item():
    latest_mtime = 0
    count = 0
    newest_cover = None
    for item in iter_current_edited_items():
        count += 1
        current_path = item.get("current_path")
        current_mtime = 0
        try:
            if current_path is not None:
                current_mtime = int(current_path.stat().st_mtime)
                latest_mtime = max(latest_mtime, current_mtime)
        except OSError:
            pass
        meta = item.get("meta") or {}
        rel_path = item.get("rel_path") or ""
        source_path = item.get("path")
        st = item.get("stat")
        if rel_path and source_path is not None and st is not None:
            cover_sort_key = (str(meta.get("updated_at") or ""), current_mtime, source_path.name.lower())
            if newest_cover is None or cover_sort_key > newest_cover["sort_key"]:
                newest_cover = {
                    "sort_key": cover_sort_key,
                    "cover": {
                        "name": source_path.name,
                        "rel_path": rel_path,
                        "thumb_sig": build_thumb_signature_from_current_edit(rel_path, st, current_edit=item),
                    },
                }
    return {
        "type": "edits",
        "name": "Edits",
        "rel_path": EDITS_DIR_KEY,
        "mtime": latest_mtime,
        "count": count,
        "cover": newest_cover["cover"] if newest_cover else None,
    }


def get_valid_favorite_paths():
    with favorites_state_lock:
        state = load_favorites_state()

    valid_paths = []
    changed = False
    for rel_path, entry in list(state["entries"].items()):
        try:
            path = resolve_safe_path(rel_path)
        except ValueError:
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        if should_exclude_png_path(path) or not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        try:
            st = path.stat()
        except OSError:
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        if int(entry.get("mtime") or 0) != int(st.st_mtime) or int(entry.get("size") or 0) != int(st.st_size):
            state["entries"].pop(rel_path, None)
            changed = True
            continue
        valid_paths.append(rel_path)

    if changed:
        with favorites_state_lock:
            save_favorites_state(state)

    return valid_paths


def list_search_scope_directories():
    ensure_root_exists()
    items = []
    try:
        with os.scandir(BROWSE_ROOT) as scan:
            for entry in scan:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                if should_skip_dir_name(entry.name):
                    continue
                items.append(entry.name)
    except OSError:
        return []
    items.sort(key=lambda value: value.lower())
    return items


def ensure_log_dir():
    APP_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_thumb_ready_state_dir():
    THUMB_READY_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_rebuild_status_dir():
    REBUILD_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_metadata_index_status_dir():
    METADATA_INDEX_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_ai_analysis_status_dir():
    AI_ANALYSIS_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_thumb_dimensions_db_dir():
    THUMB_DIMENSIONS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=True, sort_keys=True)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        tmp.write(serialized)
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def configure_logging():
    if app_logger.handlers:
        return
    app_logger.setLevel(logging.INFO)
    app_logger.propagate = False
    try:
        ensure_log_dir()
        handler = logging.FileHandler(APP_LOG_PATH, encoding="utf-8")
    except Exception:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    app_logger.addHandler(handler)


def log_event(event_type: str, message: str, **fields):
    configure_logging()
    payload = {
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "event": event_type,
        "message": message,
    }
    if has_request_context():
        user_agent = (request.headers.get("User-Agent") or "").strip()
        if user_agent and "user_agent" not in fields:
            payload["user_agent"] = user_agent[:220]
        if request.path and "request_path" not in fields:
            payload["request_path"] = request.path
    payload.update(fields)
    app_logger.info(json.dumps(payload, ensure_ascii=True))


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def normalize_rebuild_status_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        snapshot = {}
    normalized = dict(rebuild_status)
    normalized.update(snapshot)
    normalized["cancel_requested"] = bool(normalized.get("cancel_requested"))
    normalized["scope_dir"] = str(normalized.get("scope_dir") or "")
    normalized["scope_label"] = str(normalized.get("scope_label") or ("root" if not normalized["scope_dir"] else normalized["scope_dir"]))
    normalized["force_rebuild"] = bool(normalized.get("force_rebuild"))
    total_tasks = int(normalized.get("total_tasks") or 0)
    completed_tasks = int(normalized.get("completed_tasks") or 0)
    state = normalized.get("state") or "idle"
    if total_tasks <= 0:
        normalized["progress_pct"] = 100 if state == "completed" else 0
    else:
        normalized["progress_pct"] = min(100, int((completed_tasks / total_tasks) * 100))
    summary = normalized.get("summary")
    normalized["summary"] = dict(summary) if isinstance(summary, dict) else summary
    return normalized


def is_rebuild_status_active(snapshot):
    if not isinstance(snapshot, dict):
        return False
    state = snapshot.get("state")
    phase = snapshot.get("phase")
    return state in {"queued", "running"} or phase in {"queued", "scanning", "clearing-cache", "building", "cancel-requested"}


def is_rebuild_cancel_requested(snapshot):
    return bool(snapshot.get("cancel_requested")) if isinstance(snapshot, dict) else False


def get_rebuild_status_snapshot(prefer_disk=True):
    if prefer_disk:
        loaded = load_rebuild_status_from_disk()
        if isinstance(loaded, dict):
            normalized = normalize_rebuild_status_snapshot(loaded)
            with rebuild_status_lock:
                rebuild_status.update(normalized)
            return normalized
    with rebuild_status_lock:
        return normalize_rebuild_status_snapshot(rebuild_status)


def save_rebuild_status_locked():
    ensure_rebuild_status_dir()
    payload = dict(rebuild_status)
    summary = payload.get("summary")
    payload["summary"] = dict(summary) if isinstance(summary, dict) else summary
    atomic_write_json(REBUILD_STATUS_PATH, payload)


def load_rebuild_status_from_disk():
    if not REBUILD_STATUS_PATH.exists() or not REBUILD_STATUS_PATH.is_file():
        return None
    try:
        data = json.loads(REBUILD_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def initialize_rebuild_status():
    loaded = load_rebuild_status_from_disk()
    if not loaded:
        with rebuild_status_lock:
            save_rebuild_status_locked()
        return

    with rebuild_status_lock:
        rebuild_status.update(loaded)
        state = rebuild_status.get("state")
        phase = rebuild_status.get("phase")
        if state in {"queued", "running"} or phase in {"queued", "scanning", "clearing-cache", "building"}:
            rebuild_status["state"] = "interrupted"
            rebuild_status["phase"] = "interrupted"
            rebuild_status["cancel_requested"] = False
            rebuild_status["error"] = "Rebuild stopped because the app or container restarted"
            rebuild_status["current_directory"] = None
            rebuild_status["finished_at"] = utc_now_iso()
        rebuild_status.update(normalize_rebuild_status_snapshot(rebuild_status))
        save_rebuild_status_locked()


def update_rebuild_status(*, persist=True, **fields):
    with rebuild_status_lock:
        rebuild_status.update(fields)
        rebuild_status.update(normalize_rebuild_status_snapshot(rebuild_status))
        if persist:
            save_rebuild_status_locked()


def normalize_metadata_index_status_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        snapshot = {}
    normalized = dict(metadata_index_status)
    normalized.update(snapshot)
    normalized["cancel_requested"] = bool(normalized.get("cancel_requested"))
    normalized["scope_dir"] = str(normalized.get("scope_dir") or "")
    normalized["scope_label"] = str(normalized.get("scope_label") or ("root" if not normalized["scope_dir"] else normalized["scope_dir"]))
    total_tasks = int(normalized.get("total_tasks") or 0)
    completed_tasks = int(normalized.get("completed_tasks") or 0)
    state = normalized.get("state") or "idle"
    if total_tasks <= 0:
        normalized["progress_pct"] = 100 if state == "completed" else 0
    else:
        normalized["progress_pct"] = min(100, int((completed_tasks / total_tasks) * 100))
    summary = normalized.get("summary")
    normalized["summary"] = dict(summary) if isinstance(summary, dict) else summary
    return normalized


def is_metadata_index_active(snapshot):
    if not isinstance(snapshot, dict):
        return False
    state = snapshot.get("state")
    phase = snapshot.get("phase")
    return state in {"queued", "running"} or phase in {"queued", "scanning", "indexing", "finalizing", "cancel-requested"}


def is_metadata_index_cancel_requested(snapshot):
    return bool(snapshot.get("cancel_requested")) if isinstance(snapshot, dict) else False


def get_metadata_index_status_snapshot(prefer_disk=True):
    if prefer_disk:
        loaded = load_metadata_index_status_from_disk()
        if isinstance(loaded, dict):
            normalized = normalize_metadata_index_status_snapshot(loaded)
            with metadata_index_status_lock:
                metadata_index_status.update(normalized)
            return normalized
    with metadata_index_status_lock:
        return normalize_metadata_index_status_snapshot(metadata_index_status)


def save_metadata_index_status_locked():
    ensure_metadata_index_status_dir()
    payload = dict(metadata_index_status)
    summary = payload.get("summary")
    payload["summary"] = dict(summary) if isinstance(summary, dict) else summary
    atomic_write_json(METADATA_INDEX_STATUS_PATH, payload)


def load_metadata_index_status_from_disk():
    if not METADATA_INDEX_STATUS_PATH.exists() or not METADATA_INDEX_STATUS_PATH.is_file():
        return None
    try:
        data = json.loads(METADATA_INDEX_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def initialize_metadata_index_status():
    loaded = load_metadata_index_status_from_disk()
    if not loaded:
        with metadata_index_status_lock:
            save_metadata_index_status_locked()
        return

    with metadata_index_status_lock:
        metadata_index_status.update(loaded)
        state = metadata_index_status.get("state")
        phase = metadata_index_status.get("phase")
        if state in {"queued", "running"} or phase in {"queued", "scanning", "indexing", "finalizing"}:
            metadata_index_status["state"] = "interrupted"
            metadata_index_status["phase"] = "interrupted"
            metadata_index_status["cancel_requested"] = False
            metadata_index_status["error"] = "Metadata indexing stopped because the app or container restarted"
            metadata_index_status["current_directory"] = None
            metadata_index_status["finished_at"] = utc_now_iso()
        metadata_index_status.update(normalize_metadata_index_status_snapshot(metadata_index_status))
        save_metadata_index_status_locked()


def update_metadata_index_status(*, persist=True, **fields):
    with metadata_index_status_lock:
        metadata_index_status.update(fields)
        metadata_index_status.update(normalize_metadata_index_status_snapshot(metadata_index_status))
        if persist:
            save_metadata_index_status_locked()


def normalize_ai_analysis_status_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        snapshot = {}
    normalized = dict(ai_analysis_status)
    normalized.update(snapshot)
    normalized["cancel_requested"] = bool(normalized.get("cancel_requested"))
    normalized["scope_dir"] = str(normalized.get("scope_dir") or "")
    normalized["scope_label"] = str(normalized.get("scope_label") or ("root" if not normalized["scope_dir"] else normalized["scope_dir"]))
    normalized["force_rebuild"] = bool(normalized.get("force_rebuild") or normalized.get("force_reindex"))
    total_tasks = int(normalized.get("total_tasks") or 0)
    completed_tasks = int(normalized.get("completed_tasks") or 0)
    state = normalized.get("state") or "idle"
    if total_tasks <= 0:
        normalized["progress_pct"] = 100 if state == "completed" else 0
    else:
        normalized["progress_pct"] = min(100, int((completed_tasks / total_tasks) * 100))
    summary = normalized.get("summary")
    normalized["summary"] = dict(summary) if isinstance(summary, dict) else summary
    return normalized


def is_ai_analysis_active(snapshot):
    if not isinstance(snapshot, dict):
        return False
    state = snapshot.get("state")
    phase = snapshot.get("phase")
    return state in {"queued", "running"} or phase in {"queued", "scanning", "analyzing", "finalizing", "cancel-requested"}


def is_ai_analysis_cancel_requested(snapshot):
    return bool(snapshot.get("cancel_requested")) if isinstance(snapshot, dict) else False


def get_ai_analysis_status_snapshot(prefer_disk=True):
    if prefer_disk:
        loaded = load_ai_analysis_status_from_disk()
        if isinstance(loaded, dict):
            normalized = normalize_ai_analysis_status_snapshot(loaded)
            with ai_analysis_status_lock:
                ai_analysis_status.update(normalized)
            return normalized
    with ai_analysis_status_lock:
        return normalize_ai_analysis_status_snapshot(ai_analysis_status)


def save_ai_analysis_status_locked():
    ensure_ai_analysis_status_dir()
    payload = dict(ai_analysis_status)
    summary = payload.get("summary")
    payload["summary"] = dict(summary) if isinstance(summary, dict) else summary
    atomic_write_json(AI_ANALYSIS_STATUS_PATH, payload)


def load_ai_analysis_status_from_disk():
    if not AI_ANALYSIS_STATUS_PATH.exists() or not AI_ANALYSIS_STATUS_PATH.is_file():
        return None
    try:
        data = json.loads(AI_ANALYSIS_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def initialize_ai_analysis_status():
    loaded = load_ai_analysis_status_from_disk()
    if not loaded:
        with ai_analysis_status_lock:
            save_ai_analysis_status_locked()
        return

    with ai_analysis_status_lock:
        ai_analysis_status.update(loaded)
        state = ai_analysis_status.get("state")
        phase = ai_analysis_status.get("phase")
        if state in {"queued", "running"} or phase in {"queued", "scanning", "analyzing", "finalizing"}:
            ai_analysis_status["state"] = "interrupted"
            ai_analysis_status["phase"] = "interrupted"
            ai_analysis_status["cancel_requested"] = False
            ai_analysis_status["error"] = "AI vision pass stopped because the app or container restarted"
            ai_analysis_status["current_directory"] = None
            ai_analysis_status["finished_at"] = utc_now_iso()
        ai_analysis_status.update(normalize_ai_analysis_status_snapshot(ai_analysis_status))
        save_ai_analysis_status_locked()


def update_ai_analysis_status(*, persist=True, **fields):
    with ai_analysis_status_lock:
        ai_analysis_status.update(fields)
        ai_analysis_status.update(normalize_ai_analysis_status_snapshot(ai_analysis_status))
        if persist:
            save_ai_analysis_status_locked()


def load_thumb_ready_state():
    if not THUMB_READY_STATE_PATH.exists() or not THUMB_READY_STATE_PATH.is_file():
        return {}
    try:
        return json.loads(THUMB_READY_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_thumb_ready_state(state):
    ensure_thumb_ready_state_dir()
    THUMB_READY_STATE_PATH.write_text(json.dumps(state, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def get_thumbnail_mode_name(mode: str) -> str:
    if mode == "square":
        return "square"
    if mode == "preview":
        return "preview"
    return "full"


def should_exclude_png_path(path: Path) -> bool:
    if not isinstance(path, Path):
        return False
    name = path.name
    if name.startswith("Face_Upscale_"):
        return True
    if name.startswith("Upscale_"):
        parent_name = path.parent.name if path.parent != BROWSE_ROOT else ""
        return not parent_name.startswith("Upscale_")
    return False


def should_skip_hidden_name(name: str) -> bool:
    return not SHOW_HIDDEN and isinstance(name, str) and name.startswith(".")


def should_skip_dir_name(name: str) -> bool:
    if not isinstance(name, str):
        return False
    if should_skip_hidden_name(name):
        return True
    return name == "@eaDir" or name.lower().endswith("_overlays")


def directory_contains_browseable_png(dir_path: Path, cache: Optional[dict] = None) -> bool:
    if cache is None:
        cache = {}

    resolved = dir_path.resolve()
    cached = cache.get(resolved)
    if cached is not None:
        return cached

    try:
        with os.scandir(resolved) as scan:
            subdirs = []
            for entry in scan:
                name = entry.name
                if should_skip_hidden_name(name):
                    continue
                if entry.is_file(follow_symlinks=False):
                    if name.lower().endswith(".png") and not should_exclude_png_path(Path(entry.path)):
                        cache[resolved] = True
                        return True
                    continue
                if entry.is_dir(follow_symlinks=False) and not should_skip_dir_name(name):
                    subdirs.append(Path(entry.path))
    except OSError:
        cache[resolved] = False
        return False

    for subdir in subdirs:
        if directory_contains_browseable_png(subdir, cache):
            cache[resolved] = True
            return True

    cache[resolved] = False
    return False


def ensure_root_exists():
    if not BROWSE_ROOT.exists():
        raise RuntimeError(f"BROWSE_ROOT does not exist: {BROWSE_ROOT}")
    if not BROWSE_ROOT.is_dir():
        raise RuntimeError(f"BROWSE_ROOT is not a directory: {BROWSE_ROOT}")


def resolve_safe_path(rel_path: str) -> Path:
    rel_path = (rel_path or "").strip().replace("\\", "/")
    rel = Path(rel_path)
    if rel.is_absolute():
        raise ValueError("Absolute paths are not allowed")
    candidate = (BROWSE_ROOT / rel).resolve()
    try:
        candidate.relative_to(BROWSE_ROOT)
    except ValueError as exc:
        raise ValueError("Path escapes root") from exc
    return candidate


def rel_from_root(path: Path) -> str:
    return path.resolve().relative_to(BROWSE_ROOT).as_posix()


def read_png_dimensions(path: Path) -> tuple[int, int]:
    try:
        with path.open("rb") as fh:
            header = fh.read(24)
    except OSError:
        return 0, 0

    if len(header) < 24:
        return 0, 0
    if header[:8] != b"\x89PNG\r\n\x1a\n" or header[12:16] != b"IHDR":
        return 0, 0
    try:
        width, height = struct.unpack(">II", header[16:24])
    except struct.error:
        return 0, 0
    if width < 1 or height < 1:
        return 0, 0
    return int(width), int(height)


def read_image_dimensions(path: Path) -> tuple[int, int]:
    if not isinstance(path, Path) or not path.exists() or not path.is_file():
        return 0, 0
    if path.suffix.lower() == ".png":
        return read_png_dimensions(path)
    try:
        with Image.open(path) as img:
            width, height = img.size
    except Exception:
        return 0, 0
    if width < 1 or height < 1:
        return 0, 0
    return int(width), int(height)


def ensure_thumb_cache_dir():
    THUMB_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def connect_thumb_dimensions_db() -> sqlite3.Connection:
    ensure_thumb_dimensions_db_dir()
    conn = sqlite3.connect(THUMB_DIMENSIONS_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS thumb_dimensions (
            thumb_sig TEXT PRIMARY KEY,
            width INTEGER NOT NULL,
            height INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    return conn


def get_cached_thumb_dimensions(conn: sqlite3.Connection, thumb_sig: str) -> tuple[int, int]:
    if not thumb_sig:
        return 0, 0
    row = conn.execute(
        "SELECT width, height FROM thumb_dimensions WHERE thumb_sig = ?",
        (thumb_sig,),
    ).fetchone()
    if not row:
        return 0, 0
    width = int(row["width"] or 0)
    height = int(row["height"] or 0)
    if width < 1 or height < 1:
        return 0, 0
    return width, height


def save_cached_thumb_dimensions(conn: sqlite3.Connection, thumb_sig: str, width: int, height: int):
    if not thumb_sig or width < 1 or height < 1:
        return
    conn.execute(
        """
        INSERT INTO thumb_dimensions (thumb_sig, width, height, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(thumb_sig) DO UPDATE SET
            width = excluded.width,
            height = excluded.height,
            updated_at = excluded.updated_at
        """,
        (thumb_sig, int(width), int(height), utc_now_iso()),
    )


def build_thumb_cache_path(rel_path: str, st, mode: str) -> Path:
    return build_thumb_cache_path_from_signature(build_thumb_signature(rel_path, st), mode)


def get_metadata_cache_key(path: Path, st=None):
    if st is None:
        st = path.stat()
    return (str(path), getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)), st.st_size)


def get_cached_parsed_metadata(path: Path):
    st = path.stat()
    cache_key = get_metadata_cache_key(path, st)
    with metadata_cache_lock:
        cached = metadata_cache.get(cache_key)
        if cached is not None:
            metadata_cache.move_to_end(cache_key)
            return cached, st

    parsed = parser.parse_png(path, stat_result=st)
    with metadata_cache_lock:
        metadata_cache[cache_key] = parsed
        metadata_cache.move_to_end(cache_key)
        while len(metadata_cache) > METADATA_CACHE_MAX_ITEMS:
            metadata_cache.popitem(last=False)
    return parsed, st


def build_metadata_response(path: Path, parsed: dict, st=None):
    summary = parsed.get("summary") or {}
    rel_path = rel_from_root(path)
    if st is None:
        st = path.stat()
    ai_payload = load_current_ai_payload(rel_path, st)
    image_edit_payload = build_image_edit_payload(rel_path, st)
    parent_rel = rel_from_root(path.parent) if path.parent != BROWSE_ROOT else ""
    can_set_folder_cover = bool(parent_rel) and parent_rel not in {FAVORITES_DIR_KEY, EDITS_DIR_KEY}
    manual_cover = get_manual_folder_cover(path.parent) if can_set_folder_cover else None
    return {
        "file_name": path.name,
        "rel_path": rel_path,
        "parent_rel": parent_rel,
        "image_url": url_for("image_preview", file=rel_path),
        "original_image_url": url_for("image_preview", file=rel_path, variant="original"),
        "edited_image_url": url_for("image_preview", file=rel_path, variant="edited"),
        "thumb_url": url_for("image_thumbnail", file=rel_path, mode="square", v=build_thumb_signature(rel_path, st)),
        "preview_url": url_for("image_thumbnail", file=rel_path, mode="preview", v=build_thumb_signature(rel_path, st)),
        "original_preview_url": url_for("image_thumbnail", file=rel_path, mode="preview", variant="original", v=f"{int(st.st_mtime)}:{st.st_size}:original"),
        "edited_preview_url": url_for("image_thumbnail", file=rel_path, mode="preview", variant="edited", v=build_thumb_signature(rel_path, st)),
        "download_url": url_for("download_original", file=rel_path),
        "is_favorite": is_favorited(rel_path, st),
        "can_set_folder_cover": can_set_folder_cover,
        "is_folder_cover": bool(manual_cover and manual_cover.get("rel_path") == rel_path),
        "width": parsed["image"]["width"],
        "height": parsed["image"]["height"],
        "size_bytes": parsed["image"]["size_bytes"],
        "size_formatted": format_bytes(parsed["image"]["size_bytes"]),
        "prompt_blocks": extract_prompt_blocks(parsed),
        "has_prompt_json": bool(parsed.get("prompt")),
        "has_workflow_json": bool(parsed.get("workflow")),
        "has_png_info": bool(parsed.get("png_info")),
        "has_summary_json": bool(summary),
        "summary": summary,
        "active_loras": extract_active_loras(summary),
        "manual_override_rows": build_manual_override_rows(summary),
        "quad_rows": build_quad_rows(summary),
        "ai_analysis": ai_payload,
        "ai_analysis_enabled": AI_ANALYSIS_ENABLED,
        "image_edit": image_edit_payload,
    }


def build_index_metadata_fields(parsed: dict):
    summary = parsed.get("summary") or {}
    return {
        "prompt_blocks": extract_prompt_blocks(parsed),
        "summary": summary,
        "active_loras": extract_active_loras(summary),
        "manual_override_rows": build_manual_override_rows(summary),
        "quad_rows": build_quad_rows(summary),
    }


def build_raw_metadata_response(parsed: dict):
    summary = parsed.get("summary") or {}
    return {
        "raw_png_info_json": json.dumps(parsed.get("png_info"), indent=2) if parsed.get("png_info") else None,
        "raw_prompt_json": json.dumps(parsed.get("prompt"), indent=2) if parsed.get("prompt") else None,
        "raw_workflow_json": json.dumps(parsed.get("workflow"), indent=2) if parsed.get("workflow") else None,
        "summary_json": json.dumps(summary, indent=2) if summary else None,
    }


def build_ai_analysis_response(record: dict):
    if not isinstance(record, dict):
        return None

    def split_multiline(value):
        if not isinstance(value, str):
            return []
        return [line.strip() for line in value.splitlines() if line.strip()]

    raw_json = record.get("ai_raw_json")
    parsed_raw_json = None
    if isinstance(raw_json, str) and raw_json.strip():
        try:
            parsed_raw_json = json.loads(raw_json)
        except Exception:
            parsed_raw_json = None
    return {
        "summary": record.get("ai_summary"),
        "tags": split_multiline(record.get("ai_tags_text")),
        "model": record.get("ai_model"),
        "detail_level": record.get("ai_detail_level"),
        "prompt_version": record.get("ai_prompt_version"),
        "analyzed_at": record.get("ai_analyzed_at"),
        "raw_json": parsed_raw_json,
    }


def build_existing_ai_record(record: Optional[dict]) -> Optional[dict]:
    if not isinstance(record, dict):
        return None
    if not record.get("ai_analyzed_at"):
        return None
    return {
        "analyzed_at": record.get("ai_analyzed_at"),
        "model": record.get("ai_model"),
        "detail_level": record.get("ai_detail_level"),
        "prompt_version": record.get("ai_prompt_version"),
        "summary": record.get("ai_summary"),
        "tags_text": record.get("ai_tags_text"),
        "raw_json": record.get("ai_raw_json"),
    }


def load_current_ai_payload(rel_path: str, st) -> Optional[dict]:
    if not METADATA_DB_PATH.exists() or not METADATA_DB_PATH.is_file():
        return None

    try:
        conn = metadata_index.connect(METADATA_DB_PATH)
        try:
            metadata_index.initialize(conn)
            record = metadata_index.get_file_record(conn, rel_path)
        finally:
            conn.close()
    except Exception:
        return None

    if not record:
        return None

    current_mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
    if int(record.get("mtime_ns") or 0) != current_mtime_ns or int(record.get("size_bytes") or 0) != int(st.st_size):
        return None

    if not record.get("ai_analyzed_at"):
        return None

    return build_ai_analysis_response(record)


def ai_analysis_is_configured():
    return AI_ANALYSIS_ENABLED and bool((os.getenv("OPENAI_API_KEY") or "").strip())


def ai_analysis_is_current(existing: Optional[dict], stat_result) -> bool:
    if not isinstance(existing, dict):
        return False
    if not existing.get("ai_analyzed_at"):
        return False
    mtime_ns = int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000)))
    if int(existing.get("mtime_ns") or 0) != mtime_ns or int(existing.get("size_bytes") or 0) != int(stat_result.st_size):
        return False
    return (
        str(existing.get("ai_model") or "") == AI_ANALYSIS_MODEL
        and str(existing.get("ai_detail_level") or "") == AI_ANALYSIS_DETAIL
        and str(existing.get("ai_prompt_version") or "") == AI_ANALYSIS_PROMPT_VERSION
    )


def build_ai_source_image(path: Path, rel_path: str, stat_result):
    cache_path = build_thumb_cache_path(rel_path, stat_result, "full")
    if not cache_path.exists():
        cache_path = generate_thumbnail(path, rel_path, "full")
    return cache_path


def run_ai_analysis_for_item(item):
    parsed = parser.parse_png(item["path"], stat_result=item["stat"])
    metadata_response = build_index_metadata_fields(parsed)
    ai_payload = ai_analysis.build_ai_analysis_input(parsed, item["rel_path"])
    source_image_path = build_ai_source_image(item["path"], item["rel_path"], item["stat"])
    ai_result = ai_analysis.analyze_image(
        source_image_path,
        ai_payload,
        model=AI_ANALYSIS_MODEL,
        detail_level=AI_ANALYSIS_DETAIL,
        prompt_version=AI_ANALYSIS_PROMPT_VERSION,
    )
    ai_record = ai_analysis.build_ai_record(
        item["rel_path"],
        item["stat"],
        ai_result,
        AI_ANALYSIS_MODEL,
        AI_ANALYSIS_DETAIL,
        AI_ANALYSIS_PROMPT_VERSION,
        utc_now_iso(),
    )
    return metadata_index.build_index_record(
        item["rel_path"],
        parsed,
        metadata_response,
        utc_now_iso(),
        item["stat"],
        ai_record=ai_record,
    )


def run_ai_analysis_for_path(path: Path):
    st = path.stat()
    rel_path = rel_from_root(path)
    item = {
        "path": path,
        "rel_path": rel_path,
        "stat": st,
    }
    return run_ai_analysis_for_item(item)


def get_directory_thumbnail_progress_key(dir_path: Path, mode: str) -> str:
    dir_rel = rel_from_root(dir_path) if dir_path != BROWSE_ROOT else "root"
    return f"{dir_rel}:{get_thumbnail_mode_name(mode)}"


def maybe_update_directory_thumbnail_ready(
    progress_key: str,
    dir_rel: str,
    mode_name: str,
    total_count: int,
    cached_count: int,
    *,
    ready_source: str = "scan",
):
    with thumb_ready_state_lock:
        state = load_thumb_ready_state()
        last_ready_count = int(state.get(progress_key, 0) or 0)

        if total_count == 0:
            if last_ready_count:
                state[progress_key] = 0
                save_thumb_ready_state(state)
            return

        if cached_count != total_count:
            if last_ready_count > total_count:
                state[progress_key] = total_count
                save_thumb_ready_state(state)
            return

        if total_count == last_ready_count:
            return

        if last_ready_count <= 0:
            if ready_source == "generation":
                message = "Missing thumbnails generated successfully"
            else:
                message = "All thumbnails already present in cache"
            log_event(
                "thumbnail_directory_ready",
                message,
                directory=dir_rel,
                thumbnail_mode=mode_name,
                image_count=total_count,
                cached_images=cached_count,
                missing_images=max(0, total_count - cached_count),
                ready_source=ready_source,
            )
        else:
            log_event(
                "thumbnail_directory_ready",
                "Additional thumbnails created successfully",
                directory=dir_rel,
                thumbnail_mode=mode_name,
                image_count=total_count,
                additional_images=total_count - last_ready_count,
                cached_images=cached_count,
                missing_images=max(0, total_count - cached_count),
                ready_source=ready_source,
            )

        state[progress_key] = total_count
        save_thumb_ready_state(state)


def register_directory_thumbnail_progress(dir_path: Path, png_items: list, mode: str):
    if mode not in {"square", "full", "preview"}:
        return

    progress_key = get_directory_thumbnail_progress_key(dir_path, mode)
    dir_rel = rel_from_root(dir_path) if dir_path != BROWSE_ROOT else "root"
    cached_count = 0
    for item in png_items:
        if item["cache_paths"].get(mode, Path()).exists():
            cached_count += 1

    progress = {
        "dir_rel": dir_rel,
        "mode_name": get_thumbnail_mode_name(mode),
        "total_count": len(png_items),
        "cached_count": cached_count,
        "known_files": {item["rel_path"] for item in png_items},
    }
    with directory_thumbnail_progress_lock:
        directory_thumbnail_progress[progress_key] = progress

    maybe_update_directory_thumbnail_ready(
        progress_key,
        dir_rel,
        progress["mode_name"],
        progress["total_count"],
        cached_count,
        ready_source="scan",
    )


def mark_thumbnail_generated(dir_path: Path, rel_path: str, mode: str):
    progress_key = get_directory_thumbnail_progress_key(dir_path, mode)
    with directory_thumbnail_progress_lock:
        progress = directory_thumbnail_progress.get(progress_key)
        if not progress:
            return
        if rel_path not in progress["known_files"]:
            return
        if progress["cached_count"] < progress["total_count"]:
            progress["cached_count"] += 1
        cached_count = progress["cached_count"]
        total_count = progress["total_count"]
        dir_rel = progress["dir_rel"]
        mode_name = progress["mode_name"]

    maybe_update_directory_thumbnail_ready(
        progress_key,
        dir_rel,
        mode_name,
        total_count,
        cached_count,
        ready_source="generation",
    )


def generate_thumbnail(source_path: Path, rel_path: str, mode: str, variant: Optional[str] = None) -> Path:
    if mode not in {"square", "full", "preview"}:
        raise ValueError("Unsupported thumbnail mode")

    st = source_path.stat()
    variant_key = str(variant or "").strip().lower()
    if variant_key == "original":
        key = f"{rel_path}:{int(st.st_mtime)}:{st.st_size}:original:{mode}"
        digest = sha256(key.encode("utf-8")).hexdigest()
        cache_path = THUMB_CACHE_DIR / digest[:2] / f"{digest}.webp"
    else:
        cache_path = build_thumb_cache_path(rel_path, st, mode)
    if cache_path.exists():
        return cache_path

    ensure_thumb_cache_dir()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    effective_path = get_effective_image_path_for_variant(rel_path, source_path, st=st, variant=variant_key)

    try:
        with Image.open(effective_path) as img:
            image = ImageOps.exif_transpose(img)
            has_alpha = "A" in image.getbands()
            target_mode = "RGBA" if has_alpha else "RGB"
            if image.mode != target_mode:
                image = image.convert(target_mode)
            else:
                image = image.copy()

            if mode == "square":
                thumb = ImageOps.fit(
                    image,
                    (THUMB_SIZE_SQUARE, THUMB_SIZE_SQUARE),
                    method=Image.Resampling.LANCZOS,
                    centering=(0.5, 0.5),
                )
            elif mode == "full":
                thumb = image.copy()
                thumb.thumbnail((THUMB_SIZE_FULL, THUMB_SIZE_FULL), Image.Resampling.LANCZOS)
            else:
                thumb = image.copy()
                thumb.thumbnail((THUMB_SIZE_PREVIEW, THUMB_SIZE_PREVIEW), Image.Resampling.LANCZOS)

            thumb.save(cache_path, format="WEBP", quality=76, method=4)
            if variant_key != "original" and mode == "full":
                width, height = thumb.size
                if width > 0 and height > 0:
                    with thumb_dimensions_db_lock:
                        conn = connect_thumb_dimensions_db()
                        try:
                            save_cached_thumb_dimensions(
                                conn,
                                build_thumb_signature(rel_path, st),
                                width,
                                height,
                            )
                            conn.commit()
                        finally:
                            conn.close()
        mark_thumbnail_generated(source_path.parent, rel_path, mode)
    except Exception as exc:
        log_event("thumbnail_generation_failure", "Thumbnail generation failed", file=rel_path, mode=mode, error=str(exc))
        raise

    return cache_path


def normalize_selection_rect(payload, image_width: int, image_height: int) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Missing selection rectangle")
    try:
        left = float(payload.get("left"))
        top = float(payload.get("top"))
        width = float(payload.get("width"))
        height = float(payload.get("height"))
    except (TypeError, ValueError):
        raise ValueError("Selection rectangle must contain numeric left, top, width, and height")

    if image_width < 1 or image_height < 1:
        raise ValueError("Image dimensions are invalid")

    left = max(0.0, min(left, float(image_width - 1)))
    top = max(0.0, min(top, float(image_height - 1)))
    right = max(left + 1.0, min(left + width, float(image_width)))
    bottom = max(top + 1.0, min(top + height, float(image_height)))
    if right <= left or bottom <= top:
        raise ValueError("Selection rectangle is empty")
    return {
        "left": int(round(left)),
        "top": int(round(top)),
        "right": int(round(right)),
        "bottom": int(round(bottom)),
        "width": int(round(right - left)),
        "height": int(round(bottom - top)),
    }


def build_patch_rect(selection: dict, image_width: int, image_height: int) -> dict:
    longest_side = max(selection["width"], selection["height"])
    if longest_side <= 72:
        pad = int(round(longest_side * 0.25))
        pad = max(18, min(40, pad))
    elif longest_side <= 160:
        pad = int(round(longest_side * 0.32))
        pad = max(28, min(72, pad))
    else:
        pad = int(round(longest_side * 0.4))
        pad = max(48, min(256, pad))
    left = max(0, selection["left"] - pad)
    top = max(0, selection["top"] - pad)
    right = min(image_width, selection["right"] + pad)
    bottom = min(image_height, selection["bottom"] + pad)
    return {
        "left": left,
        "top": top,
        "right": right,
        "bottom": bottom,
        "width": right - left,
        "height": bottom - top,
    }


def build_patch_mask(patch_size: tuple[int, int], selection: dict, patch_rect: dict) -> Image.Image:
    patch_w, patch_h = patch_size
    mask = Image.new("RGBA", (patch_w, patch_h), (255, 255, 255, 255))
    draw = ImageDraw.Draw(mask)
    draw.rectangle(
        (
            selection["left"] - patch_rect["left"],
            selection["top"] - patch_rect["top"],
            selection["right"] - patch_rect["left"],
            selection["bottom"] - patch_rect["top"],
        ),
        fill=(255, 255, 255, 0),
    )
    return mask


def build_inpaint_mask_from_strokes(patch_size: tuple[int, int], selection: dict, patch_rect: dict, stroke_mask: Image.Image) -> Image.Image:
    patch_w, patch_h = patch_size
    mask = Image.new("L", (patch_w, patch_h), 0)
    offset = (
        max(0, selection["left"] - patch_rect["left"]),
        max(0, selection["top"] - patch_rect["top"]),
    )
    stroke_alpha = stroke_mask.convert("RGBA").getchannel("A")
    mask.paste(stroke_alpha, offset)
    return mask


def expand_mask_for_colored_artifacts(source_patch: Image.Image, mask_binary: np.ndarray) -> np.ndarray:
    if mask_binary.size == 0 or not np.any(mask_binary):
        return mask_binary

    patch_rgb = np.array(source_patch.convert("RGB"), dtype=np.uint8)
    patch_lab = cv2.cvtColor(patch_rgb, cv2.COLOR_RGB2LAB).astype(np.float32)
    patch_rgb_f = patch_rgb.astype(np.float32)

    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    search_region = cv2.dilate(mask_binary, kernel, iterations=10)
    ring_outer = cv2.dilate(mask_binary, kernel, iterations=14)
    ring_inner = cv2.dilate(mask_binary, kernel, iterations=3)
    ring_mask = np.logical_and(ring_outer > 0, ring_inner == 0)
    if not np.any(ring_mask):
        return mask_binary

    bg_lab = patch_lab[ring_mask]
    bg_rgb = patch_rgb_f[ring_mask]
    bg_l = bg_lab[:, 0]
    bg_a = bg_lab[:, 1]
    bg_b = bg_lab[:, 2]
    bg_red_excess = bg_rgb[:, 0] - np.maximum(bg_rgb[:, 1], bg_rgb[:, 2])

    mean_a = float(np.mean(bg_a))
    mean_b = float(np.mean(bg_b))
    std_a = max(4.0, float(np.std(bg_a)))
    std_b = max(4.0, float(np.std(bg_b)))
    mean_l = float(np.mean(bg_l))
    std_l = max(6.0, float(np.std(bg_l)))
    mean_red_excess = float(np.mean(bg_red_excess))
    std_red_excess = max(5.0, float(np.std(bg_red_excess)))

    delta_a = np.abs(patch_lab[:, :, 1] - mean_a)
    delta_b = np.abs(patch_lab[:, :, 2] - mean_b)
    red_excess = patch_rgb_f[:, :, 0] - np.maximum(patch_rgb_f[:, :, 1], patch_rgb_f[:, :, 2])
    luma = patch_lab[:, :, 0]

    chroma_outlier = np.logical_or(delta_a > (2.4 * std_a), delta_b > (2.4 * std_b))
    red_outlier = red_excess > max(10.0, mean_red_excess + (2.0 * std_red_excess))
    luma_outlier = luma > (mean_l + (1.4 * std_l))

    candidate_mask = np.logical_and(search_region > 0, mask_binary == 0)
    color_candidates = np.logical_and(candidate_mask, np.logical_or(red_outlier, np.logical_and(chroma_outlier, luma_outlier)))
    if not np.any(color_candidates):
        return mask_binary

    expanded_mask = mask_binary.copy()
    expanded_mask[color_candidates] = 255
    expanded_mask = cv2.morphologyEx(expanded_mask, cv2.MORPH_CLOSE, kernel, iterations=1)
    expanded_mask = cv2.dilate(expanded_mask, kernel, iterations=1)
    return expanded_mask


def refine_painted_mask_to_artifact(source_patch: Image.Image, painted_mask: np.ndarray) -> np.ndarray:
    if painted_mask.size == 0 or not np.any(painted_mask):
        return painted_mask

    patch_rgb = np.array(source_patch.convert("RGB"), dtype=np.uint8)
    patch_lab = cv2.cvtColor(patch_rgb, cv2.COLOR_RGB2LAB).astype(np.float32)
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))

    search_mask = painted_mask > 0
    ring_outer = cv2.dilate(painted_mask, kernel, iterations=16)
    ring_inner = cv2.dilate(painted_mask, kernel, iterations=4)
    ring_mask = np.logical_and(ring_outer > 0, ring_inner == 0)
    if not np.any(ring_mask):
        return painted_mask

    bg_lab = patch_lab[ring_mask]
    bg_mean = bg_lab.mean(axis=0)
    bg_std = np.maximum(bg_lab.std(axis=0), np.array([6.0, 4.0, 4.0], dtype=np.float32))

    delta_l = np.abs(patch_lab[:, :, 0] - bg_mean[0])
    delta_a = patch_lab[:, :, 1] - bg_mean[1]
    delta_b = patch_lab[:, :, 2] - bg_mean[2]
    chroma_delta = np.sqrt((delta_a * delta_a) + (delta_b * delta_b))

    candidate_mask = np.logical_and(
        search_mask,
        np.logical_or(
            chroma_delta > max(10.0, float(np.hypot(bg_std[1], bg_std[2]) * 2.2)),
            np.logical_and(
                chroma_delta > max(7.0, float(np.hypot(bg_std[1], bg_std[2]) * 1.6)),
                delta_l > max(8.0, float(bg_std[0] * 1.35)),
            ),
        ),
    )

    if not np.any(candidate_mask):
        return painted_mask

    refined_mask = np.where(candidate_mask, 255, 0).astype(np.uint8)
    refined_mask = cv2.morphologyEx(refined_mask, cv2.MORPH_CLOSE, cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5)), iterations=2)
    refined_mask = cv2.dilate(refined_mask, kernel, iterations=2)

    painted_area = max(1, int(np.count_nonzero(painted_mask)))
    refined_area = int(np.count_nonzero(refined_mask))
    if refined_area < max(24, int(painted_area * 0.02)):
        return painted_mask
    if refined_area > int(painted_area * 0.8):
        return painted_mask
    return refined_mask


def regularize_inpaint_mask(mask_binary: np.ndarray) -> np.ndarray:
    if mask_binary.size == 0 or not np.any(mask_binary):
        return mask_binary

    kernel_small = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    kernel_medium = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
    ys, xs = np.nonzero(mask_binary)
    longest_side = int(max(xs.max() - xs.min() + 1, ys.max() - ys.min() + 1))
    bridge_size = 7 if longest_side <= 96 else min(11, max(7, int(round(longest_side * 0.08)) | 1))
    kernel_bridge = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (bridge_size, bridge_size))
    regularized = cv2.morphologyEx(mask_binary, cv2.MORPH_CLOSE, kernel_bridge, iterations=1)
    regularized = cv2.morphologyEx(regularized, cv2.MORPH_CLOSE, kernel_medium, iterations=2)
    regularized = cv2.dilate(regularized, kernel_small, iterations=1)
    regularized = cv2.GaussianBlur(regularized, (0, 0), sigmaX=1.8, sigmaY=1.8)
    regularized = np.where(regularized >= 88, 255, 0).astype(np.uint8)
    regularized = cv2.morphologyEx(regularized, cv2.MORPH_CLOSE, kernel_medium, iterations=1)
    return regularized


def build_binary_inpaint_mask(stroke_mask: Image.Image, source_patch: Optional[Image.Image] = None) -> np.ndarray:
    mask_array = np.array(stroke_mask, dtype=np.uint8)
    mask_binary = np.where(mask_array >= 8, 255, 0).astype(np.uint8)
    if not np.any(mask_binary):
        return mask_binary
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    mask_binary = cv2.morphologyEx(mask_binary, cv2.MORPH_CLOSE, kernel, iterations=1)
    if source_patch is not None:
        mask_binary = refine_painted_mask_to_artifact(source_patch, mask_binary)
        mask_binary = expand_mask_for_colored_artifacts(source_patch, mask_binary)
    mask_binary = regularize_inpaint_mask(mask_binary)
    ys, xs = np.nonzero(mask_binary)
    longest_side = int(max(xs.max() - xs.min() + 1, ys.max() - ys.min() + 1))
    expand_iterations = 1 if longest_side <= 96 else 2
    mask_binary = cv2.dilate(mask_binary, kernel, iterations=expand_iterations)
    return mask_binary


def build_patch_blend_mask(patch_size: tuple[int, int], inpaint_mask: Image.Image, source_patch: Optional[Image.Image] = None) -> Image.Image:
    patch_w, patch_h = patch_size
    mask_binary = build_binary_inpaint_mask(inpaint_mask, source_patch=source_patch)
    if mask_binary.size == 0 or not np.any(mask_binary):
        return Image.new("L", (patch_w, patch_h), 0)

    ys, xs = np.nonzero(mask_binary)
    mask_w = int(xs.max() - xs.min() + 1)
    mask_h = int(ys.max() - ys.min() + 1)
    longest_side = max(mask_w, mask_h)
    grow_px = 2 if longest_side <= 96 else min(6, max(3, int(round(longest_side * 0.035))))
    feather_px = 2 if longest_side <= 64 else min(8, max(3, int(round(longest_side * 0.05))))

    kernel_size = max(3, (grow_px * 2) + 1)
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (kernel_size, kernel_size))
    grown_mask = cv2.dilate(mask_binary, kernel, iterations=1)
    mask = Image.fromarray(grown_mask, mode="L")
    if feather_px > 0:
        mask = mask.filter(ImageFilter.GaussianBlur(feather_px))
    return mask


def apply_selection_tone_adjustment(edited_patch: Image.Image, source_patch: Image.Image, inpaint_mask: Image.Image, blend_mask: Image.Image) -> Image.Image:
    if blend_mask is None:
        return edited_patch
    blend_box = blend_mask.getbbox()
    if not blend_box:
        return edited_patch

    source_rgba = np.array(source_patch.convert("RGBA"), dtype=np.float32)
    edited_region = np.array(edited_patch.crop(blend_box).convert("RGBA"), dtype=np.float32)
    blend_weights = np.array(blend_mask, dtype=np.float32) / 255.0
    mask_binary = build_binary_inpaint_mask(inpaint_mask, source_patch=source_patch)
    if mask_binary.size == 0 or not np.any(mask_binary):
        return edited_patch

    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    ring_outer = cv2.dilate(mask_binary, kernel, iterations=10)
    ring_inner = cv2.dilate(mask_binary, kernel, iterations=2)
    ring_mask = np.logical_and(ring_outer > 0, ring_inner == 0)
    if not np.any(ring_mask):
        ring_mask = blend_weights > 0

    source_rgb = source_rgba[:, :, :3]
    reference_weights = ring_mask.astype(np.float32)
    total_reference_weight = float(reference_weights.sum())
    if total_reference_weight <= 0.0:
        return edited_patch

    target_rgb = (source_rgb * reference_weights[:, :, None]).sum(axis=(0, 1)) / total_reference_weight
    edited_weights = np.array(blend_mask.crop(blend_box), dtype=np.float32) / 255.0
    alpha_weights = edited_weights * (edited_region[:, :, 3] / 255.0)
    total_weight = float(alpha_weights.sum())
    if total_weight <= 0.0:
        return edited_patch

    edt_rgb = (edited_region[:, :, :3] * alpha_weights[:, :, None]).sum(axis=(0, 1)) / total_weight
    deltas = np.clip(target_rgb - edt_rgb, -16.0, 16.0)
    if not np.any(np.abs(deltas) >= 0.5):
        return edited_patch

    adjusted = edited_region.copy()
    adjusted[:, :, :3] = np.clip(
        adjusted[:, :, :3] + (deltas[None, None, :] * alpha_weights[:, :, None]),
        0,
        255,
    )
    adjusted_region = Image.fromarray(adjusted.astype(np.uint8), mode="RGBA")
    output = edited_patch.copy()
    output.paste(adjusted_region, blend_box)
    return output


def apply_context_texture_match(edited_patch: Image.Image, source_patch: Image.Image, inpaint_mask: Image.Image, blend_mask: Image.Image) -> Image.Image:
    if blend_mask is None:
        return edited_patch
    blend_box = blend_mask.getbbox()
    if not blend_box:
        return edited_patch

    source_rgb = np.array(source_patch.convert("RGB"), dtype=np.float32)
    edited_rgba = np.array(edited_patch.convert("RGBA"), dtype=np.float32)
    blend_weights = np.array(blend_mask, dtype=np.float32) / 255.0
    mask_binary = build_binary_inpaint_mask(inpaint_mask, source_patch=source_patch)
    if mask_binary.size == 0 or not np.any(mask_binary):
        return edited_patch

    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    ring_outer = cv2.dilate(mask_binary, kernel, iterations=10)
    ring_inner = cv2.dilate(mask_binary, kernel, iterations=2)
    ring_mask = np.logical_and(ring_outer > 0, ring_inner == 0)
    if not np.any(ring_mask):
        return edited_patch

    source_blur = cv2.GaussianBlur(source_rgb, (0, 0), sigmaX=1.0, sigmaY=1.0)
    source_residual = source_rgb - source_blur
    source_luma_residual = (
        (source_residual[:, :, 0] * 0.299)
        + (source_residual[:, :, 1] * 0.587)
        + (source_residual[:, :, 2] * 0.114)
    )
    texture_std = float(np.std(source_luma_residual[ring_mask]))
    if not np.isfinite(texture_std) or texture_std < 1.1:
        return edited_patch

    texture_std = min(texture_std, 4.5)
    rng = np.random.default_rng()
    grain = rng.normal(0.0, texture_std, size=source_luma_residual.shape).astype(np.float32)
    edited_luma = (
        (edited_rgba[:, :, 0] * 0.299)
        + (edited_rgba[:, :, 1] * 0.587)
        + (edited_rgba[:, :, 2] * 0.114)
    ) / 255.0
    strength = blend_weights * np.clip(0.24 + (edited_luma * 0.16), 0.18, 0.38)
    grain_rgb = grain[:, :, None] * strength[:, :, None]
    edited_rgba[:, :, :3] = np.clip(edited_rgba[:, :, :3] + grain_rgb, 0, 255)
    return Image.fromarray(edited_rgba.astype(np.uint8), mode="RGBA")


def decode_png_data_url(data_url: str) -> bytes:
    raw = str(data_url or "").strip()
    if not raw:
        raise ValueError("Missing repair mask data")
    prefix = "data:image/png;base64,"
    if not raw.startswith(prefix):
        raise ValueError("Repair mask must be a PNG data URL")
    try:
        return base64.b64decode(raw[len(prefix):], validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("Repair mask data was invalid")


def get_local_inpaint_flag() -> int:
    if LOCAL_REPAIR_METHOD == "ns":
        return cv2.INPAINT_NS
    return cv2.INPAINT_TELEA


def apply_repair_to_image(rel_path: str, source_path: Path, st, selection_payload: dict, mask_strokes_bytes: Optional[bytes] = None) -> dict:
    effective_path = get_effective_image_path(rel_path, source_path, st=st)
    with Image.open(effective_path) as img:
        image = ImageOps.exif_transpose(img).convert("RGBA")

    selection = normalize_selection_rect(selection_payload, image.width, image.height)
    patch_rect = build_patch_rect(selection, image.width, image.height)
    patch = image.crop((patch_rect["left"], patch_rect["top"], patch_rect["right"], patch_rect["bottom"]))
    if not mask_strokes_bytes:
        raise ValueError("Missing repair mask strokes")
    with Image.open(io.BytesIO(mask_strokes_bytes)) as stroke_img:
        stroke_mask = ImageOps.exif_transpose(stroke_img).convert("RGBA")
    expected_size = (max(1, selection["width"]), max(1, selection["height"]))
    if stroke_mask.size != expected_size:
        stroke_mask = stroke_mask.resize(expected_size, Image.Resampling.LANCZOS)
    inpaint_mask = build_inpaint_mask_from_strokes((patch.width, patch.height), selection, patch_rect, stroke_mask)
    if inpaint_mask.getbbox() is None:
        raise ValueError("Paint over the unwanted artifact before sending repair")

    patch_rgb = np.array(patch.convert("RGB"), dtype=np.uint8)
    patch_bgr = cv2.cvtColor(patch_rgb, cv2.COLOR_RGB2BGR)
    mask_binary = build_binary_inpaint_mask(inpaint_mask, source_patch=patch)
    mask_area = max(1, int(np.count_nonzero(mask_binary)))
    effective_radius = max(float(LOCAL_REPAIR_RADIUS), min(4.0, 1.5 + (mask_area ** 0.5 * 0.04)))
    inpainted_bgr = cv2.inpaint(patch_bgr, mask_binary, effective_radius, get_local_inpaint_flag())
    edited_patch_rgb = cv2.cvtColor(inpainted_bgr, cv2.COLOR_BGR2RGB)
    raw_edited_patch = Image.fromarray(edited_patch_rgb, mode="RGB").convert("RGBA")
    blend_mask = build_patch_blend_mask((patch.width, patch.height), inpaint_mask, source_patch=patch)
    edited_patch = apply_selection_tone_adjustment(raw_edited_patch, patch, inpaint_mask, blend_mask)
    edited_patch = apply_context_texture_match(edited_patch, patch, inpaint_mask, blend_mask)
    blended_patch = Image.composite(edited_patch, patch, blend_mask)
    output = image.copy()
    output.paste(blended_patch, (patch_rect["left"], patch_rect["top"]))

    return save_image_edit_variant(rel_path, st, output, {
        "type": "repair_region",
        "selection": {
            "left": selection["left"],
            "top": selection["top"],
            "width": selection["width"],
            "height": selection["height"],
        },
        "patch_rect": patch_rect,
        "repair_backend": "opencv_inpaint",
        "repair_method": LOCAL_REPAIR_METHOD if LOCAL_REPAIR_METHOD == "ns" else "telea",
        "repair_radius": LOCAL_REPAIR_RADIUS,
    })


def read_recent_logs(limit: int):
    if limit < 1:
        return []
    if not APP_LOG_PATH.exists() or not APP_LOG_PATH.is_file():
        return []

    lines = deque(maxlen=limit)
    with APP_LOG_PATH.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            text = line.strip()
            if text:
                lines.append(text)

    entries = []
    for line in lines:
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            entries.append({"ts": None, "event": "log_line", "message": line})
    return entries


def clear_thumbnail_cache(log_event_enabled: bool = True):
    if not THUMB_CACHE_DIR.exists():
        ensure_thumb_cache_dir()
        if THUMB_READY_STATE_PATH.exists():
            THUMB_READY_STATE_PATH.unlink()
        return 0

    file_count = 0
    for path in THUMB_CACHE_DIR.rglob("*"):
        if path.is_file():
            file_count += 1

    ensure_thumb_cache_dir()
    for path in list(THUMB_CACHE_DIR.iterdir()):
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    if THUMB_READY_STATE_PATH.exists():
        THUMB_READY_STATE_PATH.unlink()
    with directory_thumbnail_progress_lock:
        directory_thumbnail_progress.clear()
    if log_event_enabled:
        log_event("thumbnail_cache_cleared", "Thumbnail cache cleared", deleted_files=file_count)
    return file_count


def format_scope_label(scope_path: Path) -> str:
    return "root" if scope_path == BROWSE_ROOT else rel_from_root(scope_path)


def scan_png_work(scope_path: Optional[Path] = None):
    ensure_root_exists()
    target_root = (scope_path or BROWSE_ROOT).resolve()
    work_items = []
    folders_scanned = 0
    folders_with_pngs = 0
    image_count = 0

    if target_root == BROWSE_ROOT:
        candidate_dirs = []
        with os.scandir(target_root) as scan:
            for entry in scan:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                if should_skip_dir_name(entry.name):
                    continue
                candidate_dirs.append(Path(entry.path))
    else:
        candidate_dirs = [target_root]

    for dir_path in candidate_dirs:
        folders_scanned += 1
        png_entries = []
        try:
            with os.scandir(dir_path) as scan:
                for entry in scan:
                    name = entry.name
                    if should_skip_hidden_name(name):
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    if not name.lower().endswith(".png") or should_exclude_png_path(Path(entry.path)):
                        continue
                    try:
                        st = entry.stat()
                    except OSError:
                        continue
                    path = Path(entry.path)
                    png_entries.append(
                        {
                            "path": path,
                            "rel_path": rel_from_root(path),
                            "stat": st,
                        }
                    )
        except OSError:
            continue

        if not png_entries:
            continue

        folders_with_pngs += 1
        image_count += len(png_entries)
        work_items.append((dir_path, png_entries))

    return work_items, folders_scanned, folders_with_pngs, image_count


def scan_rebuild_preview_work(scope_path: Optional[Path] = None):
    return scan_png_work(scope_path or BROWSE_ROOT)


def complete_rebuild_cancelled(scope_label, scope_dir, force_rebuild, completed_tasks, total_tasks, mode_totals, failure_count, deleted_files, folders_scanned, folders_with_pngs, image_count):
    summary = {
        "scope_dir": scope_dir,
        "scope_label": scope_label,
        "force_rebuild": bool(force_rebuild),
        "deleted_files": deleted_files,
        "folders_scanned": folders_scanned,
        "folders_with_pngs": folders_with_pngs,
        "image_count": image_count,
        "generated_full": mode_totals["full"],
        "failure_count": failure_count,
        "cancelled": True,
    }
    log_event("thumbnail_prewarm_rebuild_cancelled", "Rebuild previews cancelled", **summary)
    update_rebuild_status(
        state="cancelled",
        phase="cancelled",
        cancel_requested=False,
        completed_tasks=completed_tasks,
        total_tasks=total_tasks,
        current_directory=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_rebuild=bool(force_rebuild),
        finished_at=utc_now_iso(),
        error=None,
        summary=summary,
    )
    return summary


def rebuild_all_previews(scope_rel: str = "", force_rebuild: bool = False):
    ensure_root_exists()
    scope_path = resolve_safe_path(scope_rel)
    scope_dir = rel_from_root(scope_path) if scope_path != BROWSE_ROOT else ""
    scope_label = format_scope_label(scope_path)
    started_at = utc_now_iso()
    update_rebuild_status(
        state="running",
        phase="scanning",
        progress_pct=0,
        completed_tasks=0,
        total_tasks=0,
        folders_scanned=0,
        folders_with_pngs=0,
        image_count=0,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_rebuild=bool(force_rebuild),
        started_at=started_at,
        finished_at=None,
        error=None,
        summary=None,
    )

    work_items, folders_scanned, folders_with_pngs, image_count = scan_rebuild_preview_work(scope_path)
    total_tasks = image_count
    update_rebuild_status(
        phase="clearing-cache" if force_rebuild else "building",
        total_tasks=total_tasks,
        folders_scanned=folders_scanned,
        folders_with_pngs=folders_with_pngs,
        image_count=image_count,
        current_directory=scope_label,
        current_file=None,
    )

    if is_rebuild_cancel_requested(get_rebuild_status_snapshot()):
        return complete_rebuild_cancelled(scope_label, scope_dir, force_rebuild, 0, total_tasks, {"full": 0}, 0, 0, folders_scanned, folders_with_pngs, image_count)

    deleted_files = clear_thumbnail_cache(log_event_enabled=False) if force_rebuild and scope_path == BROWSE_ROOT else 0
    ensure_thumb_cache_dir()

    mode_totals = {"full": 0}
    failure_count = 0
    ready_state = {}
    completed_tasks = 0

    update_rebuild_status(phase="building", current_directory=scope_label)

    if is_rebuild_cancel_requested(get_rebuild_status_snapshot()):
        return complete_rebuild_cancelled(scope_label, scope_dir, force_rebuild, completed_tasks, total_tasks, mode_totals, failure_count, deleted_files, folders_scanned, folders_with_pngs, image_count)

    for dir_path, png_entries in work_items:
        update_rebuild_status(
            current_directory=rel_from_root(dir_path) if dir_path != BROWSE_ROOT else "root",
            current_file=None,
        )
        pending_items = []
        for item in png_entries:
            if force_rebuild:
                cache_path = build_thumb_cache_path(item["rel_path"], item["stat"], "full")
                if cache_path.exists():
                    try:
                        cache_path.unlink()
                    except OSError:
                        pass
            cache_path = build_thumb_cache_path(item["rel_path"], item["stat"], "full")
            if force_rebuild or not cache_path.exists():
                pending_items.append(item)
            else:
                completed_tasks += 1
                update_rebuild_status(
                    completed_tasks=completed_tasks,
                    current_directory=rel_from_root(dir_path) if dir_path != BROWSE_ROOT else "root",
                    current_file=item["rel_path"],
                )

        if pending_items:
            with ThreadPoolExecutor(max_workers=REBUILD_PREVIEW_WORKERS) as executor:
                future_map = {
                    executor.submit(generate_thumbnail, item["path"], item["rel_path"], "full"): item
                    for item in pending_items
                }
                for future in as_completed(future_map):
                    try:
                        future.result()
                        mode_totals["full"] += 1
                    except Exception:
                        failure_count += 1
                    finally:
                        completed_tasks += 1
                        item = future_map[future]
                        update_rebuild_status(
                            completed_tasks=completed_tasks,
                            current_directory=rel_from_root(dir_path) if dir_path != BROWSE_ROOT else "root",
                            current_file=item["rel_path"],
                        )

        ready_state[get_directory_thumbnail_progress_key(dir_path, "full")] = len(png_entries)
        if is_rebuild_cancel_requested(get_rebuild_status_snapshot()):
            with thumb_ready_state_lock:
                save_thumb_ready_state(ready_state)
            with directory_thumbnail_progress_lock:
                directory_thumbnail_progress.clear()
            return complete_rebuild_cancelled(
                scope_label,
                scope_dir,
                force_rebuild,
                completed_tasks,
                total_tasks,
                mode_totals,
                failure_count,
                deleted_files,
                folders_scanned,
                folders_with_pngs,
                image_count,
            )

    with thumb_ready_state_lock:
        save_thumb_ready_state(ready_state)

    with directory_thumbnail_progress_lock:
        directory_thumbnail_progress.clear()

    summary = {
        "scope_dir": scope_dir,
        "scope_label": scope_label,
        "force_rebuild": bool(force_rebuild),
        "deleted_files": deleted_files,
        "folders_scanned": folders_scanned,
        "folders_with_pngs": folders_with_pngs,
        "image_count": image_count,
        "generated_full": mode_totals["full"],
        "failure_count": failure_count,
    }
    log_event("thumbnail_prewarm_rebuilt", "Rebuild previews completed", **summary)
    update_rebuild_status(
        state="completed",
        phase="completed",
        cancel_requested=False,
        completed_tasks=total_tasks,
        total_tasks=total_tasks,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_rebuild=bool(force_rebuild),
        finished_at=utc_now_iso(),
        summary=summary,
    )
    return summary


def rebuild_previews_worker(scope_rel: str, force_rebuild: bool):
    try:
        rebuild_all_previews(scope_rel, force_rebuild=force_rebuild)
    except Exception as exc:
        log_event("thumbnail_prewarm_rebuild_failure", "Rebuild previews failed", error=str(exc))
        update_rebuild_status(
            state="failed",
            phase="failed",
            cancel_requested=False,
            error=str(exc),
            current_directory=None,
            scope_dir=str(scope_rel or ""),
            force_rebuild=bool(force_rebuild),
            finished_at=utc_now_iso(),
        )
    finally:
        rebuild_previews_lock.release()


def parse_metadata_index_record(item, existing_record=None):
    parsed = parser.parse_png(item["path"], stat_result=item["stat"])
    metadata_response = build_index_metadata_fields(parsed)
    ai_record = build_existing_ai_record(existing_record)
    return metadata_index.build_index_record(
        item["rel_path"],
        parsed,
        metadata_response,
        utc_now_iso(),
        item["stat"],
        ai_record=ai_record,
    )


def complete_metadata_index_cancelled(scope_label, scope_dir, completed_tasks, total_tasks, indexed_count, skipped_count, failure_count, deleted_count, folders_scanned, folders_with_pngs, image_count):
    summary = {
        "scope_dir": scope_dir,
        "scope_label": scope_label,
        "folders_scanned": folders_scanned,
        "folders_with_pngs": folders_with_pngs,
        "image_count": image_count,
        "indexed_count": indexed_count,
        "skipped_count": skipped_count,
        "failure_count": failure_count,
        "deleted_count": deleted_count,
        "cancelled": True,
    }
    log_event("metadata_index_rebuild_cancelled", "Metadata index rebuild cancelled", **summary)
    update_metadata_index_status(
        state="cancelled",
        phase="cancelled",
        cancel_requested=False,
        completed_tasks=completed_tasks,
        total_tasks=total_tasks,
        current_directory=None,
        finished_at=utc_now_iso(),
        error=None,
        summary=summary,
    )
    return summary


def rebuild_metadata_index(scope_rel: str = "", force_reindex: bool = False):
    ensure_root_exists()
    scope_path = resolve_safe_path(scope_rel)
    scope_dir = rel_from_root(scope_path) if scope_path != BROWSE_ROOT else ""
    scope_label = format_scope_label(scope_path)

    update_metadata_index_status(
        state="running",
        phase="scanning",
        progress_pct=0,
        completed_tasks=0,
        total_tasks=0,
        folders_scanned=0,
        folders_with_pngs=0,
        image_count=0,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_reindex=bool(force_reindex),
        started_at=utc_now_iso(),
        finished_at=None,
        error=None,
        summary=None,
    )

    work_items, folders_scanned, folders_with_pngs, image_count = scan_png_work(scope_path)
    total_tasks = image_count
    update_metadata_index_status(
        phase="indexing",
        total_tasks=total_tasks,
        folders_scanned=folders_scanned,
        folders_with_pngs=folders_with_pngs,
        image_count=image_count,
        current_directory=scope_label,
        current_file=None,
    )

    conn = metadata_index.connect(METADATA_DB_PATH)
    try:
        metadata_index.initialize(conn)
        existing_files = metadata_index.load_existing_files(conn, scope_dir)
        seen_paths = set()
        indexed_count = 0
        skipped_count = 0
        failure_count = 0
        completed_tasks = 0

        if is_metadata_index_cancel_requested(get_metadata_index_status_snapshot()):
            return complete_metadata_index_cancelled(
                scope_label, scope_dir, completed_tasks, total_tasks, indexed_count, skipped_count,
                failure_count, 0, folders_scanned, folders_with_pngs, image_count,
            )

        for dir_path, png_entries in work_items:
            current_directory = format_scope_label(dir_path)
            update_metadata_index_status(current_directory=current_directory, current_file=None)

            pending_items = []
            for item in png_entries:
                seen_paths.add(item["rel_path"])
                mtime_ns = int(getattr(item["stat"], "st_mtime_ns", int(item["stat"].st_mtime * 1_000_000_000)))
                existing = existing_files.get(item["rel_path"])
                if (not force_reindex) and existing and existing["mtime_ns"] == mtime_ns and existing["size_bytes"] == int(item["stat"].st_size):
                    skipped_count += 1
                    completed_tasks += 1
                    update_metadata_index_status(
                        completed_tasks=completed_tasks,
                        current_directory=current_directory,
                        current_file=item["rel_path"],
                    )
                    continue
                existing_record = metadata_index.get_file_record(conn, item["rel_path"]) if existing else None
                pending_items.append((item, existing_record))

            if pending_items:
                with ThreadPoolExecutor(max_workers=REBUILD_PREVIEW_WORKERS) as executor:
                    future_map = {
                        executor.submit(parse_metadata_index_record, item, existing_record): item
                        for item, existing_record in pending_items
                    }
                    for future in as_completed(future_map):
                        item = future_map[future]
                        try:
                            record = future.result()
                            metadata_index.upsert_file_record(conn, record)
                            indexed_count += 1
                        except Exception as exc:
                            failure_count += 1
                            log_event("metadata_index_file_failure", "Metadata index parse failed", file=item["rel_path"], error=str(exc))
                        finally:
                            completed_tasks += 1
                            update_metadata_index_status(
                                completed_tasks=completed_tasks,
                                current_directory=current_directory,
                                current_file=item["rel_path"],
                            )
                conn.commit()

            if is_metadata_index_cancel_requested(get_metadata_index_status_snapshot()):
                conn.commit()
                return complete_metadata_index_cancelled(
                    scope_label, scope_dir, completed_tasks, total_tasks, indexed_count, skipped_count,
                    failure_count, 0, folders_scanned, folders_with_pngs, image_count,
                )

        update_metadata_index_status(phase="finalizing", current_directory=scope_label)
        stale_paths = sorted(set(existing_files) - seen_paths)
        deleted_count = metadata_index.delete_missing_files(conn, stale_paths)
        conn.commit()

        summary = {
            "scope_dir": scope_dir,
            "scope_label": scope_label,
            "folders_scanned": folders_scanned,
            "folders_with_pngs": folders_with_pngs,
            "image_count": image_count,
            "indexed_count": indexed_count,
            "skipped_count": skipped_count,
            "failure_count": failure_count,
            "deleted_count": deleted_count,
            "db_path": str(METADATA_DB_PATH),
            "force_reindex": bool(force_reindex),
        }
        log_event("metadata_index_rebuilt", "Metadata index rebuild completed", **summary)
        update_metadata_index_status(
            state="completed",
            phase="completed",
            cancel_requested=False,
            completed_tasks=total_tasks,
            total_tasks=total_tasks,
            current_directory=None,
            current_file=None,
            finished_at=utc_now_iso(),
            error=None,
            summary=summary,
        )
        return summary
    finally:
        conn.close()


def rebuild_metadata_index_worker(scope_rel: str, force_reindex: bool):
    try:
        rebuild_metadata_index(scope_rel, force_reindex=force_reindex)
    except Exception as exc:
        log_event("metadata_index_rebuild_failure", "Metadata index rebuild failed", scope_dir=scope_rel or "", force_reindex=bool(force_reindex), error=str(exc))
        update_metadata_index_status(
            state="failed",
            phase="failed",
            cancel_requested=False,
            error=str(exc),
            current_directory=None,
            finished_at=utc_now_iso(),
        )
    finally:
        metadata_index_lock.release()


def complete_ai_analysis_cancelled(scope_label, scope_dir, completed_tasks, total_tasks, analyzed_count, skipped_count, failure_count, folders_scanned, folders_with_pngs, image_count):
    summary = {
        "scope_dir": scope_dir,
        "scope_label": scope_label,
        "folders_scanned": folders_scanned,
        "folders_with_pngs": folders_with_pngs,
        "image_count": image_count,
        "analyzed_count": analyzed_count,
        "skipped_count": skipped_count,
        "failure_count": failure_count,
        "cancelled": True,
    }
    log_event("ai_analysis_rebuild_cancelled", "AI vision pass cancelled", **summary)
    update_ai_analysis_status(
        state="cancelled",
        phase="cancelled",
        cancel_requested=False,
        completed_tasks=completed_tasks,
        total_tasks=total_tasks,
        current_directory=None,
        finished_at=utc_now_iso(),
        error=None,
        summary=summary,
    )
    return summary


def rebuild_ai_analysis(scope_rel: str = "", force_reanalyze: bool = False):
    if not ai_analysis_is_configured():
        raise RuntimeError("AI analysis is not configured. Set AI_ANALYSIS_ENABLED=1 and OPENAI_API_KEY.")

    ensure_root_exists()
    scope_path = resolve_safe_path(scope_rel)
    scope_dir = rel_from_root(scope_path) if scope_path != BROWSE_ROOT else ""
    scope_label = format_scope_label(scope_path)

    update_ai_analysis_status(
        state="running",
        phase="scanning",
        progress_pct=0,
        completed_tasks=0,
        total_tasks=0,
        folders_scanned=0,
        folders_with_pngs=0,
        image_count=0,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_rebuild=bool(force_reanalyze),
        started_at=utc_now_iso(),
        finished_at=None,
        error=None,
        summary=None,
    )

    work_items, folders_scanned, folders_with_pngs, image_count = scan_png_work(scope_path)
    total_tasks = image_count
    update_ai_analysis_status(
        phase="analyzing",
        total_tasks=total_tasks,
        folders_scanned=folders_scanned,
        folders_with_pngs=folders_with_pngs,
        image_count=image_count,
        current_directory=scope_label,
        current_file=None,
    )

    conn = metadata_index.connect(METADATA_DB_PATH)
    try:
        metadata_index.initialize(conn)
        existing_files = metadata_index.load_existing_files(conn, scope_dir)
        analyzed_count = 0
        skipped_count = 0
        failure_count = 0
        completed_tasks = 0

        if is_ai_analysis_cancel_requested(get_ai_analysis_status_snapshot()):
            return complete_ai_analysis_cancelled(
                scope_label, scope_dir, completed_tasks, total_tasks, analyzed_count, skipped_count,
                failure_count, folders_scanned, folders_with_pngs, image_count,
            )

        for dir_path, png_entries in work_items:
            current_directory = format_scope_label(dir_path)
            update_ai_analysis_status(current_directory=current_directory, current_file=None)

            pending_items = []
            for item in png_entries:
                existing = existing_files.get(item["rel_path"])
                if (not force_reanalyze) and ai_analysis_is_current(existing, item["stat"]):
                    skipped_count += 1
                    completed_tasks += 1
                    update_ai_analysis_status(
                        completed_tasks=completed_tasks,
                        current_directory=current_directory,
                        current_file=item["rel_path"],
                    )
                    continue
                pending_items.append(item)

            if pending_items:
                with ThreadPoolExecutor(max_workers=AI_ANALYSIS_MAX_WORKERS) as executor:
                    future_map = {
                        executor.submit(run_ai_analysis_for_item, item): item
                        for item in pending_items
                    }
                    for future in as_completed(future_map):
                        item = future_map[future]
                        try:
                            record = future.result()
                            metadata_index.upsert_file_record(conn, record)
                            analyzed_count += 1
                        except Exception as exc:
                            failure_count += 1
                            log_event("ai_analysis_file_failure", "AI vision pass failed", file=item["rel_path"], error=str(exc))
                        finally:
                            completed_tasks += 1
                            update_ai_analysis_status(
                                completed_tasks=completed_tasks,
                                current_directory=current_directory,
                                current_file=item["rel_path"],
                            )
                conn.commit()

            if is_ai_analysis_cancel_requested(get_ai_analysis_status_snapshot()):
                conn.commit()
                return complete_ai_analysis_cancelled(
                    scope_label, scope_dir, completed_tasks, total_tasks, analyzed_count, skipped_count,
                    failure_count, folders_scanned, folders_with_pngs, image_count,
                )

        update_ai_analysis_status(phase="finalizing", current_directory=scope_label)
        conn.commit()

        summary = {
            "scope_dir": scope_dir,
            "scope_label": scope_label,
            "folders_scanned": folders_scanned,
            "folders_with_pngs": folders_with_pngs,
            "image_count": image_count,
            "analyzed_count": analyzed_count,
            "skipped_count": skipped_count,
            "failure_count": failure_count,
            "db_path": str(METADATA_DB_PATH),
            "force_rebuild": bool(force_reanalyze),
            "thumbnail_mode": "full",
            "thumbnail_size": THUMB_SIZE_FULL,
        }
        log_event("ai_analysis_rebuilt", "AI vision pass completed", **summary)
        update_ai_analysis_status(
            state="completed",
            phase="completed",
            cancel_requested=False,
            completed_tasks=total_tasks,
            total_tasks=total_tasks,
            current_directory=None,
            current_file=None,
            finished_at=utc_now_iso(),
            error=None,
            summary=summary,
        )
        return summary
    finally:
        conn.close()


def rebuild_ai_analysis_worker(scope_rel: str, force_reanalyze: bool):
    try:
        rebuild_ai_analysis(scope_rel, force_reanalyze=force_reanalyze)
    except Exception as exc:
        log_event("ai_analysis_rebuild_failure", "AI vision pass failed", scope_dir=scope_rel or "", force_rebuild=bool(force_reanalyze), error=str(exc))
        update_ai_analysis_status(
            state="failed",
            phase="failed",
            cancel_requested=False,
            error=str(exc),
            current_directory=None,
            current_file=None,
            finished_at=utc_now_iso(),
        )
    finally:
        ai_analysis_lock.release()


initialize_rebuild_status()
initialize_metadata_index_status()
initialize_ai_analysis_status()


def clear_logs():
    ensure_log_dir()
    if not APP_LOG_PATH.exists():
        APP_LOG_PATH.write_text("", encoding="utf-8")
        return 0

    line_count = 0
    with APP_LOG_PATH.open("r", encoding="utf-8", errors="replace") as fh:
        for _line in fh:
            line_count += 1

    APP_LOG_PATH.write_text("", encoding="utf-8")
    return line_count


def find_newest_direct_png_cover(dir_path: Path, dir_stat=None):
    try:
        st_dir = dir_stat if dir_stat is not None else dir_path.stat()
        cache_key = str(dir_path.resolve())
        cache_version = f"{int(st_dir.st_mtime_ns)}:{int(st_dir.st_size)}"
    except OSError:
        return None

    with folder_cover_cache_lock:
        cached = folder_cover_cache.get(cache_key)
        if cached and cached.get("version") == cache_version:
            return cached.get("cover")

    newest = None
    try:
        with os.scandir(dir_path) as scan:
            for entry in scan:
                name = entry.name
                if should_skip_hidden_name(name):
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                if not name.lower().endswith(".png"):
                    continue
                path = Path(entry.path)
                if should_exclude_png_path(path):
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                if newest is None or (st.st_mtime, name.lower()) > (newest["stat"].st_mtime, newest["name"].lower()):
                    newest = {
                        "name": name,
                        "path": path,
                        "stat": st,
                    }
    except OSError:
        return None

    cover = None

    if newest is not None:
        rel_path = rel_from_root(newest["path"])
        thumb_sig = build_thumb_signature(rel_path, newest["stat"])
        cover = {
            "name": newest["name"],
            "rel_path": rel_path,
            "thumb_sig": thumb_sig,
        }

    with folder_cover_cache_lock:
        if len(folder_cover_cache) > 2048:
            folder_cover_cache.clear()
        folder_cover_cache[cache_key] = {
            "version": cache_version,
            "cover": cover,
        }

    return cover


def invalidate_folder_cover_cache(dir_path: Path):
    try:
        cache_key = str(dir_path.resolve())
    except OSError:
        return
    with folder_cover_cache_lock:
        folder_cover_cache.pop(cache_key, None)


def build_folder_cover_payload(rel_path: str):
    try:
        path = resolve_safe_path(rel_path)
    except ValueError:
        return None
    if should_exclude_png_path(path):
        return None
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return None
    try:
        st = path.stat()
    except OSError:
        return None
    return {
        "name": path.name,
        "rel_path": rel_path,
        "thumb_sig": build_thumb_signature(rel_path, st),
    }


def get_manual_folder_cover(dir_path: Path):
    if dir_path == BROWSE_ROOT:
        return None
    try:
        folder_rel = rel_from_root(dir_path)
    except ValueError:
        return None
    with folder_covers_state_lock:
        state = load_folder_covers_state()
        entry = state["entries"].get(folder_rel)
    if not entry:
        return None
    rel_path = str(entry.get("rel_path") or "").strip()
    cover = build_folder_cover_payload(rel_path)
    if not cover:
        return None
    try:
        cover_parent = resolve_safe_path(rel_path).parent.resolve()
        if cover_parent != dir_path.resolve():
            return None
    except OSError:
        return None
    cover["manual"] = True
    return cover


def find_folder_cover(dir_path: Path, dir_stat=None):
    try:
        st_dir = dir_stat if dir_stat is not None else dir_path.stat()
        cache_key = str(dir_path.resolve())
        cache_version = f"{int(st_dir.st_mtime_ns)}:{int(st_dir.st_size)}"
    except OSError:
        return None

    manual_cover = get_manual_folder_cover(dir_path)
    if manual_cover:
        return manual_cover

    with folder_cover_cache_lock:
        cached = folder_cover_cache.get(cache_key)
        if cached and cached.get("version") == cache_version:
            return cached.get("cover")

    cover = find_newest_direct_png_cover(dir_path, dir_stat=st_dir)

    with folder_cover_cache_lock:
        if len(folder_cover_cache) > 2048:
            folder_cover_cache.clear()
        folder_cover_cache[cache_key] = {
            "version": cache_version,
            "cover": cover,
        }
    return cover


def list_directory(
    rel_dir: str,
    sort_key: str = "date",
    sort_dir: str = "desc",
    *,
    include_thumb_dimensions: bool = False,
    include_folder_covers: bool = False,
):
    rel_dir = (rel_dir or "").strip()
    perf = {
        "scan_entries_ms": 0,
        "entry_stat_ms": 0,
        "png_item_build_ms": 0,
        "edit_lookup_ms": 0,
        "dimension_read_ms": 0,
        "folder_cover_lookup_ms": 0,
        "folder_count": 0,
        "png_count": 0,
    }
    if rel_dir == EDITS_DIR_KEY:
        png_items, breadcrumb = get_edits_listing(sort_key, sort_dir)
        perf["png_count"] = len(png_items)
        return BROWSE_ROOT, [], png_items, breadcrumb, "", perf
    if rel_dir == FAVORITES_DIR_KEY:
        png_items, breadcrumb = get_favorites_listing(sort_key, sort_dir)
        perf["png_count"] = len(png_items)
        return BROWSE_ROOT, [], png_items, breadcrumb, "", perf

    current = resolve_safe_path(rel_dir)
    if not current.exists() or not current.is_dir():
        raise FileNotFoundError("Directory not found")

    sort_key = (sort_key or "date").lower()
    sort_dir = (sort_dir or "desc").lower()
    if sort_key not in {"name", "date"}:
        sort_key = "date"
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "desc"

    entries = []
    visible_dir_cache = {}
    scan_start = time.perf_counter()
    with os.scandir(current) as scan:
        for entry in scan:
            name = entry.name
            if not SHOW_HIDDEN and name.startswith("."):
                continue
            is_dir = entry.is_dir(follow_symlinks=False)
            if is_dir and name == "@eaDir":
                continue
            if is_dir and name.lower().endswith("_overlays"):
                continue
            is_file = entry.is_file(follow_symlinks=False)
            is_png = is_file and name.lower().endswith(".png") and not should_exclude_png_path(Path(entry.path))
            if is_dir and not directory_contains_browseable_png(Path(entry.path), visible_dir_cache):
                continue
            if not (is_dir or is_png):
                continue
            try:
                stat_start = time.perf_counter()
                st = entry.stat(follow_symlinks=False)
                perf["entry_stat_ms"] += elapsed_ms(stat_start)
            except OSError:
                st = None
            entries.append(
                {
                    "name": name,
                    "path": Path(entry.path),
                    "is_dir": is_dir,
                    "is_png": is_png,
                    "stat": st,
                }
            )
    perf["scan_entries_ms"] = elapsed_ms(scan_start)

    def entry_sort_key(entry):
        group = 0 if entry["is_dir"] else 1
        if sort_key == "date":
            primary = entry["stat"].st_mtime if entry["stat"] is not None else 0
            return (group, primary, entry["name"].lower())
        return (group, entry["name"].lower())

    entries.sort(key=entry_sort_key, reverse=(sort_dir == "desc"))
    entries.sort(key=lambda item: 0 if item["is_dir"] else 1)

    folders = []
    png_items = []
    thumb_dimensions_conn = None
    thumb_dimensions_dirty = False
    if include_thumb_dimensions:
        with thumb_dimensions_db_lock:
            thumb_dimensions_conn = connect_thumb_dimensions_db()
    for entry in entries:
        path = entry["path"]
        rel_path = rel_from_root(path)
        st = entry["stat"]
        if entry["is_dir"]:
            mtime = int(st.st_mtime) if st is not None else 0
            cover = None
            if include_folder_covers:
                cover_start = time.perf_counter()
                cover = find_folder_cover(path, dir_stat=st)
                perf["folder_cover_lookup_ms"] += elapsed_ms(cover_start)
            perf["folder_count"] += 1
            folders.append(
                {
                    "type": "dir",
                    "name": entry["name"],
                    "rel_path": rel_path,
                    "mtime": mtime,
                    "cover": cover,
                }
            )
        elif entry["is_png"] and st is not None:
            item_build_start = time.perf_counter()
            edit_lookup_start = time.perf_counter()
            current_edit = get_current_image_edit_summary(rel_path, st)
            perf["edit_lookup_ms"] += elapsed_ms(edit_lookup_start)
            thumb_sig = build_thumb_signature_from_current_edit(rel_path, st, current_edit=current_edit)
            square_cache_path = build_thumb_cache_path_from_signature(thumb_sig, "square")
            full_cache_path = build_thumb_cache_path_from_signature(thumb_sig, "full")
            preview_cache_path = build_thumb_cache_path_from_signature(thumb_sig, "preview")
            image_width = 0
            image_height = 0
            if include_thumb_dimensions and thumb_dimensions_conn is not None:
                image_width, image_height = get_cached_thumb_dimensions(thumb_dimensions_conn, thumb_sig)
            if include_thumb_dimensions and (image_width < 1 or image_height < 1):
                dimension_start = time.perf_counter()
                image_width, image_height = read_image_dimensions(full_cache_path)
                perf["dimension_read_ms"] += elapsed_ms(dimension_start)
                if thumb_dimensions_conn is not None and image_width > 0 and image_height > 0:
                    save_cached_thumb_dimensions(thumb_dimensions_conn, thumb_sig, image_width, image_height)
                    thumb_dimensions_dirty = True
            png_items.append(
                {
                    "type": "png",
                    "name": entry["name"],
                    "rel_path": rel_path,
                    "size_bytes": st.st_size,
                    "mtime": int(st.st_mtime),
                    "thumb_sig": thumb_sig,
                    "image_edit": build_gallery_image_edit_payload(current_edit),
                    "width": image_width or None,
                    "height": image_height or None,
                    "aspect_ratio": (float(image_width) / float(image_height)) if image_width > 0 and image_height > 0 else None,
                    "cache_paths": {
                        "square": square_cache_path,
                        "full": full_cache_path,
                        "preview": preview_cache_path,
                    },
                }
            )
            perf["png_item_build_ms"] += elapsed_ms(item_build_start)
            perf["png_count"] += 1

    breadcrumb = [{"name": "root", "rel_path": ""}]
    if current != BROWSE_ROOT:
        parts = current.relative_to(BROWSE_ROOT).parts
        acc = Path()
        for part in parts:
            acc = acc / part
            breadcrumb.append({"name": part, "rel_path": acc.as_posix()})

    parent_rel = "" if current == BROWSE_ROOT else rel_from_root(current.parent)
    if current == BROWSE_ROOT:
        folders.insert(0, build_favorites_folder_item())
        folders.insert(0, build_edits_folder_item())
    if thumb_dimensions_conn is not None:
        try:
            if thumb_dimensions_dirty:
                thumb_dimensions_conn.commit()
        finally:
            thumb_dimensions_conn.close()
    return current, folders, png_items, breadcrumb, parent_rel, perf


def get_sibling_folder_links(current: Path, sort_key: str = "date", sort_dir: str = "desc"):
    if current == BROWSE_ROOT:
        return None, None

    parent = current.parent
    current_rel = rel_from_root(current)
    siblings = []

    visible_dir_cache = {}
    with os.scandir(parent) as scan:
        for entry in scan:
            name = entry.name
            if not SHOW_HIDDEN and name.startswith("."):
                continue
            if not entry.is_dir(follow_symlinks=False):
                continue
            if name == "@eaDir" or name.lower().endswith("_overlays"):
                continue
            if not directory_contains_browseable_png(Path(entry.path), visible_dir_cache):
                continue
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError:
                st = None
            siblings.append(
                {
                    "name": name,
                    "rel_path": rel_from_root(Path(entry.path)),
                    "mtime": st.st_mtime if st is not None else 0,
                }
            )

    if not siblings:
        return None, None

    if sort_key == "date":
        siblings.sort(key=lambda item: (item["mtime"], item["name"].lower()), reverse=(sort_dir == "desc"))
    else:
        siblings.sort(key=lambda item: item["name"].lower(), reverse=(sort_dir == "desc"))

    current_idx = next((idx for idx, item in enumerate(siblings) if item["rel_path"] == current_rel), None)
    if current_idx is None:
        return None, None

    prev_item = siblings[current_idx - 1] if current_idx > 0 else None
    next_item = siblings[current_idx + 1] if current_idx + 1 < len(siblings) else None
    return prev_item, next_item


def format_bytes(size):
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024


def format_mtime(ts):
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def elapsed_ms(start_time: float) -> int:
    return int(round((time.perf_counter() - start_time) * 1000))


def get_directory_log_label(current_dir_rel: str, is_virtual_dir: bool) -> str:
    label = str(current_dir_rel or "").strip()
    if label:
        return label
    return label if is_virtual_dir else "root"


def extract_active_loras(summary):
    power_lora = (summary or {}).get("power_lora") or []
    active = []
    for item in power_lora:
        if not isinstance(item, dict):
            continue
        name = item.get("lora") or item.get("name")
        if not isinstance(name, str):
            continue
        if ".safetensors" not in name and ".ckpt" not in name:
            continue
        enabled = item.get("on", True)
        if isinstance(enabled, str):
            enabled = enabled.lower() in {"true", "1", "yes", "on"}
        if not enabled:
            continue
        active.append(
            {
                "name": name,
                "strength": item.get("strength", 1.0),
            }
        )
    return active


def build_manual_override_rows(summary):
    summary = summary or {}
    manual = summary.get("manual_overrides") or {}

    candidates = [
        ("Lora Prefix", summary.get("lora_prefix")),
        ("Style", manual.get("style_override")),
        ("Location", manual.get("location_override")),
        ("Character", manual.get("character_override")),
        ("Pose", manual.get("pose_override")),
        ("Main Prompt", manual.get("main_prompt")),
        ("Additional Keywords", manual.get("additional_keywords")),
    ]

    rows = []
    for label, value in candidates:
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text or text.lower() == "none":
                continue
            value = text
        rows.append(
            {
                "label": label,
                "value": value,
            }
        )
    return rows


def build_quad_rows(summary):
    summary = summary or {}
    quad = summary.get("quad") or {}

    candidates = [
        ("Style", quad.get("style")),
        ("Location", quad.get("location")),
        ("Character", quad.get("character")),
        ("Pose", quad.get("pose")),
    ]

    rows = []
    for label, value in candidates:
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text or text.lower() == "none":
                continue
            value = text
        rows.append({"label": label, "value": value})
    return rows


def extract_prompt_blocks(parsed):
    if not parsed:
        return {"main": None, "negative": None}

    summary = parsed.get("summary") or {}
    final_prompt = summary.get("final_prompt")
    if isinstance(final_prompt, str) and final_prompt.strip():
        return {"main": final_prompt.strip(), "negative": None}

    prompt = parsed.get("prompt")
    if not isinstance(prompt, dict):
        return {"main": None, "negative": None}

    main_candidates = []
    negative_candidates = []

    for node in prompt.values():
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue

        class_type = str(node.get("class_type", "")).lower()
        meta = node.get("_meta") or {}
        title = str(meta.get("title", "")).lower()

        for key, value in inputs.items():
            if not isinstance(value, str):
                continue
            text = value.strip()
            if not text:
                continue
            if len(text) < 12 and " " not in text:
                continue

            key_l = str(key).lower()
            score = 0
            if "cliptextencode" in class_type:
                score += 5
            if key_l in {"text", "prompt", "positive"}:
                score += 5
            if "prompt" in title or "positive" in title:
                score += 3
            if len(text) > 60:
                score += 2
            if any(ch in text for ch in [",", "(", ")", ":"]):
                score += 1

            if score <= 0:
                continue

            item = (score, len(text), text)
            if "negative" in key_l or "negative" in title:
                negative_candidates.append(item)
            else:
                main_candidates.append(item)

    def pick_best(items):
        if not items:
            return None
        items.sort(key=lambda t: (t[0], t[1]), reverse=True)
        return items[0][2]

    return {
        "main": pick_best(main_candidates),
        "negative": pick_best(negative_candidates),
    }


def summarize_directory_thumbnail_cache(png_items: list) -> dict:
    summary = {
        "total": len(png_items),
        "square_cached": 0,
        "square_missing": 0,
        "full_cached": 0,
        "full_missing": 0,
        "preview_cached": 0,
        "preview_missing": 0,
    }
    for item in png_items:
        cache_paths = item.get("cache_paths") if isinstance(item, dict) else None
        if not isinstance(cache_paths, dict):
            continue
        for mode in ("square", "full", "preview"):
            cache_path = cache_paths.get(mode)
            if isinstance(cache_path, Path) and cache_path.exists():
                summary[f"{mode}_cached"] += 1
            else:
                summary[f"{mode}_missing"] += 1
    return summary


def build_index_view_model(rel_dir: str, view_mode: str, sort_key: str, sort_dir: str):
    error = None
    is_virtual_dir = rel_dir in {FAVORITES_DIR_KEY, EDITS_DIR_KEY}
    timings = {
        "list_directory_ms": 0,
        "register_thumbnail_progress_ms": 0,
        "get_sibling_links_ms": 0,
        "scan_entries_ms": 0,
        "entry_stat_ms": 0,
        "png_item_build_ms": 0,
        "edit_lookup_ms": 0,
        "dimension_read_ms": 0,
        "folder_cover_lookup_ms": 0,
        "thumbnail_cache_summary": {
            "total": 0,
            "square_cached": 0,
            "square_missing": 0,
            "full_cached": 0,
            "full_missing": 0,
            "preview_cached": 0,
            "preview_missing": 0,
        },
    }

    try:
        list_start = time.perf_counter()
        current_dir, folders, png_items, breadcrumb, parent_rel, list_perf = list_directory(
            rel_dir,
            sort_key,
            sort_dir,
            include_thumb_dimensions=(view_mode == "thumbs-full"),
            include_folder_covers=(view_mode == "thumbs-full"),
        )
        timings["list_directory_ms"] = elapsed_ms(list_start)
        if isinstance(list_perf, dict):
            timings["scan_entries_ms"] = int(list_perf.get("scan_entries_ms") or 0)
            timings["entry_stat_ms"] = int(list_perf.get("entry_stat_ms") or 0)
            timings["png_item_build_ms"] = int(list_perf.get("png_item_build_ms") or 0)
            timings["edit_lookup_ms"] = int(list_perf.get("edit_lookup_ms") or 0)
            timings["dimension_read_ms"] = int(list_perf.get("dimension_read_ms") or 0)
            timings["folder_cover_lookup_ms"] = int(list_perf.get("folder_cover_lookup_ms") or 0)
    except Exception as exc:
        current_dir, folders, png_items, breadcrumb, parent_rel = BROWSE_ROOT, [], [], [{"name": "root", "rel_path": ""}], ""
        error = f"Cannot open directory: {exc}"
        is_virtual_dir = False

    if png_items and not is_virtual_dir:
        timings["thumbnail_cache_summary"] = summarize_directory_thumbnail_cache(png_items)
        register_start = time.perf_counter()
        register_directory_thumbnail_progress(current_dir, png_items, "square")
        register_directory_thumbnail_progress(current_dir, png_items, "full")
        register_directory_thumbnail_progress(current_dir, png_items, "preview")
        timings["register_thumbnail_progress_ms"] = elapsed_ms(register_start)

    sibling_start = time.perf_counter()
    prev_folder, next_folder = (None, None) if is_virtual_dir else get_sibling_folder_links(current_dir, sort_key, sort_dir)
    timings["get_sibling_links_ms"] = elapsed_ms(sibling_start)
    current_dir_rel = rel_dir if is_virtual_dir else (rel_from_root(current_dir) if current_dir != BROWSE_ROOT else "")

    return {
        "view_mode": view_mode,
        "sort_key": sort_key,
        "sort_dir": sort_dir,
        "current_dir_rel": current_dir_rel,
        "folders": folders,
        "png_items": png_items,
        "breadcrumb": breadcrumb,
        "parent_rel": parent_rel,
        "prev_folder": prev_folder,
        "next_folder": next_folder,
        "is_virtual_dir": is_virtual_dir,
        "error": error,
        "timings": timings,
    }


@app.context_processor
def inject_globals():
    return {
        "app_title": APP_TITLE,
        "app_title_mobile": APP_TITLE_MOBILE,
        "browse_root": str(BROWSE_ROOT),
        "format_bytes": format_bytes,
        "format_mtime": format_mtime,
        "auth_enabled": is_login_configured(),
    }


@app.before_request
def require_login():
    allowed_endpoints = {"login", "favicon", "static"}
    if request.endpoint in allowed_endpoints:
        return None

    if not is_login_configured():
        return None

    if is_authenticated_session():
        return None

    clear_auth_session()
    next_url = request.full_path if request.query_string else request.path
    if (request.path or "").startswith("/api/"):
        return {"error": "Authentication required", "login_url": url_for("login", next=next_url)}, 401
    return redirect(url_for("login", next=next_url))


@app.route("/login", methods=["GET", "POST"])
def login():
    if not is_login_configured():
        return (
            "Login is not configured. Set SECRET_KEY and APP_PASSWORD_HASH in the environment.",
            500,
        )

    if is_authenticated_session():
        next_url = (request.args.get("next") or request.form.get("next") or "").strip()
        if next_url and next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("index"))

    error = None
    next_url = (request.args.get("next") or request.form.get("next") or "").strip()
    if next_url and not next_url.startswith("/"):
        next_url = ""

    if request.method == "POST":
        client_key = get_request_client_key()
        if is_login_rate_limited(client_key):
            error = "Too many login attempts. Wait a few minutes and try again."
        else:
            password = request.form.get("password", "")
            remember_me = request.form.get("remember_me") in {"1", "true", "on", "yes"}
            current_password_hash = get_app_password_hash()
            if password and current_password_hash and check_password_hash(current_password_hash, password):
                session.clear()
                session[AUTH_SESSION_KEY] = True
                session[AUTH_FINGERPRINT_KEY] = current_auth_fingerprint()
                session.permanent = remember_me
                clear_failed_login_attempts(client_key)
                log_event("login_success", "Login successful", remember_me=remember_me)
                if next_url:
                    return redirect(next_url)
                return redirect(url_for("index"))

            record_failed_login_attempt(client_key)
            log_event("login_failure", "Login failed")
            error = "Incorrect password."

    return render_template("login.html", error=error, next_url=next_url, remember_me_days=REMEMBER_ME_DAYS)


@app.route("/api/auth/change-password", methods=["POST"])
def api_change_password():
    if not is_login_configured():
        return {"ok": False, "error": "Login is not configured"}, 409

    payload = request.get_json(silent=True) or {}
    current_password = str(payload.get("current_password") or "")
    new_password = str(payload.get("new_password") or "")
    confirm_password = str(payload.get("confirm_password") or "")

    if not current_password:
        return {"ok": False, "error": "Current password is required"}, 400
    if not new_password:
        return {"ok": False, "error": "New password is required"}, 400
    if len(new_password) < 8:
        return {"ok": False, "error": "New password must be at least 8 characters"}, 400
    if new_password != confirm_password:
        return {"ok": False, "error": "New password and confirmation do not match"}, 400

    current_password_hash = get_app_password_hash()
    if not current_password_hash or not check_password_hash(current_password_hash, current_password):
        return {"ok": False, "error": "Current password is incorrect"}, 403

    new_password_hash = generate_password_hash(new_password)
    try:
        persist_password_hash_to_env_file(new_password_hash)
    except Exception as exc:
        log_event("password_change_persist_failure", "Failed to persist password change", error=str(exc), env_path=str(AUTH_ENV_FILE_PATH))
        return {"ok": False, "error": f"Failed to update password file: {exc}"}, 500

    set_app_password_hash(new_password_hash)
    session[AUTH_SESSION_KEY] = True
    session[AUTH_FINGERPRINT_KEY] = current_auth_fingerprint()
    log_event("password_changed", "Shared password changed", env_path=str(AUTH_ENV_FILE_PATH))
    return {"ok": True, "message": "Password updated. Other remembered sessions must sign in again."}


@app.route("/logout", methods=["POST"])
def logout():
    clear_auth_session()
    session.clear()
    log_event("logout", "Session cleared")
    return redirect(url_for("login"))


@app.route("/")
def index():
    ensure_root_exists()
    rel_dir = request.args.get("dir", "")
    rel_dir = (rel_dir or "").strip()
    view_mode = request.args.get("view", "thumbs-full").lower()
    sort_key = request.args.get("sort", "date").lower()
    sort_dir = request.args.get("order", "desc").lower()
    if view_mode not in {"list", "thumbs-full"}:
        view_mode = "thumbs-full"
    if sort_key not in {"name", "date"}:
        sort_key = "date"
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "desc"
    view_start = time.perf_counter()
    context = build_index_view_model(rel_dir, view_mode, sort_key, sort_dir)
    render_start = time.perf_counter()
    response = render_template("index.html", **context)
    render_ms = elapsed_ms(render_start)
    total_ms = elapsed_ms(view_start)
    timings = context.get("timings") if isinstance(context.get("timings"), dict) else {}
    thumb_summary = timings.get("thumbnail_cache_summary") if isinstance(timings.get("thumbnail_cache_summary"), dict) else {}
    directory_label = get_directory_log_label(context.get("current_dir_rel") or "", bool(context.get("is_virtual_dir")))
    log_event(
        "folder_view",
        "Folder view rendered",
        route="index",
        directory=directory_label,
        view_mode=view_mode,
        sort_key=sort_key,
        sort_dir=sort_dir,
        is_virtual_dir=bool(context.get("is_virtual_dir")),
        error=context.get("error") or "",
        folder_count=len(context.get("folders") or []),
        image_count=len(context.get("png_items") or []),
        list_directory_ms=int(timings.get("list_directory_ms") or 0),
        scan_entries_ms=int(timings.get("scan_entries_ms") or 0),
        entry_stat_ms=int(timings.get("entry_stat_ms") or 0),
        png_item_build_ms=int(timings.get("png_item_build_ms") or 0),
        edit_lookup_ms=int(timings.get("edit_lookup_ms") or 0),
        dimension_read_ms=int(timings.get("dimension_read_ms") or 0),
        folder_cover_lookup_ms=int(timings.get("folder_cover_lookup_ms") or 0),
        register_thumbnail_progress_ms=int(timings.get("register_thumbnail_progress_ms") or 0),
        get_sibling_links_ms=int(timings.get("get_sibling_links_ms") or 0),
        render_ms=render_ms,
        total_ms=total_ms,
        square_cached=int(thumb_summary.get("square_cached") or 0),
        square_missing=int(thumb_summary.get("square_missing") or 0),
        full_cached=int(thumb_summary.get("full_cached") or 0),
        full_missing=int(thumb_summary.get("full_missing") or 0),
        preview_cached=int(thumb_summary.get("preview_cached") or 0),
        preview_missing=int(thumb_summary.get("preview_missing") or 0),
    )
    return response


@app.route("/api/folder")
def api_folder():
    ensure_root_exists()
    rel_dir = (request.args.get("dir", "") or "").strip()
    view_mode = request.args.get("view", "thumbs-full").lower()
    sort_key = request.args.get("sort", "date").lower()
    sort_dir = request.args.get("order", "desc").lower()
    if view_mode not in {"list", "thumbs-full"}:
        view_mode = "thumbs-full"
    if sort_key not in {"name", "date"}:
        sort_key = "date"
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "desc"

    view_start = time.perf_counter()
    context = build_index_view_model(rel_dir, view_mode, sort_key, sort_dir)
    render_start = time.perf_counter()
    browser_list_html = render_template("_browser_list.html", **context)
    pathline_html = render_template("_pathline_row.html", **context)
    render_ms = elapsed_ms(render_start)
    total_ms = elapsed_ms(view_start)
    timings = context.get("timings") if isinstance(context.get("timings"), dict) else {}
    thumb_summary = timings.get("thumbnail_cache_summary") if isinstance(timings.get("thumbnail_cache_summary"), dict) else {}
    directory_label = get_directory_log_label(context.get("current_dir_rel") or "", bool(context.get("is_virtual_dir")))
    log_event(
        "folder_view",
        "Folder view rendered",
        route="api_folder",
        directory=directory_label,
        view_mode=view_mode,
        sort_key=sort_key,
        sort_dir=sort_dir,
        is_virtual_dir=bool(context.get("is_virtual_dir")),
        error=context.get("error") or "",
        folder_count=len(context.get("folders") or []),
        image_count=len(context.get("png_items") or []),
        list_directory_ms=int(timings.get("list_directory_ms") or 0),
        scan_entries_ms=int(timings.get("scan_entries_ms") or 0),
        entry_stat_ms=int(timings.get("entry_stat_ms") or 0),
        png_item_build_ms=int(timings.get("png_item_build_ms") or 0),
        edit_lookup_ms=int(timings.get("edit_lookup_ms") or 0),
        dimension_read_ms=int(timings.get("dimension_read_ms") or 0),
        folder_cover_lookup_ms=int(timings.get("folder_cover_lookup_ms") or 0),
        register_thumbnail_progress_ms=int(timings.get("register_thumbnail_progress_ms") or 0),
        get_sibling_links_ms=int(timings.get("get_sibling_links_ms") or 0),
        render_ms=render_ms,
        total_ms=total_ms,
        square_cached=int(thumb_summary.get("square_cached") or 0),
        square_missing=int(thumb_summary.get("square_missing") or 0),
        full_cached=int(thumb_summary.get("full_cached") or 0),
        full_missing=int(thumb_summary.get("full_missing") or 0),
        preview_cached=int(thumb_summary.get("preview_cached") or 0),
        preview_missing=int(thumb_summary.get("preview_missing") or 0),
    )
    return {
        "ok": True,
        "dir": context["current_dir_rel"],
        "view_mode": view_mode,
        "sort_key": sort_key,
        "sort_dir": sort_dir,
        "is_virtual_dir": context["is_virtual_dir"],
        "error": context["error"],
        "browser_list_html": browser_list_html,
        "pathline_html": pathline_html,
        "folder_count": len(context["folders"]),
        "gallery_count": len(context["png_items"]),
    }


@app.route("/api/metadata")
def api_metadata():
    ensure_root_exists()
    rel_file = request.args.get("file", "")
    if not rel_file:
        return {"error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"error": "This PNG variant is hidden by filter rules"}, 404

    if not path.is_file() or path.suffix.lower() != ".png":
        return {"error": "Not a valid PNG file"}, 400

    try:
        parsed, _st = get_cached_parsed_metadata(path)
        return build_metadata_response(path, parsed, _st)
    except Exception as exc:
        log_event("metadata_parse_failure", "Metadata parse failed", file=rel_file, error=str(exc))
        return {"error": f"Failed to parse metadata: {exc}"}, 500


@app.route("/api/metadata/raw")
def api_metadata_raw():
    ensure_root_exists()
    rel_file = request.args.get("file", "")
    if not rel_file:
        return {"error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"error": "This PNG variant is hidden by filter rules"}, 404

    if not path.is_file() or path.suffix.lower() != ".png":
        return {"error": "Not a valid PNG file"}, 400

    try:
        parsed, _st = get_cached_parsed_metadata(path)
        return build_raw_metadata_response(parsed)
    except Exception as exc:
        log_event("metadata_parse_failure", "Raw metadata parse failed", file=rel_file, error=str(exc))
        return {"error": f"Failed to parse raw metadata: {exc}"}, 500


@app.route("/api/metadata-index/search")
def api_metadata_index_search():
    ensure_root_exists()
    query = request.args.get("q", "")
    sort_key = request.args.get("sort", "date").lower()
    sort_dir = request.args.get("order", "desc").lower()
    view_mode = request.args.get("view", "thumbs-full").lower()
    scope_mode = (request.args.get("scope") or "top-level").strip().lower()
    scope_dir = (request.args.get("scope_dir") or "").strip().replace("\\", "/")
    filename_term = (request.args.get("filename") or "").strip()
    favorites_only = request.args.get("favorites_only", "0") in {"1", "true", "yes", "on"}
    try:
        offset = max(0, int(request.args.get("offset", "0")))
    except ValueError:
        offset = 0
    try:
        limit = max(1, min(int(request.args.get("limit", "60")), 120))
    except ValueError:
        limit = 60

    if sort_key not in {"name", "date"}:
        sort_key = "date"
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "desc"
    if view_mode not in {"list", "thumbs-full"}:
        view_mode = "thumbs-full"
    thumb_mode = "full"
    if scope_mode not in {"top-level", "all", "current", "favorites"}:
        scope_mode = "top-level"
    top_level_only = scope_mode == "top-level"
    if scope_mode == "favorites":
        favorites_only = True
        top_level_only = False
    if scope_mode == "current" and not scope_dir:
        return {"error": "Missing scope directory"}, 400

    scoped_dir_value = scope_dir if scope_mode == "current" else None
    favorite_paths = get_valid_favorite_paths() if favorites_only else None

    if not (METADATA_DB_PATH.exists() and METADATA_DB_PATH.is_file()):
        return {
            "query": query,
            "items": [],
            "offset": offset,
            "limit": limit,
            "total": 0,
            "has_more": False,
            "indexed": False,
            "error": "Metadata index database not found",
        }

    try:
        conn = metadata_index.connect(METADATA_DB_PATH)
        try:
            metadata_index.initialize(conn)
            tag_aliases = load_tag_aliases_config()
            result = metadata_index.search_results(
                conn,
                query,
                sort_key=sort_key,
                sort_dir=sort_dir,
                offset=offset,
                limit=limit,
                top_level_only=top_level_only,
                scope_dir=scoped_dir_value,
                favorites_only_paths=favorite_paths,
                filename_term=filename_term,
                tag_search_terms=tag_aliases.get("search_terms_by_normalized"),
            )
        finally:
            conn.close()
    except Exception as exc:
        log_event("metadata_index_search_failure", "Metadata index search failed", query=query[:160], error=str(exc))
        return {"error": f"Failed to search metadata index: {exc}"}, 500

    items = []
    for row in result["items"]:
        rel_path = row["path"]
        try:
            path = resolve_safe_path(rel_path)
            st = path.stat()
        except OSError:
            continue
        items.append(
            {
                "rel_path": rel_path,
                "name": row["name"],
                "directory": row["directory"],
                "is_favorite": is_favorited(rel_path, st),
                "thumb_sig": build_thumb_signature(rel_path, st),
                "thumb_url": url_for("image_thumbnail", file=rel_path, mode=thumb_mode, v=build_thumb_signature(rel_path, st)),
                "image_edit": build_image_edit_payload(rel_path, st),
            }
        )

    next_offset = offset + len(items)
    return {
        "query": query,
        "items": items,
        "offset": offset,
        "limit": limit,
        "next_offset": next_offset,
        "total": int(result["total"]),
        "has_more": next_offset < int(result["total"]),
        "indexed": True,
    }


@app.route("/api/metadata-index/tag-explorer")
def api_metadata_index_tag_explorer():
    ensure_root_exists()
    query = request.args.get("q", "")
    scope_mode = (request.args.get("scope") or "top-level").strip().lower()
    scope_dir = (request.args.get("scope_dir") or "").strip().replace("\\", "/")
    filename_term = (request.args.get("filename") or "").strip()
    favorites_only = request.args.get("favorites_only", "0") in {"1", "true", "yes", "on"}
    try:
        limit = max(1, min(int(request.args.get("limit", "50")), 150))
    except ValueError:
        limit = 50
    try:
        min_count = max(1, min(int(request.args.get("min_count", "1")), 999))
    except ValueError:
        min_count = 1
    raw_max_count = (request.args.get("max_count") or "").strip()
    max_count = None
    if raw_max_count:
        try:
            parsed_max_count = int(raw_max_count)
        except ValueError:
            parsed_max_count = None
        if parsed_max_count is not None:
            max_count = max(min_count, min(parsed_max_count, 999999))
    hide_generic = request.args.get("hide_generic", "1") in {"1", "true", "yes", "on"}

    if scope_mode not in {"top-level", "all", "current", "favorites"}:
        scope_mode = "top-level"
    top_level_only = scope_mode == "top-level"
    if scope_mode == "favorites":
        favorites_only = True
        top_level_only = False
    if scope_mode == "current" and not scope_dir:
        return {"error": "Missing scope directory"}, 400

    scoped_dir_value = scope_dir if scope_mode == "current" else None
    favorite_paths = get_valid_favorite_paths() if favorites_only else None

    if not (METADATA_DB_PATH.exists() and METADATA_DB_PATH.is_file()):
        return {
            "query": query,
            "items": [],
            "matched_count": 0,
            "tagged_count": 0,
            "total_tag_instances": 0,
            "indexed": False,
            "error": "Metadata index database not found",
        }

    try:
        conn = metadata_index.connect(METADATA_DB_PATH)
        try:
            metadata_index.initialize(conn)
            tag_aliases = load_tag_aliases_config()
            generic_tags = load_generic_tags_config()
            result = metadata_index.summarize_ai_tags(
                conn,
                query=query,
                top_level_only=top_level_only,
                scope_dir=scoped_dir_value,
                favorites_only_paths=favorite_paths,
                filename_term=filename_term,
                tag_limit=limit,
                min_count=min_count,
                max_count=max_count,
                tag_search_terms=tag_aliases.get("search_terms_by_normalized"),
                tag_canonical_map=tag_aliases.get("canonical_by_normalized"),
                hidden_generic_tags=generic_tags.get("hidden_generic_tags") if hide_generic else None,
            )
        finally:
            conn.close()
    except Exception as exc:
        log_event("metadata_index_tag_explorer_failure", "Tag explorer failed", query=query[:160], error=str(exc))
        return {"error": f"Failed to load tag explorer: {exc}"}, 500

    return {
        "query": query,
        "items": result["items"],
        "matched_count": int(result["matched_count"]),
        "tagged_count": int(result["tagged_count"]),
        "total_tag_instances": int(result["total_tag_instances"]),
        "hide_generic": bool(hide_generic),
        "max_count": max_count,
        "indexed": True,
    }


@app.route("/api/metadata-index/tag-explorer/export")
def api_metadata_index_tag_explorer_export():
    ensure_root_exists()
    try:
        min_count = max(1, min(int(request.args.get("min_count", "1")), 999))
    except ValueError:
        min_count = 1

    if not (METADATA_DB_PATH.exists() and METADATA_DB_PATH.is_file()):
        return {"error": "Metadata index database not found"}, 404

    try:
        conn = metadata_index.connect(METADATA_DB_PATH)
        try:
            metadata_index.initialize(conn)
            result = metadata_index.summarize_ai_tags(
                conn,
                query=None,
                top_level_only=False,
                scope_dir=None,
                favorites_only_paths=None,
                filename_term=None,
                tag_limit=None,
                min_count=min_count,
            )
        finally:
            conn.close()
    except Exception as exc:
        log_event("metadata_index_tag_export_failure", "Tag CSV export failed", error=str(exc))
        return {"error": f"Failed to export tags: {exc}"}, 500

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["tag", "count", "tagged_images", "total_tag_instances", "min_count"])
    for item in result["items"]:
        writer.writerow([
            item["tag"],
            int(item["count"]),
            int(result["tagged_count"]),
            int(result["total_tag_instances"]),
            int(min_count),
        ])

    filename = f"ai-tags-full-corpus-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
    return Response(
        csv_buffer.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@app.route("/api/search/scopes")
def api_search_scopes():
    ensure_root_exists()
    return {"items": list_search_scope_directories()}


@app.route("/image")
def image_preview():
    ensure_root_exists()
    rel_file = request.args.get("file", "")
    variant = request.args.get("variant", "")
    try:
        path = resolve_safe_path(rel_file)
    except ValueError:
        abort(400)

    if should_exclude_png_path(path):
        abort(404)

    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        abort(404)
    try:
        st = path.stat()
        effective_path = get_effective_image_path_for_variant(rel_file, path, st=st, variant=variant)
    except OSError:
        abort(404)
    return send_file(effective_path, mimetype="image/png", conditional=True)


@app.route("/thumb")
def image_thumbnail():
    ensure_root_exists()
    rel_file = request.args.get("file", "")
    mode = request.args.get("mode", "full").lower()
    variant = request.args.get("variant", "")
    try:
        path = resolve_safe_path(rel_file)
    except ValueError:
        abort(400)

    if should_exclude_png_path(path):
        abort(404)

    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        abort(404)
    if mode not in {"square", "full", "preview"}:
        abort(400)

    try:
        cache_path = generate_thumbnail(path, rel_file, mode, variant=variant)
    except Exception:
        abort(500)

    return send_file(
        cache_path,
        mimetype="image/webp",
        conditional=True,
        max_age=THUMB_CACHE_MAX_AGE,
    )


@app.route("/download")
def download_original():
    ensure_root_exists()
    rel_file = request.args.get("file", "")
    try:
        path = resolve_safe_path(rel_file)
    except ValueError:
        abort(400)

    if should_exclude_png_path(path):
        abort(404)

    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        abort(404)
    return send_file(path, mimetype="image/png", as_attachment=True, download_name=path.name)


@app.route("/api/image-edits/repair", methods=["POST"])
def api_repair_image_region():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    selection = payload.get("selection")
    mask_strokes = payload.get("mask_strokes")
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400
    if not isinstance(selection, dict):
        return {"ok": False, "error": "Missing 'selection' rectangle"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400
    if cv2 is None or np is None:
        return {"ok": False, "error": "Local image repair dependencies are not installed"}, 409

    try:
        mask_strokes_bytes = decode_png_data_url(mask_strokes) if mask_strokes else None
        st = path.stat()
        result = apply_repair_to_image(rel_file, path, st, selection, mask_strokes_bytes=mask_strokes_bytes)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        log_event("image_edit_repair_failure", "Image repair failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to repair image region: {exc}"}, 500

    log_event(
        "image_edit_repair_completed",
        "Image region repaired",
        file=rel_file,
        selection_width=int(selection.get("width") or 0),
        selection_height=int(selection.get("height") or 0),
    )
    return {
        "ok": True,
        "file": rel_file,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/image-edits/crop", methods=["POST"])
def api_crop_image():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    image_data = payload.get("image")
    selection = payload.get("selection")
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400
    if not image_data:
        return {"ok": False, "error": "Missing cropped image payload"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400

    try:
        rendered_bytes = decode_png_data_url(image_data)
        with Image.open(io.BytesIO(rendered_bytes)) as img:
            output = ImageOps.exif_transpose(img).convert("RGBA")
        if output.width < 1 or output.height < 1:
            raise ValueError("Cropped image is empty")
        st = path.stat()
        history_entry = {
            "type": "crop",
            "selection": selection if isinstance(selection, dict) else None,
        }
        result = save_image_edit_variant(rel_file, st, output, history_entry)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        log_event("image_edit_crop_failure", "Image crop save failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to save cropped image: {exc}"}, 500

    log_event(
        "image_edit_crop_completed",
        "Image crop saved",
        file=rel_file,
        crop_width=int(output.width),
        crop_height=int(output.height),
    )
    return {
        "ok": True,
        "file": rel_file,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/image-edits/color-adjust", methods=["POST"])
def api_color_adjust_image():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400
    if np is None:
        return {"ok": False, "error": "Color adjustment dependencies are not installed"}, 409

    try:
        st = path.stat()
        effective_path = get_effective_image_path(rel_file, path, st=st)
        with Image.open(effective_path) as img:
            source_image = ImageOps.exif_transpose(img).convert("RGBA")
        temperature = _clamp_adjustment(payload.get("temperature"))
        tint = _clamp_adjustment(payload.get("tint"))
        saturation = _clamp_adjustment(payload.get("saturation"))
        vibrance = _clamp_adjustment(payload.get("vibrance"))
        output = apply_color_adjustments(
            source_image,
            temperature=temperature,
            tint=tint,
            saturation=saturation,
            vibrance=vibrance,
        )
        result = save_image_edit_variant(rel_file, st, output, {
            "type": "color_balance",
            "temperature": temperature,
            "tint": tint,
            "saturation": saturation,
            "vibrance": vibrance,
        })
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        log_event("image_edit_color_adjust_failure", "Image color adjustment save failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to save color adjustment: {exc}"}, 500

    log_event(
        "image_edit_color_adjust_completed",
        "Image color adjustment saved",
        file=rel_file,
        temperature=temperature,
        tint=tint,
        saturation=saturation,
        vibrance=vibrance,
    )
    return {
        "ok": True,
        "file": rel_file,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/image-edits/matte", methods=["POST"])
def api_matte_adjust_image():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400
    if np is None:
        return {"ok": False, "error": "Matte adjustment dependencies are not installed"}, 409

    try:
        st = path.stat()
        effective_path = get_effective_image_path(rel_file, path, st=st)
        with Image.open(effective_path) as img:
            source_image = ImageOps.exif_transpose(img).convert("RGBA")
        matte = _clamp_adjustment(payload.get("matte"), 0.0, 100.0)
        curve = _clamp_adjustment(payload.get("curve"), 0.0, 100.0)
        whites = _clamp_adjustment(payload.get("whites"), -100.0, 100.0)
        output = apply_matte_adjustments(
            source_image,
            matte=matte,
            curve=curve,
            whites=whites,
        )
        result = save_image_edit_variant(rel_file, st, output, {
            "type": "matte_black",
            "matte": matte,
            "curve": curve,
            "whites": whites,
        })
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        log_event("image_edit_matte_adjust_failure", "Image matte adjustment save failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to save matte adjustment: {exc}"}, 500

    log_event(
        "image_edit_matte_adjust_completed",
        "Image matte adjustment saved",
        file=rel_file,
        matte=matte,
        curve=curve,
        whites=whites,
    )
    return {
        "ok": True,
        "file": rel_file,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/image-edits/vignette", methods=["POST"])
def api_vignette_adjust_image():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400
    if np is None:
        return {"ok": False, "error": "Vignette adjustment dependencies are not installed"}, 409

    try:
        st = path.stat()
        effective_path = get_effective_image_path(rel_file, path, st=st)
        with Image.open(effective_path) as img:
            source_image = ImageOps.exif_transpose(img).convert("RGBA")
        center_x = _clamp_adjustment(payload.get("center_x"), 0.0, 1.0)
        center_y = _clamp_adjustment(payload.get("center_y"), 0.0, 1.0)
        size = _clamp_adjustment(payload.get("size"), 0.0, 100.0)
        feather = _clamp_adjustment(payload.get("feather"), 0.0, 100.0)
        inner_brightness = _clamp_adjustment(payload.get("inner_brightness"), -100.0, 100.0)
        outer_brightness = _clamp_adjustment(payload.get("outer_brightness"), -150.0, 100.0)
        highlight_protect = _clamp_adjustment(payload.get("highlight_protect"), 0.0, 100.0)
        output = apply_vignette_adjustments(
            source_image,
            center_x=center_x,
            center_y=center_y,
            size=size,
            feather=feather,
            inner_brightness=inner_brightness,
            outer_brightness=outer_brightness,
            highlight_protect=highlight_protect,
        )
        result = save_image_edit_variant(rel_file, st, output, {
            "type": "vignette",
            "vignette_center_x": center_x,
            "vignette_center_y": center_y,
            "vignette_size": size,
            "vignette_feather": feather,
            "vignette_inner_brightness": inner_brightness,
            "vignette_outer_brightness": outer_brightness,
            "vignette_highlight_protect": highlight_protect,
        })
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        log_event("image_edit_vignette_adjust_failure", "Image vignette adjustment save failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to save vignette adjustment: {exc}"}, 500

    log_event(
        "image_edit_vignette_adjust_completed",
        "Image vignette adjustment saved",
        file=rel_file,
        center_x=center_x,
        center_y=center_y,
        size=size,
        feather=feather,
        inner_brightness=inner_brightness,
        outer_brightness=outer_brightness,
        highlight_protect=highlight_protect,
    )
    return {
        "ok": True,
        "file": rel_file,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/look-presets", methods=["GET"])
def api_list_look_presets():
    try:
        conn = connect_look_presets_db()
        try:
            rows = conn.execute(
                """
                SELECT id, name, modules_json, created_at, updated_at, last_used_at, use_count
                FROM look_presets
                ORDER BY COALESCE(last_used_at, updated_at) DESC, id DESC
                """
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": f"Failed to load look presets: {exc}"}, 500

    return {
        "ok": True,
        "presets": [serialize_look_preset(row) for row in rows],
    }


@app.route("/api/look-presets", methods=["POST"])
def api_create_look_preset():
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name") or "").strip()
    if not name:
        return {"ok": False, "error": "Missing preset name"}, 400

    look_payload = build_look_payload(payload.get("steps"), payload.get("modules"))
    if not look_payload["steps"]:
        return {"ok": False, "error": "No reusable look settings to save"}, 400

    timestamp = utc_now_iso()
    try:
        conn = connect_look_presets_db()
        try:
            cursor = conn.execute(
                """
                INSERT INTO look_presets (name, modules_json, created_at, updated_at, last_used_at, use_count)
                VALUES (?, ?, ?, ?, NULL, 0)
                """,
                (name, json.dumps(look_payload, ensure_ascii=True), timestamp, timestamp),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, name, modules_json, created_at, updated_at, last_used_at, use_count
                FROM look_presets
                WHERE id = ?
                """,
                (cursor.lastrowid,),
            ).fetchone()
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": f"Failed to save look preset: {exc}"}, 500

    return {
        "ok": True,
        "preset": serialize_look_preset(row),
    }


@app.route("/api/look-presets/<int:preset_id>", methods=["DELETE"])
def api_delete_look_preset(preset_id: int):
    try:
        conn = connect_look_presets_db()
        try:
            row = conn.execute("SELECT id FROM look_presets WHERE id = ?", (preset_id,)).fetchone()
            if not row:
                return {"ok": False, "error": "Look preset not found"}, 404
            conn.execute("DELETE FROM look_presets WHERE id = ?", (preset_id,))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": f"Failed to delete look preset: {exc}"}, 500

    return {"ok": True, "deleted_id": preset_id}


@app.route("/api/image-edits/apply-look", methods=["POST"])
def api_apply_look_preset():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    preset_id = payload.get("preset_id")
    try:
        preset_id = int(preset_id)
    except Exception:
        return {"ok": False, "error": "Missing or invalid preset id"}, 400

    try:
        conn = connect_look_presets_db()
        try:
            row = conn.execute(
                """
                SELECT id, name, modules_json, created_at, updated_at, last_used_at, use_count
                FROM look_presets
                WHERE id = ?
                """,
                (preset_id,),
            ).fetchone()
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": f"Failed to load look preset: {exc}"}, 500

    if not row:
        return {"ok": False, "error": "Look preset not found"}, 404

    preset = serialize_look_preset(row)
    steps = sanitize_look_steps(preset.get("steps"))
    if not steps:
        fallback_payload = build_look_payload(None, preset.get("modules"))
        steps = fallback_payload["steps"]
    modules = build_look_payload(steps, None)["modules"]
    if not steps:
        return {"ok": False, "error": "Look preset has no reusable modules"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400
    if np is None:
        return {"ok": False, "error": "Edit dependencies are not installed"}, 409

    try:
        st = path.stat()
        effective_path = get_effective_image_path(rel_file, path, st=st)
        with Image.open(effective_path) as img:
            output, _ = apply_look_steps_to_image(ImageOps.exif_transpose(img).convert("RGBA"), steps, None)

        color = modules.get("color")
        brightness_contrast = modules.get("brightness_contrast")
        matte = modules.get("matte")
        vignette = modules.get("vignette")
        result = save_image_edit_variant(rel_file, st, output, {
            "type": "look_preset",
            "preset_name": preset.get("name") or "",
            "look_steps": steps,
            "look_modules": modules,
            "temperature": (color or {}).get("temperature", 0.0),
            "tint": (color or {}).get("tint", 0.0),
            "saturation": (color or {}).get("saturation", 0.0),
            "vibrance": (color or {}).get("vibrance", 0.0),
            "brightness": (brightness_contrast or {}).get("brightness", 0.0),
            "contrast": (brightness_contrast or {}).get("contrast", 0.0),
            "matte": (matte or {}).get("matte", 0.0),
            "curve": (matte or {}).get("curve", 0.0),
            "whites": (matte or {}).get("whites", 0.0),
            "vignette_center_x": (vignette or {}).get("center_x", 0.5),
            "vignette_center_y": (vignette or {}).get("center_y", 0.5),
            "vignette_size": (vignette or {}).get("size", 40.0),
            "vignette_feather": (vignette or {}).get("feather", 45.0),
            "vignette_inner_brightness": (vignette or {}).get("inner_brightness", 0.0),
            "vignette_outer_brightness": (vignette or {}).get("outer_brightness", 0.0),
            "vignette_highlight_protect": (vignette or {}).get("highlight_protect", 70.0),
        })

        timestamp = utc_now_iso()
        conn = connect_look_presets_db()
        try:
            conn.execute(
                """
                UPDATE look_presets
                SET last_used_at = ?, use_count = use_count + 1
                WHERE id = ?
                """,
                (timestamp, preset_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        log_event("image_edit_apply_look_failure", "Image look preset apply failed", file=rel_file, preset_id=preset_id, error=str(exc))
        return {"ok": False, "error": f"Failed to apply look preset: {exc}"}, 500

    log_event(
        "image_edit_apply_look_completed",
        "Image look preset applied",
        file=rel_file,
        preset_id=preset_id,
        preset_name=preset.get("name") or "",
    )
    return {
        "ok": True,
        "file": rel_file,
        "preset": preset,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/image-edits/editor-apply", methods=["POST"])
def api_apply_editor_session():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400
    if np is None:
        return {"ok": False, "error": "Edit dependencies are not installed"}, 409

    try:
        st = path.stat()
        base_path = get_effective_image_path_for_variant(rel_file, path, st=st, variant="editor-base")
        with Image.open(base_path) as img:
            editor_base = ImageOps.exif_transpose(img).convert("RGBA")
        output, look_payload = apply_look_steps_to_image(editor_base, payload.get("steps"), payload.get("modules"))
        paths = get_edit_paths(rel_file)
        paths["dir"].mkdir(parents=True, exist_ok=True)
        editor_base.save(paths["editor_base"], format="PNG")
        modules = look_payload["modules"]
        color = modules.get("color")
        brightness_contrast = modules.get("brightness_contrast")
        matte = modules.get("matte")
        vignette = modules.get("vignette")
        result = save_image_edit_variant(
            rel_file,
            st,
            output,
            {
                "type": "editor_session",
                "look_steps": look_payload["steps"],
                "look_modules": modules,
                "temperature": (color or {}).get("temperature", 0.0),
                "tint": (color or {}).get("tint", 0.0),
                "saturation": (color or {}).get("saturation", 0.0),
                "vibrance": (color or {}).get("vibrance", 0.0),
                "brightness": (brightness_contrast or {}).get("brightness", 0.0),
                "contrast": (brightness_contrast or {}).get("contrast", 0.0),
                "matte": (matte or {}).get("matte", 0.0),
                "curve": (matte or {}).get("curve", 0.0),
                "whites": (matte or {}).get("whites", 0.0),
                "vignette_center_x": (vignette or {}).get("center_x", 0.5),
                "vignette_center_y": (vignette or {}).get("center_y", 0.5),
                "vignette_size": (vignette or {}).get("size", 40.0),
                "vignette_feather": (vignette or {}).get("feather", 45.0),
                "vignette_inner_brightness": (vignette or {}).get("inner_brightness", 0.0),
                "vignette_outer_brightness": (vignette or {}).get("outer_brightness", 0.0),
                "vignette_highlight_protect": (vignette or {}).get("highlight_protect", 70.0),
            },
            extra_meta={"editor_base_image": paths["editor_base"].name},
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        log_event("image_edit_editor_apply_failure", "Image editor session apply failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to apply editor session: {exc}"}, 500

    log_event(
        "image_edit_editor_apply_completed",
        "Image editor session applied",
        file=rel_file,
        steps=len(look_payload["steps"]),
    )
    return {
        "ok": True,
        "file": rel_file,
        "image_url": url_for("image_preview", file=rel_file, v=result["meta"].get("updated_at") or ""),
        "image_edit": build_image_edit_payload(rel_file, st),
    }


@app.route("/api/image-edits/revert", methods=["POST"])
def api_revert_image_edit():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400

    clear_image_edit(rel_file)
    log_event("image_edit_reverted", "Edited image reverted to original", file=rel_file)
    return {"ok": True, "file": rel_file}


@app.route("/api/favorites/toggle", methods=["POST"])
def api_toggle_favorite():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"error": "Not a valid PNG file"}, 400

    try:
        st = path.stat()
    except OSError:
        return {"error": "Unable to read file metadata"}, 500

    enabled = not is_favorited(rel_file, st)
    is_now_favorite = set_favorite(rel_file, st, enabled)
    log_event(
        "favorite_toggle",
        "Favorite updated",
        file=rel_file,
        favorite=is_now_favorite,
    )
    return {"ok": True, "is_favorite": is_now_favorite}


@app.route("/api/folder-cover", methods=["POST"])
def api_set_folder_cover():
    ensure_root_exists()
    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"error": "Not a valid PNG file"}, 400
    if path.parent == BROWSE_ROOT:
        return {"error": "Root images cannot be used as folder covers"}, 400

    folder_rel = rel_from_root(path.parent)
    if folder_rel in {FAVORITES_DIR_KEY, EDITS_DIR_KEY}:
        return {"error": "Virtual folders cannot have manual covers"}, 400
    if not directory_contains_browseable_png(path.parent, {}):
        return {"error": "Folder is not browseable"}, 400

    with folder_covers_state_lock:
        state = load_folder_covers_state()
        state["entries"][folder_rel] = {
            "rel_path": rel_file,
            "set_at": utc_now_iso(),
        }
        save_folder_covers_state(state)

    invalidate_folder_cover_cache(path.parent)
    cover = build_folder_cover_payload(rel_file)
    log_event("folder_cover_set", "Folder cover updated", folder=folder_rel, file=rel_file)
    return {
        "ok": True,
        "folder": folder_rel,
        "file": rel_file,
        "cover": cover,
    }


@app.route("/api/cache/clear", methods=["POST"])
def api_clear_cache():
    preview_status = get_rebuild_status_snapshot()
    if is_rebuild_status_active(preview_status):
        return {"error": "Cannot clear cache while preview rebuild is running"}, 409
    metadata_status = get_metadata_index_status_snapshot()
    if is_metadata_index_active(metadata_status):
        return {"error": "Cannot clear cache while metadata indexing is running"}, 409
    ai_status = get_ai_analysis_status_snapshot()
    if is_ai_analysis_active(ai_status):
        return {"error": "Cannot clear cache while AI vision pass is running"}, 409
    try:
        deleted_files = clear_thumbnail_cache()
    except Exception as exc:
        log_event("thumbnail_cache_clear_failure", "Thumbnail cache clear failed", error=str(exc))
        return {"error": f"Failed to clear cache: {exc}"}, 500
    return {"ok": True, "deleted_files": deleted_files}


def get_requested_dir_from_payload():
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        value = payload.get("dir")
        if isinstance(value, str):
            return value
    value = request.form.get("dir")
    if value is not None:
        return value
    return request.args.get("dir", "")


@app.route("/api/cache/rebuild-previews", methods=["POST"])
def api_rebuild_previews():
    status = get_rebuild_status_snapshot()
    if is_rebuild_status_active(status):
        return {"ok": False, "error": "A preview rebuild is already running", "status": status}, 409
    metadata_status = get_metadata_index_status_snapshot()
    if is_metadata_index_active(metadata_status):
        return {"ok": False, "error": "Metadata indexing is already running", "status": metadata_status}, 409
    ai_status = get_ai_analysis_status_snapshot()
    if is_ai_analysis_active(ai_status):
        return {"ok": False, "error": "AI vision pass is already running", "status": ai_status}, 409
    requested_dir = get_requested_dir_from_payload()
    payload = request.get_json(silent=True)
    force_rebuild = bool(payload.get("force")) if isinstance(payload, dict) else False
    try:
        scope_path = resolve_safe_path(requested_dir)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    if not scope_path.exists() or not scope_path.is_dir():
        return {"ok": False, "error": "Directory not found"}, 404
    if not rebuild_previews_lock.acquire(blocking=False):
        return {"ok": False, "error": "A preview rebuild is already running", "status": get_rebuild_status_snapshot(prefer_disk=False)}, 409
    scope_dir = rel_from_root(scope_path) if scope_path != BROWSE_ROOT else ""
    scope_label = format_scope_label(scope_path)

    update_rebuild_status(
        state="queued",
        phase="queued",
        progress_pct=0,
        completed_tasks=0,
        total_tasks=0,
        folders_scanned=0,
        folders_with_pngs=0,
        image_count=0,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_rebuild=force_rebuild,
        started_at=utc_now_iso(),
        finished_at=None,
        error=None,
        summary=None,
    )
    thread = threading.Thread(target=rebuild_previews_worker, args=(scope_dir, force_rebuild), name="rebuild-previews", daemon=True)
    thread.start()
    log_event("thumbnail_prewarm_rebuild_started", "Rebuild previews started in background", scope_dir=scope_dir, scope_label=scope_label, force_rebuild=force_rebuild)
    return {"ok": True, "started": True, "status": get_rebuild_status_snapshot()}


@app.route("/api/cache/rebuild-previews/cancel", methods=["POST"])
def api_cancel_rebuild_previews():
    status = get_rebuild_status_snapshot()
    if not is_rebuild_status_active(status):
        return {"ok": False, "error": "No preview rebuild is currently running", "status": status}, 409
    if is_rebuild_cancel_requested(status):
        return {"ok": True, "cancel_requested": True, "status": status}

    update_rebuild_status(
        cancel_requested=True,
        phase="cancel-requested",
        error="Cancel requested; stopping after the current directory",
    )
    updated_status = get_rebuild_status_snapshot()
    log_event("thumbnail_prewarm_rebuild_cancel_requested", "Rebuild previews cancellation requested")
    return {"ok": True, "cancel_requested": True, "status": updated_status}


@app.route("/api/cache/rebuild-previews/status")
def api_rebuild_previews_status():
    return get_rebuild_status_snapshot()


@app.route("/api/metadata-index/rebuild", methods=["POST"])
def api_rebuild_metadata_index():
    status = get_metadata_index_status_snapshot()
    if is_metadata_index_active(status):
        return {"ok": False, "error": "A metadata index rebuild is already running", "status": status}, 409
    preview_status = get_rebuild_status_snapshot()
    if is_rebuild_status_active(preview_status):
        return {"ok": False, "error": "A preview rebuild is already running", "status": preview_status}, 409
    ai_status = get_ai_analysis_status_snapshot()
    if is_ai_analysis_active(ai_status):
        return {"ok": False, "error": "An AI vision pass is already running", "status": ai_status}, 409

    requested_dir = get_requested_dir_from_payload()
    payload = request.get_json(silent=True)
    force_reindex = bool(payload.get("force")) if isinstance(payload, dict) else False
    try:
        scope_path = resolve_safe_path(requested_dir)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    if not scope_path.exists() or not scope_path.is_dir():
        return {"ok": False, "error": "Directory not found"}, 404

    if not metadata_index_lock.acquire(blocking=False):
        return {"ok": False, "error": "A metadata index rebuild is already running", "status": get_metadata_index_status_snapshot(prefer_disk=False)}, 409

    scope_dir = rel_from_root(scope_path) if scope_path != BROWSE_ROOT else ""
    scope_label = format_scope_label(scope_path)
    update_metadata_index_status(
        state="queued",
        phase="queued",
        progress_pct=0,
        completed_tasks=0,
        total_tasks=0,
        folders_scanned=0,
        folders_with_pngs=0,
        image_count=0,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_reindex=force_reindex,
        started_at=utc_now_iso(),
        finished_at=None,
        error=None,
        summary=None,
    )
    thread = threading.Thread(target=rebuild_metadata_index_worker, args=(scope_dir, force_reindex), name="rebuild-metadata-index", daemon=True)
    thread.start()
    log_event("metadata_index_rebuild_started", "Metadata index rebuild started in background", scope_dir=scope_dir, scope_label=scope_label, force_reindex=force_reindex)
    return {"ok": True, "started": True, "status": get_metadata_index_status_snapshot()}


@app.route("/api/metadata-index/rebuild/cancel", methods=["POST"])
def api_cancel_rebuild_metadata_index():
    status = get_metadata_index_status_snapshot()
    if not is_metadata_index_active(status):
        return {"ok": False, "error": "No metadata index rebuild is currently running", "status": status}, 409
    if is_metadata_index_cancel_requested(status):
        return {"ok": True, "cancel_requested": True, "status": status}

    update_metadata_index_status(
        cancel_requested=True,
        phase="cancel-requested",
        error="Cancel requested; stopping after the current directory",
    )
    updated_status = get_metadata_index_status_snapshot()
    log_event("metadata_index_rebuild_cancel_requested", "Metadata index rebuild cancellation requested", scope_dir=updated_status.get("scope_dir") or "")
    return {"ok": True, "cancel_requested": True, "status": updated_status}


@app.route("/api/metadata-index/rebuild/status")
def api_rebuild_metadata_index_status():
    return get_metadata_index_status_snapshot()


@app.route("/api/ai-analysis/rebuild", methods=["POST"])
def api_rebuild_ai_analysis():
    if not ai_analysis_is_configured():
        return {"ok": False, "error": "AI analysis is not configured"}, 409

    status = get_ai_analysis_status_snapshot()
    if is_ai_analysis_active(status):
        return {"ok": False, "error": "An AI vision pass is already running", "status": status}, 409
    preview_status = get_rebuild_status_snapshot()
    if is_rebuild_status_active(preview_status):
        return {"ok": False, "error": "A preview rebuild is already running", "status": preview_status}, 409
    metadata_status = get_metadata_index_status_snapshot()
    if is_metadata_index_active(metadata_status):
        return {"ok": False, "error": "A metadata index rebuild is already running", "status": metadata_status}, 409

    requested_dir = get_requested_dir_from_payload()
    payload = request.get_json(silent=True)
    force_reanalyze = bool(payload.get("force")) if isinstance(payload, dict) else False
    try:
        scope_path = resolve_safe_path(requested_dir)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    if not scope_path.exists() or not scope_path.is_dir():
        return {"ok": False, "error": "Directory not found"}, 404

    if not ai_analysis_lock.acquire(blocking=False):
        return {"ok": False, "error": "An AI vision pass is already running", "status": get_ai_analysis_status_snapshot(prefer_disk=False)}, 409

    scope_dir = rel_from_root(scope_path) if scope_path != BROWSE_ROOT else ""
    scope_label = format_scope_label(scope_path)
    update_ai_analysis_status(
        state="queued",
        phase="queued",
        progress_pct=0,
        completed_tasks=0,
        total_tasks=0,
        folders_scanned=0,
        folders_with_pngs=0,
        image_count=0,
        current_directory=None,
        current_file=None,
        scope_dir=scope_dir,
        scope_label=scope_label,
        force_rebuild=force_reanalyze,
        started_at=utc_now_iso(),
        finished_at=None,
        error=None,
        summary=None,
    )
    thread = threading.Thread(target=rebuild_ai_analysis_worker, args=(scope_dir, force_reanalyze), name="rebuild-ai-analysis", daemon=True)
    thread.start()
    log_event("ai_analysis_rebuild_started", "AI vision pass started in background", scope_dir=scope_dir, scope_label=scope_label, force_rebuild=force_reanalyze, thumbnail_mode="full", thumbnail_size=THUMB_SIZE_FULL)
    return {"ok": True, "started": True, "status": get_ai_analysis_status_snapshot()}


@app.route("/api/ai-analysis/rebuild/cancel", methods=["POST"])
def api_cancel_rebuild_ai_analysis():
    status = get_ai_analysis_status_snapshot()
    if not is_ai_analysis_active(status):
        return {"ok": False, "error": "No AI vision pass is currently running", "status": status}, 409
    if is_ai_analysis_cancel_requested(status):
        return {"ok": True, "cancel_requested": True, "status": status}

    update_ai_analysis_status(
        cancel_requested=True,
        phase="cancel-requested",
        error="Cancel requested; stopping after the current directory",
    )
    updated_status = get_ai_analysis_status_snapshot()
    log_event("ai_analysis_rebuild_cancel_requested", "AI vision pass cancellation requested", scope_dir=updated_status.get("scope_dir") or "")
    return {"ok": True, "cancel_requested": True, "status": updated_status}


@app.route("/api/ai-analysis/rebuild/status")
def api_rebuild_ai_analysis_status():
    return get_ai_analysis_status_snapshot()


@app.route("/api/ai-analysis/image", methods=["POST"])
def api_run_ai_analysis_for_image():
    if not ai_analysis_is_configured():
        return {"ok": False, "error": "AI analysis is not configured"}, 409
    if is_rebuild_status_active(get_rebuild_status_snapshot()):
        return {"ok": False, "error": "A preview rebuild is currently running"}, 409
    if is_metadata_index_active(get_metadata_index_status_snapshot()):
        return {"ok": False, "error": "A metadata index rebuild is currently running"}, 409
    if is_ai_analysis_active(get_ai_analysis_status_snapshot()):
        return {"ok": False, "error": "An AI vision pass is currently running"}, 409

    payload = request.get_json(silent=True) or {}
    rel_file = str(payload.get("file") or "").strip()
    if not rel_file:
        return {"ok": False, "error": "Missing 'file' parameter"}, 400

    try:
        path = resolve_safe_path(rel_file)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400

    if should_exclude_png_path(path):
        return {"ok": False, "error": "This PNG variant is hidden by filter rules"}, 404
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".png":
        return {"ok": False, "error": "Not a valid PNG file"}, 400

    log_event("ai_analysis_single_image_started", "Single-image AI vision pass started", file=rel_file, thumbnail_mode="full", thumbnail_size=THUMB_SIZE_FULL, force_rebuild=True)

    try:
        record = run_ai_analysis_for_path(path)
        conn = metadata_index.connect(METADATA_DB_PATH)
        try:
            metadata_index.initialize(conn)
            metadata_index.upsert_file_record(conn, record)
            conn.commit()
            saved = metadata_index.get_file_record(conn, rel_file)
        finally:
            conn.close()
    except Exception as exc:
        log_event("ai_analysis_single_image_failure", "Single-image AI vision pass failed", file=rel_file, error=str(exc))
        return {"ok": False, "error": f"Failed to run AI vision pass: {exc}"}, 500

    log_event("ai_analysis_single_image_completed", "Single-image AI vision pass completed", file=rel_file, thumbnail_mode="full", thumbnail_size=THUMB_SIZE_FULL, force_rebuild=True)
    return {
        "ok": True,
        "file": rel_file,
        "ai_analysis": build_ai_analysis_response(saved) if saved else None,
    }


@app.route("/api/logs")
def api_logs():
    limit = request.args.get("limit", "200")
    try:
        parsed_limit = max(1, min(int(limit), 500))
    except ValueError:
        parsed_limit = 200
    return {"entries": read_recent_logs(parsed_limit), "log_path": str(APP_LOG_PATH)}


@app.route("/api/logs/client", methods=["POST"])
def api_client_log():
    payload = request.get_json(silent=True) or {}
    event_type = str(payload.get("event") or "").strip().lower()
    message = str(payload.get("message") or "").strip()
    fields = payload.get("fields") or {}

    if not event_type.startswith("gallery_"):
        return {"ok": False, "error": "Unsupported client log event"}, 400
    if not isinstance(fields, dict):
        return {"ok": False, "error": "Invalid client log fields"}, 400

    sanitized_fields = {}
    for key, value in fields.items():
        safe_key = str(key or "").strip()
        if not safe_key:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            sanitized_fields[safe_key] = value
        else:
            sanitized_fields[safe_key] = str(value)

    log_event(event_type, message or event_type.replace("_", " "), **sanitized_fields)
    return {"ok": True}


@app.route("/api/logs/clear", methods=["POST"])
def api_clear_logs():
    try:
        deleted_entries = clear_logs()
    except Exception as exc:
        return {"error": f"Failed to clear logs: {exc}"}, 500
    return {"ok": True, "deleted_entries": deleted_entries}


@app.route("/favicon.ico")
def favicon():
    return send_file(Path(app.static_folder) / "favicon.svg", mimetype="image/svg+xml", conditional=True)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
