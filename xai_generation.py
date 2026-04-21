"""xAI Grok Imagine API integration for image editing and video generation."""

import base64
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


XAI_BASE_URL = "https://api.x.ai/v1"

DEFAULT_IMAGE_MODEL = "grok-imagine-image"
DEFAULT_VIDEO_MODEL = "grok-imagine-video"
DEFAULT_IMAGE_QUALITY = os.getenv("XAI_IMAGE_QUALITY", "high").strip().lower() or "high"
DEFAULT_IMAGE_RESOLUTION = os.getenv("XAI_IMAGE_RESOLUTION", "2k").strip().lower() or "2k"
DEFAULT_VIDEO_DURATION = int(os.getenv("XAI_VIDEO_DURATION", "10"))
DEFAULT_VIDEO_RESOLUTION = os.getenv("XAI_VIDEO_RESOLUTION", "720p").strip().lower() or "720p"


class XAIGenerationError(RuntimeError):
    pass


def _get_api_key():
    key = (os.getenv("XAI_API_KEY") or "").strip().lstrip("=")
    if not key:
        raise XAIGenerationError("XAI_API_KEY is not configured")
    return key


def _image_to_data_url(image_path: Path) -> str:
    suffix = image_path.suffix.lower()
    mime_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp"}
    mime = mime_map.get(suffix, "image/png")
    with image_path.open("rb") as fh:
        encoded = base64.b64encode(fh.read()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _post_json(url: str, body: dict, api_key: str, timeout: int = 120) -> dict:
    req = urllib.request.Request(
        url,
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise XAIGenerationError(f"xAI request failed with HTTP {exc.code}: {detail[:500]}")
    except urllib.error.URLError as exc:
        raise XAIGenerationError(f"xAI request failed: {exc.reason}")


def _get_json(url: str, api_key: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise XAIGenerationError(f"xAI request failed with HTTP {exc.code}: {detail[:500]}")
    except urllib.error.URLError as exc:
        raise XAIGenerationError(f"xAI request failed: {exc.reason}")


def _download_url(url: str, dest: Path, timeout: int = 120):
    req = urllib.request.Request(url, method="GET")
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            with dest.open("wb") as fh:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    fh.write(chunk)
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        try:
            dest.unlink(missing_ok=True)
        except OSError:
            pass
        raise XAIGenerationError(f"Failed to download result: {exc}")


def edit_image(image_path: Path, prompt: str, quality: str = None, resolution: str = None) -> dict:
    """Send an image + prompt to xAI for editing. Returns response with image URL or b64."""
    api_key = _get_api_key()
    data_url = _image_to_data_url(image_path)

    body = {
        "model": DEFAULT_IMAGE_MODEL,
        "prompt": prompt,
        "image": {"url": data_url},
        "n": 1,
        "quality": quality or DEFAULT_IMAGE_QUALITY,
        "resolution": resolution or DEFAULT_IMAGE_RESOLUTION,
        "response_format": "b64_json",
    }

    result = _post_json(f"{XAI_BASE_URL}/images/edits", body, api_key, timeout=180)

    data_list = result.get("data")
    if not isinstance(data_list, list) or not data_list:
        raise XAIGenerationError("xAI response did not contain image data")

    return {
        "b64_json": data_list[0].get("b64_json"),
        "url": data_list[0].get("url"),
        "mime_type": data_list[0].get("mime_type", "image/png"),
        "revised_prompt": data_list[0].get("revised_prompt", ""),
    }


def generate_video(image_path: Path, prompt: str, duration: int = None, resolution: str = None) -> str:
    """Submit image-to-video generation. Returns the request_id for polling."""
    api_key = _get_api_key()
    data_url = _image_to_data_url(image_path)

    body = {
        "model": DEFAULT_VIDEO_MODEL,
        "prompt": prompt,
        "image": {"url": data_url},
        "duration": duration or DEFAULT_VIDEO_DURATION,
        "resolution": resolution or DEFAULT_VIDEO_RESOLUTION,
    }

    result = _post_json(f"{XAI_BASE_URL}/videos/generations", body, api_key, timeout=120)

    request_id = result.get("request_id")
    if not request_id:
        raise XAIGenerationError("xAI response did not contain a request_id")
    return request_id


def poll_video_status(request_id: str) -> dict:
    """Poll the status of a video generation job. Returns status dict."""
    api_key = _get_api_key()
    result = _get_json(f"{XAI_BASE_URL}/videos/{request_id}", api_key)
    status = result.get("status", "unknown")
    video_info = result.get("video") or {}
    return {
        "status": status,
        "video_url": video_info.get("url"),
        "duration": video_info.get("duration"),
    }


def save_edited_image(b64_data: str, dest_path: Path, mime_type: str = "image/png"):
    """Decode base64 image data and save to disk."""
    raw = base64.b64decode(b64_data)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("wb") as fh:
        fh.write(raw)


def download_video(video_url: str, dest_path: Path):
    """Download a video from a URL to a local path."""
    _download_url(video_url, dest_path)
