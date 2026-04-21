import base64
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_MODEL = os.getenv("AI_ANALYSIS_MODEL", "gpt-4.1-mini")
DEFAULT_DETAIL = os.getenv("AI_ANALYSIS_DETAIL", "low").strip().lower() or "low"
DEFAULT_PROMPT_VERSION = os.getenv("AI_ANALYSIS_PROMPT_VERSION", "v1").strip() or "v1"
DEFAULT_BASE_URL = (os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1") or "https://api.openai.com/v1").rstrip("/")

SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "summary",
        "tags",
    ],
}


class AIAnalysisError(RuntimeError):
    pass


def normalize_text(value):
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return str(value).strip() or None


def normalize_tag_list(values):
    if not isinstance(values, list):
        return []
    normalized = []
    seen = set()
    for item in values:
        text = normalize_text(item)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def join_tag_text(values):
    normalized = normalize_tag_list(values)
    return "\n".join(normalized) if normalized else None


def clamp_score(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return 0.0
    if parsed > 1:
        return 1.0
    return parsed


def build_ai_analysis_input(parsed_metadata, rel_path):
    parsed_metadata = parsed_metadata or {}
    summary = parsed_metadata.get("summary") or {}
    prompt = parsed_metadata.get("prompt") or {}
    workflow = parsed_metadata.get("workflow") or {}

    manual = summary.get("manual_overrides") or {}
    quad = summary.get("quad") or summary.get("quad_selections") or {}
    models = summary.get("models") or {}

    def clean_map(source, keys):
        payload = {}
        for key in keys:
            value = normalize_text(source.get(key)) if isinstance(source, dict) else None
            if value:
                payload[key] = value
        return payload

    return {
        "rel_path": normalize_text(rel_path),
        "image": {
            "width": ((parsed_metadata.get("image") or {}).get("width")),
            "height": ((parsed_metadata.get("image") or {}).get("height")),
        },
        "prompt_text": normalize_text(summary.get("final_prompt")),
        "negative_prompt_text": None,
        "manual_overrides": clean_map(
            manual,
            [
                "style_override",
                "location_override",
                "character_override",
                "pose_override",
                "main_prompt",
                "additional_keywords",
            ],
        ),
        "quad_selections": clean_map(
            quad,
            ["style", "location", "character", "pose"],
        ),
        "generation_summary": clean_map(
            summary,
            ["resolution", "lora_prefix"],
        ),
        "models": clean_map(
            models,
            ["unet_model", "text_encoder_model", "vae_model"],
        ),
        "has_prompt_json": isinstance(prompt, dict) and bool(prompt),
        "has_workflow_json": isinstance(workflow, dict) and bool(workflow),
    }


def _prompt_text(metadata_payload, prompt_version):
    lines = [
        "You are analyzing a PNG render for indexing and retrieval.\n"
        "Return strict JSON only.\n"
        "Do not add markdown.\n"
        "Prefer literal visible facts and normalized tags.\n"
        "Return exactly two fields: summary and tags.\n"
        "The summary must be one cohesive, detailed description of the visible image in prompt-like prose for search retrieval.\n"
        "Describe the visible subject matter, clothing, objects, environment, background details, actions, pose, facial expression, materials, and notable visual details when present.\n"
        "Do not split the description into headings or bullet points.\n"
        "Do not include confidence, quality judgments, prompt critique, or technical photography analysis.\n"
        "The tags array must contain concise, meaningful search tags directly grounded in visible image content.\n"
        "Select tags independently from the summary instead of merely rephrasing summary sentences.\n"
        "Tags should be useful retrieval terms, not generic filler.\n"
        "Return between 8 and 18 tags when possible.\n"
        "Each tag should usually be 1 to 3 words, and should never be a full sentence or long clause.\n"
        "Prefer distinctive visible subjects, objects, environments, materials, colors, and scene elements when they are helpful for retrieval.\n"
        "Do not force categories that are not present in the image.\n"
        "If the image is a landscape, use landscape-relevant tags; if it is a character scene, use character-scene tags; if it is abstract or object-focused, use only what is visibly present.\n"
        "Avoid low-value filler tags like beautiful, nice, detailed, artwork, image, or similar generic terms unless they are truly essential to retrieval.\n"
        f"Prompt version: {prompt_version}\n"
        "Expected schema keys: summary, tags.\n\n"
        "Use only the visual content in the image. Do not infer from unseen metadata or filename.\n"
    ]

    payload = metadata_payload if isinstance(metadata_payload, dict) else {}
    prompt_text = normalize_text(payload.get("prompt_text"))
    negative_prompt_text = normalize_text(payload.get("negative_prompt_text"))
    manual = payload.get("manual_overrides") if isinstance(payload.get("manual_overrides"), dict) else {}
    quad = payload.get("quad_selections") if isinstance(payload.get("quad_selections"), dict) else {}

    return "".join(lines)


def _image_data_url(image_path):
    mime = "image/webp"
    if image_path.suffix.lower() == ".png":
        mime = "image/png"
    with image_path.open("rb") as fh:
        encoded = base64.b64encode(fh.read()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _extract_message_text(message_content):
    if isinstance(message_content, str):
        return message_content
    if not isinstance(message_content, list):
        return None
    parts = []
    for item in message_content:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text" and isinstance(item.get("text"), str):
            parts.append(item["text"])
    text = "\n".join(part for part in parts if part.strip())
    return text or None


def _extract_json_text(payload):
    if not isinstance(payload, dict):
        return None
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        message = choices[0].get("message") or {}
        text = _extract_message_text(message.get("content"))
        if text:
            return text
    output = payload.get("output")
    if isinstance(output, list):
        parts = []
        for item in output:
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") in {"output_text", "text"} and isinstance(block.get("text"), str):
                    parts.append(block["text"])
        text = "\n".join(part for part in parts if part.strip())
        if text:
            return text
    if isinstance(payload.get("text"), str):
        return payload["text"]
    return None


def call_openai_analysis(image_path, metadata_payload, model, detail_level, prompt_version, api_key=None, base_url=None, timeout=120):
    api_key = normalize_text(api_key or os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise AIAnalysisError("OPENAI_API_KEY is not configured")

    target_base = (base_url or DEFAULT_BASE_URL).rstrip("/")
    body = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _prompt_text(metadata_payload, prompt_version)},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": _image_data_url(image_path),
                            "detail": detail_level,
                        },
                    },
                ],
            }
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "png_ai_analysis",
                "schema": SCHEMA,
                "strict": True,
            },
        },
    }
    req = urllib.request.Request(
        target_base + "/chat/completions",
        method="POST",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AIAnalysisError(f"OpenAI request failed with HTTP {exc.code}: {detail[:500]}")
    except urllib.error.URLError as exc:
        raise AIAnalysisError(f"OpenAI request failed: {exc.reason}")

    text = _extract_json_text(raw)
    if not text:
        raise AIAnalysisError("OpenAI response did not contain JSON content")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise AIAnalysisError(f"OpenAI response was not valid JSON: {exc}")


def normalize_ai_response(raw_response):
    if not isinstance(raw_response, dict):
        raise AIAnalysisError("AI response must be a JSON object")
    normalized = {
        "summary": normalize_text(raw_response.get("summary")),
        "tags": normalize_tag_list(raw_response.get("tags")),
    }
    if not normalized["summary"]:
        raise AIAnalysisError("AI response did not include a summary")
    return normalized


def build_ai_record(rel_path, stat_result, normalized_response, model, detail_level, prompt_version, analyzed_at):
    return {
        "file_path": rel_path,
        "source_mtime_ns": int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))),
        "source_size_bytes": int(stat_result.st_size),
        "analyzed_at": analyzed_at,
        "model": normalize_text(model),
        "detail_level": normalize_text(detail_level),
        "prompt_version": normalize_text(prompt_version),
        "summary": normalized_response.get("summary"),
        "tags_text": join_tag_text(normalized_response.get("tags")),
        "raw_json": json.dumps(normalized_response, ensure_ascii=True, sort_keys=True),
    }


def analyze_image(image_path, metadata_payload, model=None, detail_level=None, prompt_version=None, api_key=None, base_url=None):
    raw = call_openai_analysis(
        image_path=image_path,
        metadata_payload=metadata_payload,
        model=model or DEFAULT_MODEL,
        detail_level=detail_level or DEFAULT_DETAIL,
        prompt_version=prompt_version or DEFAULT_PROMPT_VERSION,
        api_key=api_key,
        base_url=base_url,
    )
    return normalize_ai_response(raw)
