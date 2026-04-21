"""Sensor realism effects ported from the ComfyUI Minx nodes.

PIL-in, PIL-out. Depends only on Pillow + numpy — no torch, no GPU.
"""

from __future__ import annotations

import numpy as np
from PIL import Image, ImageFilter


def apply_phone_look(
    image: Image.Image,
    shadow_lift: float = 0.15,
    highlight_compress: float = 0.10,
    local_tone_map: float = 0.40,
    local_radius_pct: float = 8.0,
    saturation_boost: float = 1.15,
    warmth: float = 0.02,
) -> Image.Image:
    """Flat, computational-photography 'phone' look.

    Sequence: local tone map → shadow lift + highlight compress →
    gentle S-curve → saturation boost → warmth shift.
    """
    src_mode = image.mode
    alpha_band = image.split()[-1] if src_mode in ("RGBA", "LA") else None
    rgb = image.convert("RGB")

    arr = np.asarray(rgb, dtype=np.float32) / 255.0
    h, w = arr.shape[:2]

    if local_tone_map > 0:
        # Gaussian blur acts as the local-mean estimator. ComfyUI node
        # used sigma = radius_px / 3; Pillow's GaussianBlur radius is
        # interpreted as sigma, so pass the same value.
        radius_px = max(3.0, min(h, w) * float(local_radius_pct) / 100.0)
        sigma = radius_px / 3.0
        local_mean_img = rgb.filter(ImageFilter.GaussianBlur(radius=sigma))
        local_mean = np.asarray(local_mean_img, dtype=np.float32) / 255.0
        arr = arr * (1.0 - local_tone_map) + local_mean * local_tone_map

    low = float(shadow_lift)
    high = 1.0 - float(highlight_compress)
    if high > low:
        arr = low + arr * (high - low)

    arr = np.clip(arr, 0.0, 1.0)
    s_strength = 0.15
    s_curve = 3.0 * arr ** 2 - 2.0 * arr ** 3
    arr = arr * (1.0 - s_strength) + s_curve * s_strength

    if saturation_boost != 1.0:
        luma = (
            0.2126 * arr[:, :, 0]
            + 0.7152 * arr[:, :, 1]
            + 0.0722 * arr[:, :, 2]
        )[:, :, None]
        arr = luma + (arr - luma) * float(saturation_boost)

    if abs(float(warmth)) > 0.001:
        arr[:, :, 0] = arr[:, :, 0] + float(warmth)
        arr[:, :, 2] = arr[:, :, 2] - float(warmth) * 0.7

    arr = np.clip(arr, 0.0, 1.0)
    out_rgb = Image.fromarray((arr * 255.0).round().astype(np.uint8), mode="RGB")

    if alpha_band is not None:
        out = out_rgb.convert("RGBA")
        out.putalpha(alpha_band)
        return out
    return out_rgb
