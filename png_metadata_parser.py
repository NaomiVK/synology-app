import json
from pathlib import Path

from PIL import Image


def safe_get(values, index, default=None):
    if values and index < len(values):
        value = values[index]
        if value is not None:
            return value
    return default


class PNGMetadataParser:
    """Standalone parser for PNG metadata, including ComfyUI workflow metadata."""

    def parse_png(self, file_path, stat_result=None):
        path = Path(file_path)
        st = stat_result or path.stat()
        with Image.open(path) as img:
            info = dict(img.info or {})
            width, height = img.size

        workflow = self._load_json_field(info.get("workflow"))
        prompt = self._load_json_field(info.get("prompt"))

        workflow_with_extras = None
        if isinstance(workflow, dict):
            workflow_with_extras = dict(workflow)
            if "minx_lora_weight" in info:
                try:
                    workflow_with_extras["_minx_lora_weight"] = float(info["minx_lora_weight"])
                except (TypeError, ValueError):
                    pass

        summary = self._extract_summary(workflow_with_extras or {})

        return {
            "image": {
                "filename": path.name,
                "size_bytes": st.st_size,
                "width": width,
                "height": height,
            },
            "png_info": self._sanitize_for_json(info),
            "workflow": workflow,
            "prompt": prompt,
            "summary": summary,
        }

    def _load_json_field(self, value):
        if not value:
            return None
        try:
            return json.loads(value)
        except Exception:
            return None

    def _build_workflow_context(self, workflow):
        nodes = workflow.get("nodes", [])
        links = workflow.get("links", [])
        nodes_by_type = {}
        nodes_by_title = {}
        nodes_by_id = {}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_type = node.get("type")
            if node_type is not None:
                nodes_by_type.setdefault(node_type, []).append(node)
            title = node.get("title")
            if title:
                nodes_by_title.setdefault(title, []).append(node)
            prop_title = (node.get("properties") or {}).get("title")
            if prop_title:
                nodes_by_title.setdefault(prop_title, []).append(node)
            node_id = node.get("id")
            if node_id is not None:
                nodes_by_id[node_id] = node

        link_map = {}
        for link in links:
            if len(link) >= 5:
                link_id, _from_node, _from_slot, to_node, _to_slot = link[:5]
                link_map[link_id] = to_node

        return {
            "nodes": nodes,
            "nodes_by_type": nodes_by_type,
            "nodes_by_title": nodes_by_title,
            "nodes_by_id": nodes_by_id,
            "link_map": link_map,
        }

    def _find_nodes_by_type(self, workflow, node_type, ctx=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)
        return list(ctx["nodes_by_type"].get(node_type, []))

    def _find_node_by_title(self, workflow, title, ctx=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)
        matches = ctx["nodes_by_title"].get(title, [])
        return matches[0] if matches else None

    def _get_node_title(self, node):
        if not isinstance(node, dict):
            return ""
        return str(node.get("title") or (node.get("properties") or {}).get("title") or "")

    def _get_widgets(self, node):
        return node.get("widgets_values", [])

    def _node_is_bypassed(self, node):
        """Best-effort bypass detection for ComfyUI/LiteGraph nodes."""
        mode = node.get("mode")
        if mode in (2, 4):  # common muted/bypass modes seen in workflows
            return True

        flags = node.get("flags") or {}
        for key in ("bypassed", "bypass", "disabled", "muted"):
            if bool(flags.get(key)):
                return True

        return False

    def _extract_post_processing_cards(self, workflow, ctx=None):
        """Extract known post-processing nodes into UI-friendly cards.

        Only includes nodes that exist and are not bypassed.
        """
        specs = [
            {
                "type_match": "minx lens blur",
                "title": "Lens Blur",
                "fields": [
                    "strength", "focus_origin", "shape", "direction", "direction_angle",
                    "focus_size", "feather", "use_face", "blur_enabled", "blur_strength",
                    "ca_enabled", "ca_strength",
                ],
            },
            {
                "type_match": "minx vignette",
                "title": "Vignette",
                "fields": [
                    "strength", "focus_origin", "shape", "direction", "direction_angle",
                    "focus_size", "feather", "use_face", "blur_enabled", "blur_strength",
                    "bleed_highlights", "bleed_strength", "dither_enabled", "dither_amount",
                    "dither_grain_size", "dither_monochrome", "dither_before_blur",
                ],
            },
            {
                "type_match": "minx halation",
                "title": "Halation",
                "fields": [
                    "preset", "halation_enabled", "hal_threshold", "hal_radius_scale",
                    "hal_amount", "hal_tint_r", "hal_tint_g", "hal_tint_b",
                    "hal_edge_bias", "hal_highlight_rolloff",
                ],
            },
            {
                "type_match": "minx light leaks",
                "title": "Light Leaks",
                "fields": [
                    "preset", "randomize_preset", "use_image_hues", "strength", "blend",
                    "angle_deg", "randomize_angle", "leak_count", "light_leak_hotspots",
                    "streak_width_pct", "streak_softness", "length_pct_min", "length_pct_max",
                    "length_softness", "seed", "control_after_generate",
                ],
            },
            {
                "type_match": "minx orton effect",
                "title": "Orton Effect",
                "fields": ["sharp_mix", "blur_strength", "brighten"],
            },
            {
                "type_match": "minx sharpen",
                "title": "Sharpen",
                "fields": [
                    "strength", "sharpening_mode", "focus_origin", "shape", "direction",
                    "direction_angle", "focus_size", "feather", "use_face", "noise_radius",
                    "preserve_edges", "sharpen", "center_ratio", "edge_ratio", "fullframe_ratio",
                ],
            },
            {
                "type_match": "minx matte black",
                "title": "Matte Black",
                "fields": ["matte_level", "s_curve", "clamp_whites"],
            },
            {
                "type_match": "minx vibrance + saturation",
                "title": "Vibrance + Saturation",
                "fields": [
                    "vibrance", "saturation", "skin_protection", "neutral_bias",
                    "highlight_protect", "preserve_luminance", "gamut_mode",
                ],
            },
            {
                "type_match": "minx film noise",
                "title": "Film Noise",
                "fields": [
                    "amount", "grain_size", "grain_size_mode", "grain_size_rel_pct",
                    "monochrome", "chroma_scale", "clumpiness", "shadow_power",
                    "shadow_threshold", "shadow_knee", "hi_start", "hi_end",
                    "film_stock", "mode", "global_weight", "seed", "control_after_generate",
                ],
            },
        ]

        if ctx is None:
            ctx = self._build_workflow_context(workflow)
        nodes = ctx["nodes"]
        cards = []
        for spec in specs:
            type_match = spec["type_match"]
            matching_nodes = []
            for node in nodes:
                node_type = str(node.get("type", "")).lower()
                if type_match in node_type:
                    matching_nodes.append(node)

            for idx_node, node in enumerate(matching_nodes, start=1):
                if self._node_is_bypassed(node):
                    continue

                widgets = self._get_widgets(node)
                fields = []
                for idx, label in enumerate(spec["fields"]):
                    if idx >= len(widgets):
                        break
                    value = widgets[idx]
                    if value is None:
                        continue
                    fields.append({
                        "label": label,
                        "value": value,
                    })

                if not fields:
                    continue

                title = spec["title"]
                if len(matching_nodes) > 1:
                    title = f"{title} {idx_node}"

                cards.append({
                    "node_type": node.get("type"),
                    "title": title,
                    "fields": fields,
                })

        return cards

    def _field_has_value(self, value):
        if value is None:
            return False
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return False
            if text.lower() == "none":
                return False
        return True

    def _append_fields_from_specs(self, fields, values, specs):
        for label, idx in specs:
            value = safe_get(values, idx, None)
            if not self._field_has_value(value):
                continue
            fields.append({"label": label, "value": value})

    def _extract_face_detailer_cards(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "FaceDetailer", ctx)
        cards = []
        for idx_node, node in enumerate(nodes, start=1):
            if self._node_is_bypassed(node):
                continue

            w = self._get_widgets(node)
            if not isinstance(w, list) or not w:
                continue

            fields = []
            # Mapped from the provided workflow export (Impact-Pack FaceDetailer widget order).
            self._append_fields_from_specs(
                fields,
                w,
                [
                    ("guide_size", 0),
                    ("guide_size_for", 1),
                    ("max_size", 2),
                    ("seed", 3),
                    ("control_after_generate", 4),
                    ("steps", 5),
                    ("cfg", 6),
                    ("sampler_name", 7),
                    ("scheduler", 8),
                    ("denoise", 9),
                    ("feather", 10),
                    ("noise_mask", 11),
                    ("force_inpaint", 12),
                    ("bbox_threshold", 13),
                    ("bbox_dilation", 14),
                    ("bbox_crop_factor", 15),
                    ("sam_detection_hint", 16),
                    ("sam_dilation", 17),
                    ("sam_threshold", 18),
                    ("sam_bbox_expansion", 19),
                    ("sam_mask_hint_threshold", 20),
                    ("sam_mask_hint_use_negative", 21),
                    ("drop_size", 22),
                    ("wildcard", 23),
                ],
            )

            for field in fields:
                if field["label"] == "guide_size_for" and isinstance(field["value"], bool):
                    field["value"] = "bbox" if field["value"] else "crop_region"

            if not fields:
                continue

            title = "FaceDetailer" if len(nodes) == 1 else f"FaceDetailer {idx_node}"
            cards.append({"title": title, "fields": fields})

        return cards

    def _extract_character_logic_card(self, workflow, ctx=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)
        logic_node = next(
            (n for n in ctx["nodes"] if str(n.get("type", "")).lower() == "minx character logic (minx)".lower()),
            None,
        )
        loader_node = next(
            (n for n in ctx["nodes"] if str(n.get("type", "")).lower() == "minx character loader (minx)".lower()),
            None,
        )

        sections = []

        if logic_node and not self._node_is_bypassed(logic_node):
            w = self._get_widgets(logic_node)
            logic_fields = []
            self._append_fields_from_specs(
                logic_fields,
                w,
                [
                    ("character", 0),
                    ("use_char_1", 1),
                    ("use_char_2", 2),
                    ("fd_use_char1", 3),
                    ("fd_use_char2", 4),
                    ("fd_weight", 5),
                    ("face_d_extra", 6),
                ],
            )
            if logic_fields:
                sections.append({"title": "Minx Character Lora Logic", "fields": logic_fields})

        if loader_node and not self._node_is_bypassed(loader_node):
            w = self._get_widgets(loader_node)
            loader_fields = []
            self._append_fields_from_specs(
                loader_fields,
                w,
                [
                    ("character_1", 0),
                    ("character_2", 1),
                    ("character_1_strength", 2),
                    ("character_2_strength", 3),
                    ("use_both", 4),
                    ("use_char1", 5),
                    ("use_char2", 6),
                    ("log_loads", 7),
                ],
            )
            if loader_fields:
                sections.append({"title": "Minx Character Lora Loader", "fields": loader_fields})

        if not sections:
            return None
        return {"sections": sections}

    def _extract_sampler_settings(self, workflow, instance=0, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "ClownsharKSampler_Beta", ctx)
        if instance < len(nodes):
            w = self._get_widgets(nodes[instance])
            return {
                "node_type": "ClownsharKSampler_Beta",
                "eta": safe_get(w, 0, 0.5),
                "sampler": safe_get(w, 1, ""),
                "scheduler": safe_get(w, 2, ""),
                "steps": safe_get(w, 3, 8),
                "cfg": safe_get(w, 4, 1.0),
                "denoise": safe_get(w, 5, 1.0),
            }
        return None

    def _extract_sampler_settings_advanced(self, workflow, instance=0, ctx=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)

        nodes = self._find_nodes_by_type(workflow, "SamplerCustomAdvanced", ctx)
        active_nodes = [node for node in nodes if not self._node_is_bypassed(node)]
        if instance >= len(active_nodes):
            return None

        node = active_nodes[instance]
        inputs = node.get("inputs", [])
        links = workflow.get("links", [])
        links_by_id = {}
        for link in links:
            if isinstance(link, list) and len(link) >= 5:
                links_by_id[link[0]] = link

        upstream_nodes = {}
        for input_def in inputs:
            link_id = input_def.get("link")
            if not link_id:
                continue
            link = links_by_id.get(link_id)
            if not link:
                continue
            source_node_id = link[1]
            upstream = ctx["nodes_by_id"].get(source_node_id)
            if upstream is not None:
                upstream_nodes[input_def.get("name")] = upstream

        sampler_node = upstream_nodes.get("sampler")
        scheduler_node = upstream_nodes.get("sigmas")
        guider_node = upstream_nodes.get("guider")

        sampler_widgets = self._get_widgets(sampler_node) if sampler_node else []
        scheduler_widgets = self._get_widgets(scheduler_node) if scheduler_node else []
        guider_widgets = self._get_widgets(guider_node) if guider_node else []

        return {
            "node_type": "SamplerCustomAdvanced",
            "eta": 0.5,
            "sampler": safe_get(sampler_widgets, 0, ""),
            "scheduler": safe_get(scheduler_widgets, 0, ""),
            "steps": safe_get(scheduler_widgets, 1, 0),
            "cfg": safe_get(guider_widgets, 0, 1.0),
            "denoise": safe_get(scheduler_widgets, 2, 1.0),
        }

    def _sampler_sort_key(self, sampler):
        if not isinstance(sampler, dict):
            return (1, float("inf"))

        denoise = sampler.get("denoise")
        try:
            denoise_value = float(denoise)
        except (TypeError, ValueError):
            denoise_value = None

        # Primary sampler should be the full denoise pass, typically 1.0.
        if denoise_value is None:
            return (1, float("inf"))
        return (0 if abs(denoise_value - 1.0) < 1e-9 else 1, abs(denoise_value - 1.0))

    def _extract_ordered_samplers(self, workflow, ctx=None):
        samplers = []
        index = 0

        while True:
            sampler = self._extract_sampler_settings(workflow, index, ctx)
            if sampler is None:
                break
            samplers.append(sampler)
            index += 1

        if not samplers:
            index = 0
            while True:
                sampler = self._extract_sampler_settings_advanced(workflow, index, ctx)
                if sampler is None:
                    break
                samplers.append(sampler)
                index += 1

        samplers.sort(key=self._sampler_sort_key)
        return samplers

    def _extract_sigma_scaling(self, workflow, instance=0, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "ClownOptions_SigmaScaling_Beta", ctx)
        if instance < len(nodes):
            w = self._get_widgets(nodes[instance])
            return {
                "s_noise": safe_get(w, 0, 1.0),
                "s_noise_substep": safe_get(w, 1, 1.0),
                "noise_anchor_sde": safe_get(w, 2, 1.0),
                "lying": safe_get(w, 3, 1.0),
                "lying_inv": safe_get(w, 4, 1.0),
                "lying_start_step": safe_get(w, 5, 0),
                "lying_inv_start_step": safe_get(w, 6, 0),
            }
        return None

    def _extract_resolution(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Minx Qwen Resolution Selector", ctx)
        if nodes:
            return safe_get(self._get_widgets(nodes[0]), 0, "")
        return ""

    def _extract_loader_model_name(self, workflow, title_candidates=None, type_candidates=None, ctx=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)
        title_candidates = {str(t).lower() for t in (title_candidates or [])}
        type_candidates = [str(t).lower() for t in (type_candidates or [])]

        for node in ctx["nodes"]:
            if not isinstance(node, dict):
                continue

            node_type = str(node.get("type", "")).lower()
            title = str((node.get("properties") or {}).get("title", "")).lower()

            title_match = title and title in title_candidates
            type_match = any(candidate in node_type for candidate in type_candidates)
            if not (title_match or type_match):
                continue

            value = safe_get(self._get_widgets(node), 0, "")
            if isinstance(value, str):
                return value
            if value is not None:
                return str(value)

        return ""

    def _extract_model_loaders(self, workflow, ctx=None):
        return {
            "unet_model": self._extract_loader_model_name(
                workflow,
                title_candidates=["Load Diffusion Model", "Load UNET", "Load UNet"],
                type_candidates=["load diffusion model", "unetloader"],
                ctx=ctx,
            ),
            "text_encoder_model": self._extract_loader_model_name(
                workflow,
                title_candidates=["Load CLIP", "Load Text Encoder"],
                type_candidates=["load clip", "cliploader"],
                ctx=ctx,
            ),
            "vae_model": self._extract_loader_model_name(
                workflow,
                title_candidates=["Load VAE"],
                type_candidates=["load vae", "vaeloader"],
                ctx=ctx,
            ),
        }

    def _extract_detail_boost(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "ClownOptions_DetailBoost_Beta", ctx)
        if nodes:
            w = self._get_widgets(nodes[0])
            return {
                "boost_strength": safe_get(w, 0, 1.0),
                "mode": safe_get(w, 1, ""),
            }
        return None

    def _extract_detail_boosts(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "ClownOptions_DetailBoost_Beta", ctx)
        cards = []
        for idx, node in enumerate(nodes, start=1):
            if self._node_is_bypassed(node):
                continue

            w = self._get_widgets(node)
            cards.append(
                {
                    "title": f"Detail Boost {idx}" if len(nodes) > 1 else "Detail Boost",
                    "weight": safe_get(w, 0, 1.0),
                    "method": safe_get(w, 1, ""),
                    "mode": safe_get(w, 2, ""),
                    "eta": safe_get(w, 3, 0.5),
                    "start_step": safe_get(w, 4, 0),
                    "end_step": safe_get(w, 5, 0),
                }
            )
        return cards

    def _extract_aura_flow_shift(self, workflow, instance=0, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "ModelSamplingAuraFlow", ctx)
        if instance < len(nodes):
            return safe_get(self._get_widgets(nodes[instance]), 0, 5.0)
        return None

    def _extract_vignette(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Minx Vignette", ctx)
        if nodes:
            return safe_get(self._get_widgets(nodes[0]), 0, 0.5)
        return None

    def _extract_matte_black(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Minx Matte Black", ctx)
        if nodes:
            w = self._get_widgets(nodes[0])
            return {
                "matte_level": safe_get(w, 0, ""),
                "s_curve": safe_get(w, 1, ""),
                "clamp_whites": safe_get(w, 2, ""),
            }
        return None

    def _extract_lora_prefix(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Minx Lora Prefixes", ctx)
        if nodes:
            return safe_get(self._get_widgets(nodes[0]), 0, "")
        return None

    def _extract_quad_randomizer(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Minx • Quad Randomizer", ctx)
        if not nodes:
            return None

        w = self._get_widgets(nodes[0])
        if not isinstance(w, list) or not w:
            return None

        def normalize_quad_value(value):
            if isinstance(value, str) and value.lower() == "none":
                return ""
            return value

        def extract_group_value(group):
            # Legacy workflows store each quad group as:
            # [file, randomize, search, selection]
            # Newer workflows insert a step-through toggle:
            # [file, randomize, stepthru, search, selection]
            if len(group) >= 5 and isinstance(group[2], bool):
                randomize = bool(group[1])
                search_value = group[3]
                selection_value = group[4]
            elif len(group) >= 4:
                randomize = bool(group[1])
                search_value = group[2]
                selection_value = group[3]
            else:
                return ""
            return normalize_quad_value(search_value if randomize else selection_value)

        file_indices = [
            idx for idx, value in enumerate(w)
            if isinstance(value, str) and value.lower().endswith(".txt")
        ]
        if len(file_indices) >= 4:
            labels = ("style", "location", "character", "pose")
            result = {}
            for label, start_idx, end_idx in zip(labels, file_indices[:4], file_indices[1:4] + [len(w)]):
                result[label] = extract_group_value(w[start_idx:end_idx])
            return result

        def get_value(randomize_idx, search_idx, selection_idx):
            randomize = safe_get(w, randomize_idx, False)
            val = safe_get(w, search_idx if randomize else selection_idx, "")
            return normalize_quad_value(val)

        return {
            "style": get_value(1, 3, 4),
            "location": get_value(6, 8, 9),
            "character": get_value(11, 13, 14),
            "pose": get_value(16, 18, 19),
        }

    def _extract_quad_selections(self, workflow, ctx=None):
        node = self._find_node_by_title(workflow, "💩 Quad Selections", ctx)
        if not node:
            node = self._find_node_by_title(workflow, "Quad Selections", ctx)
        if not node:
            return None

        raw = safe_get(self._get_widgets(node), 0, "")
        if not isinstance(raw, str) or not raw.strip():
            return None

        parts = [part.strip() for part in raw.splitlines()]
        parts += [""] * max(0, 4 - len(parts))
        labels = ("style", "location", "character", "pose")

        result = {}
        for idx, label in enumerate(labels):
            value = parts[idx]
            if value.lower() == "none":
                value = ""
            result[label] = value
        return result

    def _extract_manual_overrides(self, workflow, ctx=None):
        """Extract Minx Manual Text Overrides settings.

        Widget order (from minx_manual_text_overrides.py INPUT_TYPES):
        [0] = Style Manual      -> style_override
        [1] = Location Manual   -> location_override
        [2] = Character Manual  -> character_override
        [3] = Pose Manual       -> pose_override
        [4] = Main Prompt       -> main_prompt
        [5] = Additional Keywords -> additional_keywords
        """
        nodes = self._find_nodes_by_type(workflow, "Minx Manual Text Overrides", ctx)
        if nodes:
            w = self._get_widgets(nodes[0])
            return {
                "style_override": safe_get(w, 0, ""),
                "location_override": safe_get(w, 1, ""),
                "character_override": safe_get(w, 2, ""),
                "pose_override": safe_get(w, 3, ""),
                "main_prompt": safe_get(w, 4, ""),
                "additional_keywords": safe_get(w, 5, ""),
            }
        return None

    def _extract_minx_guide(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "MinxGuide", ctx)
        if nodes:
            node = nodes[0]
            if self._node_is_bypassed(node):
                return None

            w = self._get_widgets(node)
            return {
                "guide_mode": safe_get(w, 0, ""),
                "channelwise_mode": safe_get(w, 1, False),
                "projection_mode": safe_get(w, 2, False),
                "weight": safe_get(w, 3, 0.0),
                "cutoff": safe_get(w, 4, 0.0),
                "weight_scheduler": safe_get(w, 5, ""),
                "start_step": safe_get(w, 6, 0),
                "end_step": safe_get(w, 7, 0),
            }
        return None

    def _find_display_text_by_titles(self, workflow, title_candidates, ctx=None, source_type_candidates=None, source_title_candidates=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)

        title_candidates = {str(title).strip().lower() for title in title_candidates if str(title).strip()}
        source_type_candidates = {
            str(node_type).strip().lower() for node_type in (source_type_candidates or []) if str(node_type).strip()
        }
        source_title_candidates = {
            str(title).strip().lower() for title in (source_title_candidates or []) if str(title).strip()
        }

        links_by_id = {}
        for link in workflow.get("links", []):
            if isinstance(link, list) and len(link) >= 5:
                links_by_id[link[0]] = link

        for node in ctx["nodes"]:
            if not isinstance(node, dict) or node.get("type") != "Minx Display Any":
                continue
            if self._node_is_bypassed(node):
                continue

            node_title = self._get_node_title(node).strip().lower()
            if node_title not in title_candidates:
                continue

            if source_type_candidates or source_title_candidates:
                source_link_id = safe_get(node.get("inputs", []), 0, {}).get("link")
                if not source_link_id:
                    continue
                source_link = links_by_id.get(source_link_id)
                if not source_link:
                    continue
                source_node = ctx["nodes_by_id"].get(source_link[1])
                if not source_node or self._node_is_bypassed(source_node):
                    continue

                source_type = str(source_node.get("type", "")).strip().lower()
                source_title = self._get_node_title(source_node).strip().lower()
                if source_type_candidates and source_type not in source_type_candidates:
                    if not source_title_candidates or source_title not in source_title_candidates:
                        continue
                elif source_title_candidates and source_title not in source_title_candidates and source_type not in source_type_candidates:
                    continue

            value = safe_get(self._get_widgets(node), 0, "")
            if isinstance(value, str) and value.strip():
                return value.strip()

        return ""

    def _trace_output_to_display(self, workflow, start_node_id, output_slot, depth=0, ctx=None):
        if depth > 5:
            return None

        if ctx is None:
            ctx = self._build_workflow_context(workflow)

        start_node = ctx["nodes_by_id"].get(start_node_id)
        if not start_node:
            return None
        outputs = start_node.get("outputs", [])
        if output_slot >= len(outputs):
            return None

        for link_id in outputs[output_slot].get("links", []):
            to_node_id = ctx["link_map"].get(link_id)
            if to_node_id is None:
                continue
            target = ctx["nodes_by_id"].get(to_node_id)
            if not target:
                continue
            node_type = target.get("type")
            if node_type == "Minx Display Any":
                prompt = safe_get(self._get_widgets(target), 0, "")
                if prompt:
                    return prompt
            if node_type == "Minx Concatenate":
                traced = self._trace_output_to_display(workflow, to_node_id, 0, depth + 1, ctx)
                if traced:
                    return traced
        return None

    def _extract_final_prompt(self, workflow, ctx=None):
        if ctx is None:
            ctx = self._build_workflow_context(workflow)

        final_prompt = self._find_display_text_by_titles(
            workflow,
            ["🦝 Final Prompt", "Final Prompt"],
            ctx=ctx,
        )
        if final_prompt:
            return final_prompt

        node = self._find_node_by_title(workflow, "Final Prompt", ctx)
        if node and not self._node_is_bypassed(node):
            return safe_get(self._get_widgets(node), 0, "") or ""

        llm_prompt = self._find_display_text_by_titles(
            workflow,
            ["🦝 LLM Prompt Result", "💩 LLM Prompt Output"],
            ctx=ctx,
            source_type_candidates=[
                "Minx P00p Muse",
                "P00pMinx Prompt",
                "💩 Minx • P00p Muse",
                "💩 Minx • P00p Prompt",
            ],
            source_title_candidates=[
                "💩 Minx • P00p Muse",
                "💩 Minx • P00p Prompt",
            ],
        )
        if llm_prompt:
            return llm_prompt

        return ""

    def _extract_power_lora_loader(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Power Lora Loader (rgthree)", ctx)
        if nodes:
            return self._get_widgets(nodes[0])
        return None

    def _extract_lora_increment(self, workflow, ctx=None):
        nodes = self._find_nodes_by_type(workflow, "Minx Lora Increment", ctx)
        if not nodes:
            return None

        node = nodes[0]
        w = self._get_widgets(node)
        result = {
            "lora_name": safe_get(w, 0, ""),
            "start_weight": safe_get(w, 1, 0.0),
            "increment": safe_get(w, 2, 0.5),
            "max_weight": safe_get(w, 3, 2.0),
            "current_weight": None,
        }
        if workflow.get("_minx_lora_weight") is not None:
            result["current_weight"] = workflow["_minx_lora_weight"]
        else:
            node_id = node.get("id")
            if node_id is not None:
                weight_str = self._trace_output_to_display(workflow, node_id, 2, ctx=ctx)
                try:
                    if weight_str is not None:
                        result["current_weight"] = float(weight_str)
                except (TypeError, ValueError):
                    pass
        return result

    def _extract_summary(self, workflow):
        if not workflow:
            return {}

        ctx = self._build_workflow_context(workflow)
        ordered_samplers = self._extract_ordered_samplers(workflow, ctx)
        summary = {
            "models": self._extract_model_loaders(workflow, ctx),
            "sampler1": ordered_samplers[0] if len(ordered_samplers) > 0 else None,
            "sampler2": ordered_samplers[1] if len(ordered_samplers) > 1 else None,
            "sigma1": self._extract_sigma_scaling(workflow, 0, ctx),
            "sigma2": self._extract_sigma_scaling(workflow, 1, ctx),
            "detail_boosts": self._extract_detail_boosts(workflow, ctx),
            "face_detailers": self._extract_face_detailer_cards(workflow, ctx),
            "resolution": self._extract_resolution(workflow, ctx),
            "aura_flow_shift1": self._extract_aura_flow_shift(workflow, 0, ctx),
            "aura_flow_shift2": self._extract_aura_flow_shift(workflow, 1, ctx),
            "vignette": self._extract_vignette(workflow, ctx),
            "matte_black": self._extract_matte_black(workflow, ctx),
            "lora_prefix": self._extract_lora_prefix(workflow, ctx),
            "detail_boost": self._extract_detail_boost(workflow, ctx),
            "quad": self._extract_quad_randomizer(workflow, ctx),
            "quad_selections": self._extract_quad_selections(workflow, ctx),
            "character_logic": self._extract_character_logic_card(workflow, ctx),
            "manual_overrides": self._extract_manual_overrides(workflow, ctx),
            "guide": self._extract_minx_guide(workflow, ctx),
            "lora_increment": self._extract_lora_increment(workflow, ctx),
            "power_lora": self._extract_power_lora_loader(workflow, ctx),
            "final_prompt": self._extract_final_prompt(workflow, ctx),
            "post_processing": self._extract_post_processing_cards(workflow, ctx),
        }
        return self._sanitize_for_json(summary)

    def _sanitize_for_json(self, value):
        if isinstance(value, dict):
            return {str(k): self._sanitize_for_json(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_for_json(v) for v in value]
        if isinstance(value, tuple):
            return [self._sanitize_for_json(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return repr(value)
