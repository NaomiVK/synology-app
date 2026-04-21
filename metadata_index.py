import json
import sqlite3
from collections import Counter
from pathlib import Path


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS metadata_files (
        path TEXT PRIMARY KEY,
        directory TEXT NOT NULL,
        name TEXT NOT NULL,
        mtime_ns INTEGER NOT NULL,
        size_bytes INTEGER NOT NULL,
        indexed_at TEXT NOT NULL,
        style TEXT,
        character TEXT,
        location TEXT,
        pose TEXT,
        lora_prefix TEXT,
        loras_text TEXT,
        main_prompt TEXT,
        keywords_text TEXT,
        manual_text_overrides TEXT,
        summary_json TEXT,
        ai_analyzed_at TEXT,
        ai_model TEXT,
        ai_detail_level TEXT,
        ai_prompt_version TEXT,
        ai_summary TEXT,
        ai_tags_text TEXT,
        ai_raw_json TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_metadata_files_directory ON metadata_files(directory)",
    "CREATE INDEX IF NOT EXISTS idx_metadata_files_name ON metadata_files(name)",
    """
    CREATE TABLE IF NOT EXISTS metadata_loras (
        file_path TEXT NOT NULL,
        position INTEGER NOT NULL,
        lora_name TEXT NOT NULL,
        strength REAL,
        PRIMARY KEY (file_path, position),
        FOREIGN KEY (file_path) REFERENCES metadata_files(path) ON DELETE CASCADE
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_metadata_loras_name ON metadata_loras(lora_name)",
]

FTS_TABLE = "metadata_files_fts"
FTS_COLUMNS = [
    "path",
    "style",
    "character",
    "location",
    "pose",
    "lora_prefix",
    "loras_text",
    "main_prompt",
    "keywords_text",
    "manual_text_overrides",
    "ai_summary",
    "ai_tags_text",
]
SEARCH_FIELD_MAP = {
    "any": [
        "style",
        "character",
        "location",
        "pose",
        "lora_prefix",
        "loras_text",
        "main_prompt",
        "keywords_text",
        "manual_text_overrides",
        "ai_summary",
        "ai_tags_text",
    ],
    "style": ["style"],
    "character": ["character"],
    "location": ["location"],
    "pose": ["pose"],
    "lora": ["lora_prefix", "loras_text"],
    "prompt": ["main_prompt"],
    "keywords": ["keywords_text"],
    "ai": ["ai_summary", "ai_tags_text"],
    "tag": ["ai_tags_text"],
}
DEFAULT_SEARCH_COLUMNS = [
    "style",
    "character",
    "location",
    "pose",
    "lora_prefix",
    "loras_text",
    "main_prompt",
    "keywords_text",
    "manual_text_overrides",
]


def normalize_text(value):
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "none":
            return None
        return text
    return str(value)


def normalize_tag_key(value):
    text = normalize_text(value)
    if not text:
        return None
    return " ".join(text.lower().split())


def _join_text_parts(values):
    parts = []
    seen = set()
    for value in values:
        text = normalize_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        parts.append(text)
    return "\n".join(parts) if parts else None


def _extract_active_loras_from_summary(summary):
    active = []
    for item in (summary or {}).get("power_lora") or []:
        if not isinstance(item, dict):
            continue
        name = normalize_text(item.get("lora") or item.get("name"))
        if not name:
            continue
        enabled = item.get("on", True)
        if isinstance(enabled, str):
            enabled = enabled.lower() in {"true", "1", "yes", "on"}
        if not enabled:
            continue
        strength = item.get("strength")
        try:
            strength = float(strength) if strength is not None else None
        except (TypeError, ValueError):
            strength = None
        active.append({"name": name, "strength": strength})
    return active


def _row_value_by_label(rows, label):
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("label") or "").strip().lower() == label.lower():
            return row.get("value")
    return None


def ensure_metadata_files_columns(conn):
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(metadata_files)").fetchall()
    }
    expected_columns = {
        "keywords_text": "TEXT",
        "ai_analyzed_at": "TEXT",
        "ai_model": "TEXT",
        "ai_detail_level": "TEXT",
        "ai_prompt_version": "TEXT",
        "ai_summary": "TEXT",
        "ai_tags_text": "TEXT",
        "ai_raw_json": "TEXT",
    }
    for column_name, column_type in expected_columns.items():
        if column_name not in columns:
            conn.execute(f"ALTER TABLE metadata_files ADD COLUMN {column_name} {column_type}")


def initialize_fts(conn):
    existing = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (FTS_TABLE,)).fetchone()
    if existing is not None:
        current_columns = [row["name"] for row in conn.execute(f"PRAGMA table_info({FTS_TABLE})").fetchall()]
        if current_columns != FTS_COLUMNS:
            conn.execute(f"DROP TABLE IF EXISTS {FTS_TABLE}")
    conn.execute(
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {FTS_TABLE} USING fts5(
            path UNINDEXED,
            style,
            character,
            location,
            pose,
            lora_prefix,
            loras_text,
            main_prompt,
            keywords_text,
            manual_text_overrides,
            ai_summary,
            ai_tags_text,
            tokenize='unicode61 remove_diacritics 2'
        )
        """
    )


def rebuild_fts_from_metadata_files(conn):
    conn.execute(f"DELETE FROM {FTS_TABLE}")
    conn.execute(
        f"""
        INSERT INTO {FTS_TABLE} (
            path, style, character, location, pose,
            lora_prefix, loras_text, main_prompt, keywords_text, manual_text_overrides,
            ai_summary, ai_tags_text
        )
        SELECT
            path, style, character, location, pose,
            lora_prefix, loras_text, main_prompt, keywords_text, manual_text_overrides,
            ai_summary, ai_tags_text
        FROM metadata_files
        """
    )


def ensure_fts_synced(conn):
    metadata_count = int(conn.execute("SELECT COUNT(*) AS n FROM metadata_files").fetchone()["n"])
    fts_count = int(conn.execute(f"SELECT COUNT(*) AS n FROM {FTS_TABLE}").fetchone()["n"])
    if metadata_count != fts_count:
        rebuild_fts_from_metadata_files(conn)


def upsert_fts_record(conn, record):
    conn.execute(f"DELETE FROM {FTS_TABLE} WHERE path = ?", (record["path"],))
    conn.execute(
        f"""
        INSERT INTO {FTS_TABLE} (
            path, style, character, location, pose,
            lora_prefix, loras_text, main_prompt, keywords_text, manual_text_overrides,
            ai_summary, ai_tags_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["path"],
            record["style"],
            record["character"],
            record["location"],
            record["pose"],
            record["lora_prefix"],
            record["loras_text"],
            record["main_prompt"],
            record["keywords_text"],
            record["manual_text_overrides"],
            record["ai_summary"],
            record["ai_tags_text"],
        ),
    )


def build_index_record(rel_path, parsed, metadata_response, indexed_at, stat_result, ai_record=None):
    summary = parsed.get("summary") or {}
    manual_rows = metadata_response.get("manual_override_rows") or []
    quad_rows = metadata_response.get("quad_rows") or []
    prompt_blocks = metadata_response.get("prompt_blocks") or {}
    active_loras = metadata_response.get("active_loras") or _extract_active_loras_from_summary(summary)

    style = normalize_text(_row_value_by_label(manual_rows, "Style")) or normalize_text(_row_value_by_label(quad_rows, "Style"))
    location = normalize_text(_row_value_by_label(manual_rows, "Location")) or normalize_text(_row_value_by_label(quad_rows, "Location"))
    character = normalize_text(_row_value_by_label(manual_rows, "Character")) or normalize_text(_row_value_by_label(quad_rows, "Character"))
    pose = normalize_text(_row_value_by_label(manual_rows, "Pose")) or normalize_text(_row_value_by_label(quad_rows, "Pose"))
    lora_prefix = normalize_text(_row_value_by_label(manual_rows, "Lora Prefix"))
    main_prompt = normalize_text((prompt_blocks or {}).get("main"))
    keywords_text = normalize_text(_row_value_by_label(manual_rows, "Additional Keywords"))
    manual_text_overrides = _join_text_parts([row.get("value") for row in manual_rows if isinstance(row, dict)])
    loras_text = _join_text_parts([item.get("name") for item in active_loras])
    directory = str(Path(rel_path).parent.as_posix())
    if directory == ".":
        directory = ""
    ai_record = ai_record or {}

    return {
        "path": rel_path,
        "directory": directory,
        "name": Path(rel_path).name,
        "mtime_ns": int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))),
        "size_bytes": int(stat_result.st_size),
        "indexed_at": indexed_at,
        "style": style,
        "character": character,
        "location": location,
        "pose": pose,
        "lora_prefix": lora_prefix,
        "loras_text": loras_text,
        "main_prompt": main_prompt,
        "keywords_text": keywords_text,
        "manual_text_overrides": manual_text_overrides,
        "summary_json": json.dumps(summary, ensure_ascii=True, sort_keys=True) if summary else None,
        "ai_analyzed_at": ai_record.get("analyzed_at"),
        "ai_model": ai_record.get("model"),
        "ai_detail_level": ai_record.get("detail_level"),
        "ai_prompt_version": ai_record.get("prompt_version"),
        "ai_summary": ai_record.get("summary"),
        "ai_tags_text": ai_record.get("tags_text"),
        "ai_raw_json": ai_record.get("raw_json"),
        "loras": active_loras,
    }


def connect(db_path):
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def initialize(conn):
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
    ensure_metadata_files_columns(conn)
    initialize_fts(conn)
    ensure_fts_synced(conn)
    conn.commit()


def scope_where_clause(scope_dir):
    if not scope_dir:
        return "", []
    return " WHERE directory = ? OR directory LIKE ? OR path = ?", [scope_dir, scope_dir + "/%", scope_dir]


def load_existing_files(conn, scope_dir):
    where_clause, params = scope_where_clause(scope_dir)
    rows = conn.execute(
        """
        SELECT
            path,
            mtime_ns,
            size_bytes,
            ai_analyzed_at,
            ai_model,
            ai_detail_level,
            ai_prompt_version
        FROM metadata_files
        """ + where_clause,
        params,
    ).fetchall()
    return {
        row["path"]: {
            "mtime_ns": int(row["mtime_ns"]),
            "size_bytes": int(row["size_bytes"]),
            "ai_analyzed_at": row["ai_analyzed_at"],
            "ai_model": row["ai_model"],
            "ai_detail_level": row["ai_detail_level"],
            "ai_prompt_version": row["ai_prompt_version"],
        }
        for row in rows
    }


def upsert_file_record(conn, record):
    conn.execute(
        """
        INSERT INTO metadata_files (
            path, directory, name, mtime_ns, size_bytes, indexed_at,
            style, character, location, pose, lora_prefix, loras_text,
            main_prompt, keywords_text, manual_text_overrides, summary_json,
            ai_analyzed_at, ai_model, ai_detail_level, ai_prompt_version,
            ai_summary, ai_tags_text, ai_raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            directory = excluded.directory,
            name = excluded.name,
            mtime_ns = excluded.mtime_ns,
            size_bytes = excluded.size_bytes,
            indexed_at = excluded.indexed_at,
            style = excluded.style,
            character = excluded.character,
            location = excluded.location,
            pose = excluded.pose,
            lora_prefix = excluded.lora_prefix,
            loras_text = excluded.loras_text,
            main_prompt = excluded.main_prompt,
            keywords_text = excluded.keywords_text,
            manual_text_overrides = excluded.manual_text_overrides,
            summary_json = excluded.summary_json,
            ai_analyzed_at = excluded.ai_analyzed_at,
            ai_model = excluded.ai_model,
            ai_detail_level = excluded.ai_detail_level,
            ai_prompt_version = excluded.ai_prompt_version,
            ai_summary = excluded.ai_summary,
            ai_tags_text = excluded.ai_tags_text,
            ai_raw_json = excluded.ai_raw_json
        """,
        (
            record["path"],
            record["directory"],
            record["name"],
            record["mtime_ns"],
            record["size_bytes"],
            record["indexed_at"],
            record["style"],
            record["character"],
            record["location"],
            record["pose"],
            record["lora_prefix"],
            record["loras_text"],
            record["main_prompt"],
            record["keywords_text"],
            record["manual_text_overrides"],
            record["summary_json"],
            record["ai_analyzed_at"],
            record["ai_model"],
            record["ai_detail_level"],
            record["ai_prompt_version"],
            record["ai_summary"],
            record["ai_tags_text"],
            record["ai_raw_json"],
        ),
    )
    conn.execute("DELETE FROM metadata_loras WHERE file_path = ?", (record["path"],))
    for idx, item in enumerate(record["loras"]):
        conn.execute(
            "INSERT INTO metadata_loras (file_path, position, lora_name, strength) VALUES (?, ?, ?, ?)",
            (record["path"], idx, item["name"], item["strength"]),
        )
    upsert_fts_record(conn, record)


def delete_missing_files(conn, rel_paths):
    if not rel_paths:
        return 0
    conn.executemany(f"DELETE FROM {FTS_TABLE} WHERE path = ?", [(rel_path,) for rel_path in rel_paths])
    conn.executemany("DELETE FROM metadata_files WHERE path = ?", [(rel_path,) for rel_path in rel_paths])
    return len(rel_paths)


def get_file_record(conn, rel_path):
    row = conn.execute("SELECT * FROM metadata_files WHERE path = ?", (rel_path,)).fetchone()
    return dict(row) if row is not None else None


def _split_unquoted(text, separator):
    parts = []
    current = []
    quote_char = None
    for char in text:
        if char in {"'", '"'}:
            if quote_char == char:
                quote_char = None
            elif quote_char is None:
                quote_char = char
            current.append(char)
            continue
        if char == separator and quote_char is None:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(char)
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


def _split_terms_preserving_quotes(text):
    return _split_unquoted(text, " ")


def _strip_wrapping_quotes(text):
    value = normalize_text(text)
    if not value or len(value) < 2:
        return value
    if (value[0] == value[-1]) and value[0] in {'"', "'"}:
        inner = value[1:-1].strip()
        return inner or None
    return value


def _has_wrapping_quotes(text):
    if not isinstance(text, str):
        return False
    stripped = text.strip()
    return len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {'"', "'"}


def _parse_search_clauses(query):
    text = normalize_text(query)
    if not text:
        return []

    has_explicit_clauses = bool(_split_unquoted(text, "+")[1:])
    terms = _split_unquoted(text, "+") if has_explicit_clauses else _split_terms_preserving_quotes(text)
    if not terms:
        return []

    clauses = []
    for term in terms:
        prefix, sep, value = term.partition(":")
        normalized_value = _strip_wrapping_quotes(value)
        if sep and prefix.lower() in SEARCH_FIELD_MAP and normalized_value:
            clauses.append(
                {
                    "field_key": prefix.lower(),
                    "columns": list(SEARCH_FIELD_MAP[prefix.lower()]),
                    "value": normalized_value,
                    "quoted": _has_wrapping_quotes(value),
                }
            )
        else:
            normalized_term = _strip_wrapping_quotes(term)
            if not normalized_term:
                continue
            clauses.append(
                {
                    "field_key": None,
                    "columns": None,
                    "value": normalized_term,
                    "quoted": _has_wrapping_quotes(term),
                }
            )
    return clauses


def _fts_escape_phrase(value):
    return '"' + str(value).replace('"', '""') + '"'


def _build_fts_query(query, tag_search_terms=None):
    clauses = _parse_search_clauses(query)
    if not clauses:
        return None

    parts = []
    for clause in clauses:
        terms = [clause["value"]]
        if clause.get("field_key") == "tag" and isinstance(tag_search_terms, dict):
            normalized_key = normalize_tag_key(clause["value"])
            expanded_terms = tag_search_terms.get(normalized_key) or []
            if expanded_terms:
                terms = list(expanded_terms)
        columns = clause["columns"]
        term_parts = []
        for value in terms:
            term = _fts_escape_phrase(value)
            if not columns:
                term_parts.append("(" + " OR ".join([f"{column}:{term}" for column in DEFAULT_SEARCH_COLUMNS]) + ")")
                continue
            if len(columns) == 1:
                term_parts.append(f"{columns[0]}:{term}")
                continue
            term_parts.append("(" + " OR ".join([f"{column}:{term}" for column in columns]) + ")")
        if term_parts:
            parts.append(term_parts[0] if len(term_parts) == 1 else "(" + " OR ".join(term_parts) + ")")

    if not parts:
        return None
    return " AND ".join(parts)


def search_paths(conn, scope_dir, query, limit=5000, tag_search_terms=None):
    fts_query = _build_fts_query(query, tag_search_terms=tag_search_terms)
    if not fts_query:
        return []

    where_parts = [f"{FTS_TABLE} MATCH ?"]
    params = [fts_query]
    if scope_dir:
        where_parts.append("mf.directory = ?")
        params.append(scope_dir)
    params.append(int(limit))
    rows = conn.execute(
        f"""
        SELECT mf.path
        FROM {FTS_TABLE}
        JOIN metadata_files AS mf ON mf.path = {FTS_TABLE}.path
        WHERE {" AND ".join(where_parts)}
        ORDER BY mf.name COLLATE NOCASE ASC, mf.path COLLATE NOCASE ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [row["path"] for row in rows]


def _build_metadata_search_filters(query=None, top_level_only=True, scope_dir=None, favorites_only_paths=None, filename_term=None, tag_search_terms=None):
    where_parts = []
    params = []
    fts_query = _build_fts_query(query, tag_search_terms=tag_search_terms)
    use_fts = bool(fts_query)
    if use_fts:
        where_parts.append(f"{FTS_TABLE} MATCH ?")
        params.append(fts_query)

    if top_level_only:
        where_parts.append("mf.directory <> '' AND instr(mf.directory, '/') = 0")
    elif scope_dir:
        where_parts.append("(mf.directory = ? OR mf.directory LIKE ? OR mf.path = ?)")
        params.extend([scope_dir, scope_dir + "/%", scope_dir])

    filename_text = normalize_text(filename_term)
    if filename_text:
        where_parts.append("mf.name LIKE ? ESCAPE '\\'")
        escaped = filename_text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params.append(f"%{escaped}%")

    if favorites_only_paths is not None:
        normalized_paths = [normalize_text(path) for path in favorites_only_paths or []]
        normalized_paths = [path for path in normalized_paths if path]
        if not normalized_paths:
            return {
                "use_fts": use_fts,
                "where_parts": ["0 = 1"],
                "params": params,
            }
        placeholders = ", ".join(["?"] * len(normalized_paths))
        where_parts.append(f"mf.path IN ({placeholders})")
        params.extend(normalized_paths)

    return {
        "use_fts": use_fts,
        "where_parts": where_parts,
        "params": params,
    }


def search_results(conn, query, sort_key="date", sort_dir="desc", offset=0, limit=60, top_level_only=True, scope_dir=None, favorites_only_paths=None, filename_term=None, tag_search_terms=None):
    fts_query = _build_fts_query(query, tag_search_terms=tag_search_terms)
    if not fts_query:
        return {"total": 0, "items": []}

    filters = _build_metadata_search_filters(
        query=query,
        top_level_only=top_level_only,
        scope_dir=scope_dir,
        favorites_only_paths=favorites_only_paths,
        filename_term=filename_term,
        tag_search_terms=tag_search_terms,
    )
    where_parts = list(filters["where_parts"])
    params = list(filters["params"])

    sort_key = "name" if str(sort_key).lower() == "name" else "date"
    sort_dir = "asc" if str(sort_dir).lower() == "asc" else "desc"
    if sort_key == "name":
        order_by = "mf.name COLLATE NOCASE " + ("ASC" if sort_dir == "asc" else "DESC") + ", mf.path COLLATE NOCASE ASC"
    else:
        order_by = "mf.mtime_ns " + ("ASC" if sort_dir == "asc" else "DESC") + ", mf.path COLLATE NOCASE ASC"

    total = conn.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {FTS_TABLE}
        JOIN metadata_files AS mf ON mf.path = {FTS_TABLE}.path
        WHERE {" AND ".join(where_parts)}
        """,
        params,
    ).fetchone()["n"]

    rows = conn.execute(
        f"""
        SELECT mf.path, mf.directory, mf.name, mf.mtime_ns, mf.size_bytes
        FROM {FTS_TABLE}
        JOIN metadata_files AS mf ON mf.path = {FTS_TABLE}.path
        WHERE {" AND ".join(where_parts)}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
        """,
        params + [int(limit), int(offset)],
    ).fetchall()
    return {
        "total": int(total),
        "items": [dict(row) for row in rows],
    }


def summarize_ai_tags(conn, query=None, top_level_only=True, scope_dir=None, favorites_only_paths=None, filename_term=None, tag_limit=50, min_count=1, max_count=None, tag_search_terms=None, tag_canonical_map=None, hidden_generic_tags=None):
    filters = _build_metadata_search_filters(
        query=query,
        top_level_only=top_level_only,
        scope_dir=scope_dir,
        favorites_only_paths=favorites_only_paths,
        filename_term=filename_term,
        tag_search_terms=tag_search_terms,
    )
    from_clause = "FROM metadata_files AS mf"
    if filters["use_fts"]:
        from_clause = f"FROM {FTS_TABLE} JOIN metadata_files AS mf ON mf.path = {FTS_TABLE}.path"

    where_sql = ""
    if filters["where_parts"]:
        where_sql = " WHERE " + " AND ".join(filters["where_parts"])

    rows = conn.execute(
        f"""
        SELECT mf.ai_tags_text
        {from_clause}
        {where_sql}
        """,
        filters["params"],
    ).fetchall()

    counts = Counter()
    matched_count = 0
    tagged_count = 0
    total_tag_instances = 0
    threshold = max(1, int(min_count))
    max_threshold = None
    if max_count is not None:
        try:
            parsed_max = int(max_count)
        except (TypeError, ValueError):
            parsed_max = None
        if parsed_max is not None and parsed_max >= threshold:
            max_threshold = parsed_max
    hidden_generic_keys = set(hidden_generic_tags or [])

    for row in rows:
        matched_count += 1
        raw_text = row["ai_tags_text"]
        if not isinstance(raw_text, str):
            continue
        tags = [line.strip() for line in raw_text.splitlines() if line.strip()]
        if not tags:
            continue
        tagged_count += 1
        seen_canonical = set()
        for tag in tags:
            normalized_tag = normalize_tag_key(tag)
            canonical_tag = tag
            if normalized_tag and isinstance(tag_canonical_map, dict):
                canonical_tag = tag_canonical_map.get(normalized_tag, tag) or tag
            canonical_key = normalize_tag_key(canonical_tag) or canonical_tag
            if canonical_key in hidden_generic_keys:
                continue
            if canonical_key in seen_canonical:
                continue
            seen_canonical.add(canonical_key)
            counts[canonical_tag] += 1
            total_tag_instances += 1

    limit_value = None
    if tag_limit is not None:
        try:
            parsed_limit = int(tag_limit)
        except (TypeError, ValueError):
            parsed_limit = 50
        if parsed_limit > 0:
            limit_value = parsed_limit

    ranked_tags = counts.most_common()
    filtered_tags = [
        {"tag": tag, "count": count}
        for tag, count in ranked_tags
        if count >= threshold and (max_threshold is None or count <= max_threshold)
    ]
    if limit_value is not None:
        filtered_tags = filtered_tags[:limit_value]
    items = filtered_tags
    return {
        "matched_count": matched_count,
        "tagged_count": tagged_count,
        "total_tag_instances": total_tag_instances,
        "items": items,
    }
