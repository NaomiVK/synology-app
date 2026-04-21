# Synology ComfyUI PNG Metadata Viewer

Small web app for browsing a configured folder tree and viewing metadata embedded in PNG files (ComfyUI `workflow`, `prompt`, and Minx-specific extracted summary fields).

## Key behavior

- Root-scoped folder browsing with dynamic in-page navigation
- On-demand thumbnail generation plus background preview rebuilds
- SQLite metadata indexing with FTS search
- Favorites persisted to disk
- Optional AI image analysis stored alongside parsed PNG metadata
- Local brush-mask image repair using CPU-only OpenCV inpainting
- Read-only access to your PNG folder via Docker volume mount

## Local run (optional)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export BROWSE_ROOT="/path/to/png/folder"
python3 app.py
```

Open `http://localhost:8080`.

## Docker (general)

```bash
cp docker-compose.yml.example docker-compose.yml
docker compose up --build -d
```

Then open `http://<host>:8088` (or whatever `HOST_PORT` you set).

## Synology NAS setup (step-by-step)

These steps assume DSM 7 with **Container Manager** installed.

### 1. Put the project files on the NAS

Create a folder on the NAS, for example:

- `/volume1/docker/comfy-png-viewer`

Copy this project folder into that location (all files: `Dockerfile`, `app.py`, `png_metadata_parser.py`, `templates/`, `static/`, etc.).

Easy copy methods:

- Windows File Explorer: open `\\YOUR-NAS\docker\` and copy the folder in
- Synology File Station: upload the project folder contents

### 2. Create the compose file

Inside the NAS project folder, copy `docker-compose.yml.example` to `docker-compose.yml` and edit it.

Set these values:

- `HOST_PORT`: the port you want on the NAS (example `8088`)
- volume mount source path: your actual PNG folder
- `BROWSE_ROOT`: keep `/data/output` unless you change the mount target

Important line for your folder:

```yaml
volumes:
  - /volume1/Predator/SD/ComfyUIQwen/output:/data/output:ro
  - /volume1/docker/comfy-png-viewer-cache:/cache
```

### 3. Build and run on the NAS

You can do this either with Container Manager Projects (UI) or SSH.

#### Option A: Container Manager (UI)

- Open **Container Manager**
- Go to **Project**
- Click **Create**
- Choose the project folder you copied to the NAS (`/volume1/docker/comfy-png-viewer`)
- Select `docker-compose.yml`
- Deploy the project

If your DSM version does not build the image from `build: .` in the UI, use Option B once to build it.

#### Option B: SSH (reliable fallback)

Enable SSH in DSM if needed, then connect and run:

```bash
cd /volume1/docker/comfy-png-viewer
sudo docker compose up --build -d
```

To change the port later:

- Edit `docker-compose.yml` and change the left side of `HOST_PORT:8080`
- Re-run:

```bash
sudo docker compose up -d
```

### 4. Open the app

In your browser:

- `http://YOUR-NAS-IP:8088`

Replace `8088` with whatever host port you configured.

### 5. Update the app later

After you change files in the NAS project folder:

```bash
cd /volume1/docker/comfy-png-viewer
sudo docker compose up --build -d
```

## Configuration

Environment variables in `docker-compose.yml`:

- `APP_TITLE`: title shown in the UI
- `BROWSE_ROOT`: internal container path to browse (should match your mount target)
- `SHOW_HIDDEN`: `1` to show hidden files/folders, otherwise `0`
- `THUMB_CACHE_DIR`: writable cache directory for generated gallery thumbnails (example: `/cache/thumbs`, sharded into up to 256 hashed subfolders)
- `THUMB_SIZE_PREVIEW`: max size for the larger in-viewer preview image cache (default `1600`)
- `APP_LOG_PATH`: writable path for structured app logs (example: `/cache/viewer.log`)
- `THUMB_READY_STATE_PATH`: writable JSON state file used to emit one thumbnail-ready summary log per directory/mode (example: `/cache/thumb-ready-state.json`)
- `REBUILD_STATUS_PATH`: writable JSON status file for preview rebuild progress (default `/tmp/rebuild-status.json`)
- `METADATA_DB_PATH`: writable SQLite database path for extracted metadata records (default `/tmp/metadata-index.sqlite`)
- `METADATA_INDEX_STATUS_PATH`: writable JSON status file for metadata indexing progress (default `/tmp/metadata-index-status.json`)
- `METADATA_CACHE_MAX_ITEMS`: max number of parsed metadata entries to keep in the in-memory LRU cache (default `128`)
- `AI_ANALYSIS_ENABLED`: `1` to enable the separate AI vision pass job
- `OPENAI_API_KEY`: OpenAI API key used for AI analysis
- `OPENAI_BASE_URL`: optional API base URL override
- `AI_ANALYSIS_MODEL`: model used for image analysis (default `gpt-4.1-mini`)
- `AI_ANALYSIS_DETAIL`: image detail level sent to the vision model (`low` or `high`)
- `AI_ANALYSIS_MAX_WORKERS`: max concurrent AI analysis jobs during indexing
- `AI_ANALYSIS_PROMPT_VERSION`: version label stored with AI records for cache invalidation / prompt tuning
- `AI_ANALYSIS_STATUS_PATH`: writable JSON status file for AI vision pass progress (default `/tmp/ai-analysis-status.json`)
- `EDITS_DIR`: writable directory for cached edited image variants and edit metadata (default `/cache/edits`)
- `LOCAL_REPAIR_RADIUS`: OpenCV inpaint radius used for brush-mask repair (default `2`)
- `LOCAL_REPAIR_METHOD`: local inpaint algorithm for brush-mask repair (`telea` or `ns`, default `telea`)

For the Synology compose file in this repo, AI secrets/config are expected in:

- `/volume2/NVME/comfy-png-viewer-cache/.env`

Port configuration:

- In `ports`, format is `HOST_PORT:CONTAINER_PORT`
- Example: `8088:8080`
- `8088` is what you visit in the browser
- `8080` is the app port inside the container (usually leave this as-is)

## Security notes

- The app is read-only against your PNG folder (`:ro` mount)
- Generated gallery thumbnails should be stored on a separate writable cache volume such as `/cache`
- The container filesystem is also set to read-only (`read_only: true`)
- Browsing is restricted to the configured `BROWSE_ROOT`
