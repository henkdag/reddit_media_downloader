# Reddit Media Downloader

A batch downloader for Reddit post media based on an exported vote CSV, with a specific focus on NSFW content.

It reads a CSV with Reddit post IDs and permalinks, downloads media from posts with `direction=up`, and keeps state so reruns only process new items.

## Maintenance Status

This project is not under active development. It is shared as-is, and issues or pull requests may or may not be reviewed or resolved.

## Features

- Downloads images, galleries, Reddit-hosted videos, and some external embeds
- Automatically picks the newest CSV file in the input directory
- Keeps state in CSV and log files so repeated runs are incremental
- Supports cookie-based access for content that needs a logged-in Reddit session
- Stops immediately on Reddit `429` rate limits by default
- Includes a repair utility for invalid images and duplicate media cleanup

## Requirements

- Python 3.12+
- A Reddit cookies file in Netscape format if you want authenticated downloads
- A CSV with at least these columns:

```csv
id,permalink,direction
abc123,https://www.reddit.com/r/example/comments/abc123/post_title/,up
```

Only rows where `direction` is `up` are processed.

## Getting The CSV

The expected input CSV usually comes from your Reddit account data export.

Request it from Reddit at `https://www.reddit.com/settings/data-request`, where you can submit a GDPR data request for your own account data. After Reddit prepares the export, look for the upvoted posts CSV and use that file as the downloader input.

## Quick Start

### Option 1: Local Python

1. Create and activate a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Put your files in place:

- Input CSV: place it in the project root, or set `DOWNLOADER_INPUT_DIR`
- Cookies: place a Netscape-format cookie file at `./cookies.txt`, or set `DOWNLOADER_COOKIES_FILE`
- Downloads: by default files are written to `./downloads`
- Custom output location: set `DOWNLOADER_OUTPUT_DIR` to change it

4. Run the downloader:

```bash
python main.py
```

### Option 2: Docker Compose

1. Adjust the host paths as needed:

```bash
export DOWNLOADER_INPUT_HOST_DIR=/path/to/input
export DOWNLOADER_STATE_HOST_DIR=/path/to/state
export DOWNLOADER_OUTPUT_HOST_DIR=/path/to/downloads
export DOWNLOADER_COOKIES_HOST_FILE=/path/to/cookies.txt
```

2. Build and run:

```bash
docker compose build
docker compose run --rm reddit-downloader
```

If you do not set those variables, the compose file defaults to:

- Input: `./docker/input`
- State: `./docker/state`
- Output: `./downloads`
- Cookies: `./cookies.txt`

Change the Docker output location by setting `DOWNLOADER_OUTPUT_HOST_DIR` before running `docker compose`.

More Docker details are in [DOCKER.md](/home/hvo/reddit_downloader/DOCKER.md).

## How Input Selection Works

By default the downloader:

- scans `DOWNLOADER_INPUT_DIR`
- looks for all `.csv` files
- ignores its own state CSV files such as `processed_posts.csv` and `failed.csv`
- selects the newest file by modification time

To force a specific file, set:

```bash
export DOWNLOADER_CSV_FILE=/absolute/path/to/my_batch.csv
```

## Output Layout

The downloader creates and updates these files:

- Downloads: `DOWNLOADER_OUTPUT_DIR`
- Processed state: `logs/processed_posts.csv` inside `DOWNLOADER_STATE_DIR`
- Failed items: `logs/failed.csv` inside `DOWNLOADER_STATE_DIR`
- Blacklisted subreddits: `blacklisted_subreddits.txt`
- Logs: `logs/downloader.log` and `logs/downloader.log.old` inside `DOWNLOADER_STATE_DIR`

Subreddit media is stored in per-subreddit folders inside the output directory.

## Configuration

Main environment variables:

```env
DOWNLOADER_INPUT_DIR=/path/to/input
DOWNLOADER_STATE_DIR=/path/to/state
DOWNLOADER_OUTPUT_DIR=/path/to/downloads
DOWNLOADER_COOKIE_DIR=/path/to/cookies
DOWNLOADER_COOKIES_FILE=/path/to/cookies.txt
DOWNLOADER_CSV_FILE=
DOWNLOADER_FAILED_FILE=/path/to/state/logs/failed.csv
DOWNLOADER_PROCESSED_FILE=/path/to/state/logs/processed_posts.csv
DOWNLOADER_LOG_FILE=/path/to/state/logs/downloader.log
DOWNLOADER_LOG_OLD_FILE=/path/to/state/logs/downloader.log.old
DOWNLOADER_MIN_REQUEST_DELAY=1.0
DOWNLOADER_MAX_REQUEST_DELAY=2.0
DOWNLOADER_MAX_RETRIES=4
DOWNLOADER_BACKOFF_BASE=2
DOWNLOADER_STOP_ON_RATE_LIMIT=1
DOWNLOADER_REQUIRE_VALID_COOKIES=0
DOWNLOADER_TEST_FORCE_429=0
```

Behavior notes:

- `DOWNLOADER_STOP_ON_RATE_LIMIT=1` stops the whole batch on the first Reddit `429`
- `DOWNLOADER_REQUIRE_VALID_COOKIES=1` makes startup fail if the cookie file is missing or does not contain active Reddit session cookies
- `DOWNLOADER_CSV_FILE` overrides automatic CSV discovery

## Cookie File

The downloader expects a Netscape-format cookie file.

At startup it checks:

- whether the cookie file exists
- whether it can be loaded
- whether it contains active `reddit.com` cookies
- whether it contains active Reddit session cookies when `DOWNLOADER_REQUIRE_VALID_COOKIES=1`

If strict cookie validation is enabled and the check fails, the process exits with code `3`.

## Exit Codes

- `0`: completed successfully
- `1`: startup/input error such as missing CSV
- `2`: stopped because Reddit returned `429`
- `3`: cookie validation failed

## Repair Utility

The included [repair_invalid_images.py](./repair_invalid_images.py) script can scan the download directory for:

- corrupted JPEG or image files
- suspicious mixed-resolution image sets
- likely duplicate images
- likely duplicate videos

Example dry run:

```bash
python repair_invalid_images.py --mode duplicate-images --root-dir /path/to/downloads
```

Apply the cleanup:

```bash
python repair_invalid_images.py --mode duplicate-images --root-dir /path/to/downloads --apply
```

## Troubleshooting

### `input CSV not found`

- Confirm `DOWNLOADER_CSV_FILE` points to a real file, or
- confirm `DOWNLOADER_INPUT_DIR` exists and contains at least one `.csv`

### `cookie validation failed`

- Make sure the cookie file exists
- make sure it is in Netscape cookie jar format
- refresh the cookies if the Reddit session expired

### Stopped on `429`

This is expected with the default safety setting. Wait and rerun later, or adjust pacing carefully with:

- `DOWNLOADER_MIN_REQUEST_DELAY`
- `DOWNLOADER_MAX_REQUEST_DELAY`

## Files in This Repo

- [main.py](./main.py): downloader entrypoint
- [repair_invalid_images.py](./repair_invalid_images.py): cleanup utility
- [docker-compose.yml](./docker-compose.yml): one-shot Docker runner
- [docker.env](./docker.env): container-side defaults
- [reddit-downloader.env](./reddit-downloader.env): local environment example
