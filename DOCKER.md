# Docker Usage

This project can run as a one-shot batch job in Docker. The downloader code is baked into the image, while input, output, cookies, and state live on mounted host paths.

By default the container automatically selects the newest `.csv` file from the input directory.

## Mounts

- `input`: directory containing one or more batch CSV files
- `downloads`: final media output mounted at `/data/downloads`
- `state`: runtime state such as `logs/processed_posts.csv`, `logs/failed.csv`, `blacklisted_subreddits.txt`, and `logs/`
- `cookies`: read-only Netscape cookie file mounted at `/data/cookies/cookies.txt`

## Host Path Configuration

Set these variables in your shell before running `docker compose`, or define them in a local `.env` file:

```bash
export DOWNLOADER_INPUT_HOST_DIR=/path/to/input
export DOWNLOADER_STATE_HOST_DIR=/path/to/state
export DOWNLOADER_OUTPUT_HOST_DIR=/path/to/downloads
export DOWNLOADER_COOKIES_HOST_FILE=/path/to/cookies.txt
```

Defaults from `docker-compose.yml`:

- `DOWNLOADER_INPUT_HOST_DIR=./docker/input`
- `DOWNLOADER_STATE_HOST_DIR=./docker/state`
- `DOWNLOADER_OUTPUT_HOST_DIR=./downloads`
- `DOWNLOADER_COOKIES_HOST_FILE=./cookies.txt`

Change the output location by setting `DOWNLOADER_OUTPUT_HOST_DIR` to any host directory you want to use.

## Build And Run

```bash
docker compose build
docker compose run --rm reddit-downloader
```

## CSV Selection

Default behavior:

- the container scans `/data/input`
- all files ending in `.csv` are considered
- the newest `.csv` by modification time is used

To force a specific file, set this in `docker.env`:

```env
DOWNLOADER_CSV_FILE=/data/input/my_batch.csv
```

## Cookie Validation

At startup the container checks:

- whether the cookie file exists
- whether the Netscape cookie file can be loaded
- whether it contains active `reddit.com` cookies
- whether it contains active Reddit session cookies when `DOWNLOADER_REQUIRE_VALID_COOKIES=1`

If cookie validation fails, the container exits with code `3`.

## Rate Limit Behavior

- the first Reddit `429` stops the batch immediately
- exit code for rate limit stop: `2`
- there is no automatic restart

## Quick 429 Stop Test

```bash
docker compose run --rm \
  -e DOWNLOADER_TEST_FORCE_429=1 \
  reddit-downloader

echo $?
```
