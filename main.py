import csv
import hashlib
import sys
import os
import re
import time
import random
import requests
import json
from http.cookiejar import MozillaCookieJar
from html import unescape
from urllib.parse import urlparse

try:
    from mutagen.id3 import COMM, TALB, TCON, TIT2, TPE1, TXXX, ID3, ID3NoHeaderError
    from mutagen.mp3 import MP3
    from mutagen.mp4 import MP4
    AUDIO_METADATA_AVAILABLE = True
except ImportError:
    AUDIO_METADATA_AVAILABLE = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEAD_DOMAINS = {
    "gfycat.com",
    "www.gfycat.com"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}


def env_float(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return float(value)


def env_int(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return int(value)


def env_bool(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def env_str(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value


INPUT_DIR = env_str("DOWNLOADER_INPUT_DIR", BASE_DIR)
STATE_DIR = env_str("DOWNLOADER_STATE_DIR", BASE_DIR)
OUTPUT_DIR = env_str("DOWNLOADER_OUTPUT_DIR", os.path.join(BASE_DIR, "downloads"))
COOKIE_DIR = env_str("DOWNLOADER_COOKIE_DIR", BASE_DIR)

CSV_FILE = env_str("DOWNLOADER_CSV_FILE", "")
FAILED_FILE = env_str("DOWNLOADER_FAILED_FILE", os.path.join(STATE_DIR, "logs", "failed.csv"))
PROCESSED_FILE = env_str("DOWNLOADER_PROCESSED_FILE", os.path.join(STATE_DIR, "logs", "processed_posts.csv"))
BLACKLISTED_SUBREDDITS_FILE = env_str(
    "DOWNLOADER_BLACKLISTED_SUBREDDITS_FILE",
    os.path.join(STATE_DIR, "blacklisted_subreddits.txt"),
)
LOG_FILE = env_str("DOWNLOADER_LOG_FILE", os.path.join(STATE_DIR, "logs", "downloader.log"))
LOG_OLD_FILE = env_str("DOWNLOADER_LOG_OLD_FILE", os.path.join(STATE_DIR, "logs", "downloader.log.old"))
COOKIES_FILE = env_str("DOWNLOADER_COOKIES_FILE", os.path.join(COOKIE_DIR, "cookies.txt"))

MIN_REQUEST_DELAY = env_float("DOWNLOADER_MIN_REQUEST_DELAY", 1.0)
MAX_REQUEST_DELAY = env_float("DOWNLOADER_MAX_REQUEST_DELAY", 2.0)
MAX_RETRIES = env_int("DOWNLOADER_MAX_RETRIES", 4)
BACKOFF_BASE = env_int("DOWNLOADER_BACKOFF_BASE", 2)
STOP_ON_RATE_LIMIT = env_bool("DOWNLOADER_STOP_ON_RATE_LIMIT", True)
TEST_FORCE_429 = env_bool("DOWNLOADER_TEST_FORCE_429", False)
REQUIRE_VALID_COOKIES = env_bool("DOWNLOADER_REQUIRE_VALID_COOKIES", False)
RATE_LIMIT_EXIT_CODE = 2
COOKIE_EXIT_CODE = 3

if MIN_REQUEST_DELAY > MAX_REQUEST_DELAY:
    MIN_REQUEST_DELAY, MAX_REQUEST_DELAY = MAX_REQUEST_DELAY, MIN_REQUEST_DELAY


class RateLimitExceeded(RuntimeError):
    pass


class CookieValidationError(RuntimeError):
    pass


class InputCsvNotFoundError(RuntimeError):
    pass


EXPECTED_INPUT_COLUMNS = {"id", "permalink", "direction"}

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(os.path.dirname(LOG_OLD_FILE), exist_ok=True)
processed_posts = set()

blacklisted_subreddits = set()
audio_metadata_warning_shown = False



def build_session(with_cookies=False):
    s = requests.Session()
    s.headers.update(HEADERS)

    if with_cookies and os.path.exists(COOKIES_FILE):
        try:
            jar = MozillaCookieJar()
            jar.load(COOKIES_FILE, ignore_discard=True, ignore_expires=True)
            s.cookies = jar
        except Exception:
            pass

    return s


plain_session = build_session(with_cookies=False)
cookie_session = build_session(with_cookies=True)


def load_cookie_jar():
    jar = MozillaCookieJar()
    jar.load(COOKIES_FILE, ignore_discard=True, ignore_expires=True)
    return jar


def validate_cookie_file():
    def fail_or_warn(message):
        if REQUIRE_VALID_COOKIES:
            raise CookieValidationError(message)
        log_message(f"[*] Cookie check warning: {message}")
        return False

    if not os.path.exists(COOKIES_FILE):
        return fail_or_warn(f"Cookie file is missing: {COOKIES_FILE}")

    try:
        jar = load_cookie_jar()
    except Exception as e:
        if REQUIRE_VALID_COOKIES:
            raise CookieValidationError(f"Could not load cookie file: {e}") from e
        return fail_or_warn(f"Could not load cookie file: {e}")

    now = time.time()
    reddit_cookies = [cookie for cookie in jar if "reddit.com" in (cookie.domain or "").lower()]
    active_cookies = [
        cookie for cookie in reddit_cookies
        if not cookie.expires or cookie.expires > now
    ]
    auth_cookie_names = {"reddit_session", "token_v2", "session_tracker"}
    active_auth_cookies = [cookie for cookie in active_cookies if cookie.name in auth_cookie_names]

    if not reddit_cookies:
        return fail_or_warn("Cookie file does not contain any reddit.com cookies")

    if not active_cookies:
        return fail_or_warn("Cookie file only contains expired reddit.com cookies")

    if REQUIRE_VALID_COOKIES and not active_auth_cookies:
        return fail_or_warn("Cookie file does not contain any active Reddit session cookies")

    log_message(
        f"[*] Cookie check: {len(active_cookies)} active reddit cookie(s), "
        f"{len(active_auth_cookies)} active session cookie(s)"
    )
    return True


def resolve_input_csv_file():
    excluded_paths = {
        os.path.abspath(FAILED_FILE),
        os.path.abspath(PROCESSED_FILE),
    }

    def is_valid_input_csv(path):
        absolute_path = os.path.abspath(path)
        if absolute_path in excluded_paths:
            return False

        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                headers = next(reader, [])
        except Exception:
            return False

        normalized_headers = {header.strip() for header in headers if header}
        return EXPECTED_INPUT_COLUMNS.issubset(normalized_headers)

    if CSV_FILE:
        if os.path.exists(CSV_FILE):
            if not is_valid_input_csv(CSV_FILE):
                raise InputCsvNotFoundError(
                    f"Configured CSV is not a valid input file with columns {sorted(EXPECTED_INPUT_COLUMNS)}: {CSV_FILE}"
                )
            return CSV_FILE
        raise InputCsvNotFoundError(f"CSV file not found: {CSV_FILE}")

    if not os.path.isdir(INPUT_DIR):
        raise InputCsvNotFoundError(f"Input directory not found: {INPUT_DIR}")

    csv_candidates = []
    for name in os.listdir(INPUT_DIR):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(INPUT_DIR, name)
        if os.path.isfile(path) and is_valid_input_csv(path):
            csv_candidates.append(path)

    if not csv_candidates:
        raise InputCsvNotFoundError(
            f"No valid input CSV files found in input directory: {INPUT_DIR}"
        )

    csv_candidates.sort(key=lambda path: (os.path.getmtime(path), os.path.basename(path)))
    return csv_candidates[-1]


def log_failed(post_id, permalink, reason):
    exists = os.path.exists(FAILED_FILE)
    with open(FAILED_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["id", "permalink", "reason"])
        writer.writerow([post_id, permalink, reason])


def log_message(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def rotate_log_file():
    if not os.path.exists(LOG_FILE):
        return

    if os.path.exists(LOG_OLD_FILE):
        os.remove(LOG_OLD_FILE)

    os.replace(LOG_FILE, LOG_OLD_FILE)



def load_processed_posts():
    processed = set()

    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                post_id = row.get("id")
                if post_id:
                    processed.add(post_id)

    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                post_id = row.get("id")
                if post_id:
                    processed.add(post_id)
    log_message(f"[*] Loaded {len(processed)} processed IDs from files")
    return processed


def load_blacklisted_subreddits():
    if not os.path.exists(BLACKLISTED_SUBREDDITS_FILE):
        return set()

    blacklisted = set()
    with open(BLACKLISTED_SUBREDDITS_FILE, encoding="utf-8") as f:
        for line in f:
            name = normalize_subreddit_name(line)
            if name:
                blacklisted.add(name)
    return blacklisted



def add_blacklisted_subreddit(subreddit):
    subreddit = normalize_subreddit_name(subreddit)
    if not subreddit or subreddit in blacklisted_subreddits:
        return

    with open(BLACKLISTED_SUBREDDITS_FILE, "a", encoding="utf-8") as f:
        f.write(subreddit + "\n")

    blacklisted_subreddits.add(subreddit)
    log_message(f"[!] Subreddit added to blacklist: {subreddit}")


def mark_processed(post_id, permalink, status, source=""):
    exists = os.path.exists(PROCESSED_FILE)
    with open(PROCESSED_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["id", "permalink", "status", "source", "processed_at"])
        writer.writerow([
            post_id,
            permalink,
            status,
            source,
            time.strftime("%Y-%m-%d %H:%M:%S"),
        ])
    processed_posts.add(post_id)


def sleep_with_jitter(multiplier=1.0):
    delay = random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY) * multiplier
    time.sleep(delay)


def is_dead_external_url(url):
    host = (urlparse(url).hostname or "").lower()
    return host in DEAD_DOMAINS

def is_redgifs_url(url):
    host = (urlparse(url).hostname or "").lower()
    return host in {"redgifs.com", "www.redgifs.com"}


def is_soundgasm_url(url):
    host = (urlparse(url).hostname or "").lower()
    return host in {"soundgasm.net", "www.soundgasm.net"}


def get_redgifs_slug(url):
    if not url:
        return None

    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host not in {"redgifs.com", "www.redgifs.com"}:
        return None

    parts = [part for part in (parsed.path or "").split("/") if part]
    if not parts:
        return None

    if parts[0] in {"watch", "ifr"} and len(parts) > 1:
        return parts[1].lower()

    return parts[-1].lower()


def is_reddit_page_url(url):
    if not url:
        return False

    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()

    if host not in {
        "reddit.com",
        "www.reddit.com",
        "old.reddit.com",
        "np.reddit.com",
    }:
        return False

    return (
        path.startswith("/gallery/")
        or "/comments/" in path
        or path.startswith("/r/")
    )


def is_preview_media_url(url):
    host = (urlparse(url).hostname or "").lower()
    return host == "preview.redd.it"


def is_reddit_related_url(url):
    host = (urlparse(url).hostname or "").lower()
    return host in {
        "reddit.com",
        "www.reddit.com",
        "old.reddit.com",
        "np.reddit.com",
        "i.redd.it",
        "preview.redd.it",
        "v.redd.it",
    }


def upgrade_preview_reddit_image_url(url):
    if not url:
        return None

    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    ext = os.path.splitext(path)[1].lower()

    if host != "preview.redd.it":
        return None

    if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
        return None

    return parsed._replace(netloc="i.redd.it", query="", fragment="").geturl()

# Helper to detect imgur placeholder
def is_imgur_placeholder_content(response, first_chunk):
    try:
        text = first_chunk.decode(errors="ignore").lower()

        if "image you are requesting does not exist" in text:
            return True
        if "imgur" in text and "not available" in text:
            return True
    except Exception:
        pass
    return False

def is_html_response(response, first_chunk):
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "text/html" in content_type:
        return True

    try:
        text = first_chunk.decode(errors="ignore").lstrip().lower()
        if text.startswith("<!doctype html") or text.startswith("<html"):
            return True
    except Exception:
        pass

    return False


def maybe_raise_rate_limit(url):
    if is_reddit_related_url(url) and STOP_ON_RATE_LIMIT:
        raise RateLimitExceeded(f"Reddit rate limit hit for {url}")

def get_with_retries(session, url, *, stream=False, timeout=20):
    for attempt in range(MAX_RETRIES):
        try:
            if TEST_FORCE_429 and is_reddit_related_url(url):
                maybe_raise_rate_limit(url)
            response = session.get(url, stream=stream, timeout=timeout)
            if response.status_code == 404:
                log_message(f"[!] 404 Not Found for {url} - skipping")
                raise requests.HTTPError(f"404 Not Found for url: {url}", response=response)

            if response.status_code == 403:
                log_message(f"[!] 403 Blocked for {url} - skipping")
                raise requests.HTTPError(f"403 Blocked for url: {url}", response=response)

            if response.status_code == 429:
                if is_reddit_related_url(url):
                    log_message(f"[!] Reddit rate limit reached for {url}")
                    maybe_raise_rate_limit(url)
                wait_time = BACKOFF_BASE ** attempt
                log_message(f"[!] Rate limited on {url} - waiting {wait_time:.1f}s")
                sleep_with_jitter(wait_time)
                continue

            if response.status_code in (500, 502, 503, 504):
                wait_time = BACKOFF_BASE ** attempt
                log_message(f"[!] Temporary server error {response.status_code} on {url} - waiting {wait_time:.1f}s")
                sleep_with_jitter(wait_time)
                continue

            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if hasattr(e, 'response') and e.response is not None and e.response.status_code in (403, 404):
                raise

            error_text = str(e)
            if "NameResolutionError" in error_text or "Failed to resolve" in error_text:
                log_message(f"[!] Dead or unreachable host for {url} - skipping")
                raise

            if attempt == MAX_RETRIES - 1:
                raise
            wait_time = BACKOFF_BASE ** attempt
            log_message(f"[!] Request error for {url}: {e} - retrying in {wait_time:.1f}s")
            sleep_with_jitter(wait_time)

    raise RuntimeError(f"Could not fetch URL after {MAX_RETRIES} attempts: {url}")


def has_complete_jpeg_marker(path):
    try:
        if os.path.getsize(path) < 2:
            return False
        with open(path, "rb") as f:
            f.seek(-2, os.SEEK_END)
            return f.read(2) == b"\xff\xd9"
    except OSError:
        return False


def validate_downloaded_media(path, expected_bytes=None):
    actual_bytes = os.path.getsize(path)
    if expected_bytes is not None and actual_bytes != expected_bytes:
        raise IOError(
            f"Incomplete download: expected {expected_bytes} bytes, got {actual_bytes}"
        )

    ext = os.path.splitext(path)[1].lower()
    if ext in {".jpg", ".jpeg"} and not has_complete_jpeg_marker(path):
        raise IOError("Incomplete JPEG download: missing end marker")


def download_file(session, url, path, *, hash_file=False):
    temp_path = f"{path}.part"

    for attempt in range(MAX_RETRIES):
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)

            sleep_with_jitter()
            r = get_with_retries(session, url, stream=True, timeout=20)
            r.raise_for_status()

            first_chunk = next(r.iter_content(1024 * 64), b"")

            if is_imgur_placeholder_content(r, first_chunk):
                raise ValueError(f"Imgur placeholder detected for url: {url}")

            if is_html_response(r, first_chunk):
                raise ValueError(f"HTML response detected for url: {url}")

            expected_bytes = None
            content_length = r.headers.get("Content-Length")
            if content_length and content_length.isdigit():
                expected_bytes = int(content_length)

            hasher = hashlib.sha256() if hash_file else None
            written_bytes = 0

            with open(temp_path, "wb") as f:
                if first_chunk:
                    f.write(first_chunk)
                    written_bytes += len(first_chunk)
                    if hasher:
                        hasher.update(first_chunk)
                for chunk in r.iter_content(1024 * 64):
                    if chunk:
                        f.write(chunk)
                        written_bytes += len(chunk)
                        if hasher:
                            hasher.update(chunk)

            validate_downloaded_media(temp_path, expected_bytes=expected_bytes)
            os.replace(temp_path, path)
            return hasher.hexdigest() if hasher else None
        except ValueError:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)

            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = BACKOFF_BASE ** attempt
            log_message(f"[!] Incomplete download for {url}: {e} - retrying in {wait_time:.1f}s")
            sleep_with_jitter(wait_time)

def safe_name(text):
    text = text or "unknown"
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip("_") or "unknown"


def normalize_subreddit_name(value):
    value = (value or "").strip().lower()
    if not value:
        return None

    value = re.sub(r"^https?://(?:www\.|old\.|np\.)?reddit\.com/r/", "", value)
    value = re.sub(r"^/?r/", "", value)
    value = re.sub(r"^self\.", "", value)
    value = value.strip("/")
    if not value:
        return None

    if "/" in value:
        value = value.split("/", 1)[0]

    normalized = safe_name(value)
    return normalized if normalized != "unknown" else None

def get_subreddit_dir(base_dir, post):
    subreddit = safe_name(post.get("subreddit", "unknown"))
    folder = os.path.join(base_dir, subreddit)
    os.makedirs(folder, exist_ok=True)
    return folder

def get_subreddit_from_permalink(permalink):
    match = re.search(r"/r/([^/]+)/", permalink)
    if not match:
        return None
    return normalize_subreddit_name(match.group(1))

def fetch_json(permalink):
    json_url = permalink.rstrip("/") + ".json"

    for label, session in [("plain", plain_session), ("cookie", cookie_session)]:
        for attempt in range(MAX_RETRIES):
            try:
                sleep_with_jitter()
                if TEST_FORCE_429:
                    maybe_raise_rate_limit(json_url)
                r = session.get(json_url, timeout=20)

                if r.status_code == 404:
                    try:
                        payload = r.json()
                        if isinstance(payload, dict) and payload.get("reason") == "banned":
                            return payload, "banned"
                    except Exception:
                        pass

                    log_message(f"[!] 404 Not Found for {json_url} - skipping")
                    break

                if r.status_code == 403:
                    log_message(f"[!] 403 Blocked for {json_url} - skipping")
                    break

                if r.status_code == 429:
                    log_message(f"[!] Reddit rate limit reached for {json_url}")
                    maybe_raise_rate_limit(json_url)
                    wait_time = BACKOFF_BASE ** attempt
                    log_message(f"[!] Rate limited on {json_url} - waiting {wait_time:.1f}s")
                    sleep_with_jitter(wait_time)
                    continue

                if r.status_code in (500, 502, 503, 504):
                    wait_time = BACKOFF_BASE ** attempt
                    log_message(f"[!] Temporary server error {r.status_code} on {json_url} - waiting {wait_time:.1f}s")
                    sleep_with_jitter(wait_time)
                    continue

                r.raise_for_status()

                try:
                    payload = r.json()
                    if isinstance(payload, dict) and payload.get("reason") == "banned":
                        return payload, "banned"
                    return payload, label
                except Exception:
                    break

            except requests.RequestException as e:
                error_text = str(e)
                if "NameResolutionError" in error_text or "Failed to resolve" in error_text:
                    log_message(f"[!] Dead or unreachable host for {json_url} - skipping")
                    break

                if attempt == MAX_RETRIES - 1:
                    break

                wait_time = BACKOFF_BASE ** attempt
                log_message(f"[!] Request error for {json_url}: {e} - retrying in {wait_time:.1f}s")
                sleep_with_jitter(wait_time)

    return None, None


def fetch_html_media(permalink, session):
    try:
        sleep_with_jitter()
        r = get_with_retries(session, permalink, timeout=20)
        html = r.text

        media_urls = set()

        # og:image
        for match in re.findall(r'property="og:image" content="([^"]+)"', html):
            media_urls.add(match)

        # og:video
        for match in re.findall(r'property="og:video" content="([^"]+)"', html):
            media_urls.add(match)

        # <source src="...">
        for match in re.findall(r'<source[^>]+src="([^"]+)"', html):
            media_urls.add(match)

        return list(media_urls)
    except Exception as e:
        log_message(f"[!] HTML fallback failed for {permalink}: {e}")
        return []

def resolve_redgifs_media_url(url, session):
    try:
        sleep_with_jitter()
        r = get_with_retries(session, url, timeout=20)
        html = r.text

        patterns = [
            r'property="og:video" content="([^"]+)"',
            r'name="twitter:player:stream" content="([^"]+)"',
            r'<source[^>]+src="([^"]+)"',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html)
            for match in matches:
                if match:
                    return match.replace("&amp;", "&")
    except Exception as e:
        log_message(f"[!] Redgifs resolve failed for {url}: {e}")

    return None


def resolve_soundgasm_media_url(url, session):
    try:
        sleep_with_jitter()
        r = get_with_retries(session, url, timeout=20)
        html = r.text

        patterns = [
            r'\bm4a:\s*"([^"]+)"',
            r'\bmp3:\s*"([^"]+)"',
            r'<source[^>]+src="([^"]+)"',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html)
            for match in matches:
                if match:
                    return match.replace("&amp;", "&")
    except Exception as e:
        log_message(f"[!] Soundgasm resolve failed for {url}: {e}")

    return None

def ext_from_url(url, default=".bin"):
    base = url.split("?")[0].lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".m4a", ".mp3"):
        if base.endswith(ext):
            return ext
    return default


def is_hash_dedup_image_ext(ext):
    return ext.lower() in {".jpg", ".jpeg", ".png", ".webp"}


def canonical_media_url(url):
    url = (url or "").replace("&amp;", "&").strip()
    if not url:
        return ""

    parsed = urlparse(url)
    path = (parsed.path or "").rstrip("/")
    return parsed._replace(path=path, query="", fragment="").geturl()


def guess_ext_from_media_url(url, post=None):
    ext = ext_from_url(url, "")
    if ext:
        return ext

    if "v.redd.it" in url:
        return ".mp4"
    if "i.redd.it" in url or "preview.redd.it" in url:
        return ".jpg"
    if "i.imgur.com" in url:
        return ".jpg"
    if is_redgifs_url(url):
        return ".mp4"
    if is_soundgasm_url(url):
        return ".m4a"
    if post and post.get("is_video"):
        return ".mp4"
    return ".bin"


def parse_title_metadata(raw_title):
    raw_title = (raw_title or "").strip()
    tags = [tag.strip() for tag in re.findall(r"\[([^\]]+)\]", raw_title) if tag.strip()]
    cleaned_title = re.sub(r"\s*\[[^\]]+\]", "", raw_title).strip()
    cleaned_title = re.sub(r"\s{2,}", " ", cleaned_title)
    return {
        "full_title": raw_title,
        "display_title": cleaned_title or raw_title or "unknown",
        "tags": tags,
    }


def build_audio_metadata(post):
    title_info = parse_title_metadata(post.get("title"))
    author = (post.get("author") or "unknown").strip() or "unknown"
    subreddit = (post.get("subreddit") or "unknown").strip() or "unknown"
    permalink = (post.get("permalink") or "").strip()
    if permalink.startswith("/"):
        permalink = f"https://www.reddit.com{permalink}"

    comments = []
    if title_info["tags"]:
        comments.append(f"Tags: {', '.join(title_info['tags'])}")
    if permalink:
        comments.append(f"Reddit: {permalink}")

    return {
        "title": title_info["display_title"],
        "full_title": title_info["full_title"],
        "author": author,
        "subreddit": subreddit,
        "tags": title_info["tags"],
        "permalink": permalink,
        "comment": " | ".join(comments),
    }


def apply_audio_metadata(path, post):
    global audio_metadata_warning_shown

    if not post:
        return

    ext = os.path.splitext(path)[1].lower()
    if ext not in {".m4a", ".mp3"}:
        return
    if not AUDIO_METADATA_AVAILABLE:
        if not audio_metadata_warning_shown:
            log_message("[*] Audio metadata tagging skipped: install mutagen to enable it")
            audio_metadata_warning_shown = True
        return

    metadata = build_audio_metadata(post)

    try:
        if ext == ".m4a":
            audio = MP4(path)
            audio["\xa9nam"] = [metadata["title"]]
            audio["\xa9ART"] = [metadata["author"]]
            audio["\xa9alb"] = [metadata["subreddit"]]
            if metadata["tags"]:
                audio["\xa9gen"] = [", ".join(metadata["tags"])]
            if metadata["comment"]:
                audio["\xa9cmt"] = [metadata["comment"]]
            if metadata["permalink"]:
                audio["----:com.apple.iTunes:reddit_permalink"] = [metadata["permalink"].encode("utf-8")]
            if metadata["tags"]:
                audio["----:com.apple.iTunes:reddit_tags"] = [", ".join(metadata["tags"]).encode("utf-8")]
            audio.save()
            return

        try:
            tags = ID3(path)
        except ID3NoHeaderError:
            tags = ID3()

        tags.delall("TIT2")
        tags.delall("TPE1")
        tags.delall("TALB")
        tags.delall("TCON")
        tags.delall("COMM")
        tags.delall("TXXX:reddit_permalink")
        tags.delall("TXXX:reddit_tags")

        tags.add(TIT2(encoding=3, text=metadata["title"]))
        tags.add(TPE1(encoding=3, text=metadata["author"]))
        tags.add(TALB(encoding=3, text=metadata["subreddit"]))
        if metadata["tags"]:
            tags.add(TCON(encoding=3, text=[", ".join(metadata["tags"])]))
        if metadata["comment"]:
            tags.add(COMM(encoding=3, lang="eng", desc="", text=metadata["comment"]))
        if metadata["permalink"]:
            tags.add(TXXX(encoding=3, desc="reddit_permalink", text=metadata["permalink"]))
        if metadata["tags"]:
            tags.add(TXXX(encoding=3, desc="reddit_tags", text=", ".join(metadata["tags"])))

        tags.save(path)
    except Exception as e:
        log_message(f"[!] Could not write audio metadata for {path}: {e}")


def extract_media_urls(post):
    media_urls = []
    seen = set()
    queued = set()
    still_priorities = {}

    def normalize_url(url):
        if not url:
            return None
        return url.replace("&amp;", "&")

    def add_url(url):
        url = normalize_url(url)
        if not url:
            return
        if url not in seen:
            seen.add(url)
            media_urls.append(url)

    def is_animated_url(url):
        if not url:
            return False
        url = url.lower()
        return (
            url.endswith(".gif")
            or url.endswith(".mp4")
            or "v.redd.it" in url
            or "reddit_video" in url
            or is_redgifs_url(url)
        )

    media = post.get("media") or {}
    secure_media = post.get("secure_media") or {}
    preview = post.get("preview") or {}
    has_reddit_video_fallback = any(
        (
            media.get("reddit_video", {}).get("fallback_url"),
            secure_media.get("reddit_video", {}).get("fallback_url"),
            preview.get("reddit_video_preview", {}).get("fallback_url"),
        )
    )
    direct_url = post.get("url_overridden_by_dest") or post.get("url")
    direct_url_is_external_embed = (
        bool(direct_url)
        and not is_reddit_page_url(direct_url)
        and not (
            "v.redd.it" in direct_url
            or "i.redd.it" in direct_url
            or "preview.redd.it" in direct_url
        )
    )

    animated_urls = []
    still_urls = []

    def add_animated(url):
        url = normalize_url(url)
        if url and url not in queued:
            queued.add(url)
            animated_urls.append(url)

    def add_still(url, priority=0):
        url = normalize_url(url)
        if not url or is_reddit_page_url(url):
            return

        upgraded_url = upgrade_preview_reddit_image_url(url)
        if upgraded_url and upgraded_url != url:
            add_still(upgraded_url, priority=priority + 1)
            return

        previous_priority = still_priorities.get(url)
        if previous_priority is None or priority > previous_priority:
            still_priorities[url] = priority

        if url not in queued:
            queued.add(url)
            still_urls.append(url)

    def extract_selftext_candidate_urls():
        candidates = []
        seen_candidates = set()

        def add_candidate(url):
            url = normalize_url(url)
            if not url or url in seen_candidates:
                return
            seen_candidates.add(url)
            candidates.append(url)

        selftext_html = unescape(post.get("selftext_html") or "")
        for match in re.findall(r'href="([^"]+)"', selftext_html):
            add_candidate(match)

        selftext = post.get("selftext") or ""
        for match in re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", selftext):
            add_candidate(match)

        return candidates

    if post.get("is_gallery"):
        media_metadata = post.get("media_metadata", {})
        items = post.get("gallery_data", {}).get("items", [])
        for item in items:
            media_id = item.get("media_id")
            meta = media_metadata.get(media_id, {})
            if meta.get("e") == "Image":
                animated = meta.get("s", {}).get("mp4") or meta.get("s", {}).get("gif")
                still = meta.get("s", {}).get("u")
                if animated:
                    add_animated(animated)
                elif still:
                    add_still(still, priority=3)

    add_animated(media.get("reddit_video", {}).get("fallback_url"))
    add_animated(secure_media.get("reddit_video", {}).get("fallback_url"))
    if not direct_url_is_external_embed:
        add_animated(preview.get("reddit_video_preview", {}).get("fallback_url"))

    if not is_reddit_page_url(direct_url):
        if "v.redd.it" in (direct_url or "") and has_reddit_video_fallback:
            pass
        elif is_animated_url(direct_url):
            add_animated(direct_url)
        else:
            add_still(direct_url, priority=2)

    crossposts = post.get("crosspost_parent_list") or []
    if crossposts:
        for url in extract_media_urls(crossposts[0]):
            if is_animated_url(url):
                add_animated(url)
            else:
                add_still(url, priority=2)

    for url in extract_selftext_candidate_urls():
        if is_soundgasm_url(url):
            add_still(url, priority=2)

    for url in animated_urls:
        add_url(url)
    best_still_priority = max(still_priorities.values(), default=0)
    still_urls.sort(
        key=lambda url: (
            -still_priorities.get(url, 0),
            is_preview_media_url(url),
            url,
        )
    )
    for url in still_urls:
        if best_still_priority >= 2 and still_priorities.get(url, 0) < 2:
            continue
        add_url(url)

    return media_urls


def process_post(post_id, permalink):
    if permalink.rstrip("/").endswith("/deleted_by_user"):
        log_failed(post_id, permalink, "deleted_by_user_permalink")
        mark_processed(post_id, permalink, "deleted_by_user_permalink")
        log_message(f"[!] Skip {post_id}: permalink points to deleted_by_user")
        return

    subreddit_from_permalink = get_subreddit_from_permalink(permalink)
    if subreddit_from_permalink and subreddit_from_permalink in blacklisted_subreddits:
        log_failed(post_id, permalink, f"blacklisted_subreddit:{subreddit_from_permalink}")
        mark_processed(post_id, permalink, f"blacklisted_subreddit:{subreddit_from_permalink}")
        log_message(f"[!] Skip {post_id}: subreddit is blacklisted ({subreddit_from_permalink})")
        return

    data, source = fetch_json(permalink)
    if data and source == "banned":
        subreddit = subreddit_from_permalink or "unknown"
        add_blacklisted_subreddit(subreddit)
        log_failed(post_id, permalink, f"banned_subreddit:{subreddit}")
        mark_processed(post_id, permalink, f"banned_subreddit:{subreddit}", "banned")
        log_message(f"[!] Skip {post_id}: subreddit is banned ({subreddit})")
        return

    if data and source == "html":
        post_dir = os.path.join(OUTPUT_DIR, "html_fallback")
        os.makedirs(post_dir, exist_ok=True)
        session = cookie_session
        downloaded = 0
        media_urls = data.get("html_fallback", [])
        seen_non_image_urls = set()
        seen_image_hashes = set()
        seen_redgifs_slugs = set()

        for idx, url in enumerate(media_urls):
            if is_dead_external_url(url):
                log_failed(post_id, permalink, f"dead_external_url:{url}")
                log_message(f"[!] Skip {post_id}: dead external URL {url}")
                continue

            download_url = url
            if is_redgifs_url(url):
                redgifs_slug = get_redgifs_slug(url)
                if redgifs_slug and redgifs_slug in seen_redgifs_slugs:
                    log_message(f"[-] Skip {post_id}: duplicate Redgifs slug {redgifs_slug}")
                    continue
                resolved_url = resolve_redgifs_media_url(url, session)
                if not resolved_url:
                    log_failed(post_id, permalink, f"redgifs_resolve_failed:{url}")
                    log_message(f"[!] Skip {post_id}: could not resolve Redgifs URL {url}")
                    continue
                if redgifs_slug:
                    seen_redgifs_slugs.add(redgifs_slug)
                download_url = resolved_url
            elif is_soundgasm_url(url):
                resolved_url = resolve_soundgasm_media_url(url, session)
                if not resolved_url:
                    log_failed(post_id, permalink, f"soundgasm_resolve_failed:{url}")
                    log_message(f"[!] Skip {post_id}: could not resolve Soundgasm URL {url}")
                    continue
                download_url = resolved_url

            ext = guess_ext_from_media_url(download_url)
            canonical_url = canonical_media_url(download_url)
            should_hash = is_hash_dedup_image_ext(ext)

            if not should_hash:
                if canonical_url in seen_non_image_urls:
                    log_message(f"[-] Skip {post_id}: duplicate media-URL {download_url}")
                    continue
                seen_non_image_urls.add(canonical_url)

            path = os.path.join(post_dir, f"{post_id}_{idx}{ext}")
            if os.path.exists(path):
                continue
            try:
                file_hash = download_file(session, download_url, path, hash_file=should_hash)
                if should_hash:
                    if file_hash in seen_image_hashes:
                        os.remove(path)
                        log_message(f"[-] Skip {post_id}: removed duplicate image {download_url}")
                        continue
                    seen_image_hashes.add(file_hash)
                downloaded += 1
            except ValueError as e:
                log_failed(post_id, permalink, f"html_invalid_media:{download_url}:{e}")
                log_message(f"[!] Skip {post_id}: invalid HTML media for {download_url} ({e})")
            except Exception as e:
                log_failed(post_id, permalink, f"html_download_failed:{download_url}:{e}")

        if downloaded > 0:
            mark_processed(post_id, permalink, f"html_downloaded:{downloaded}", "html")
            log_message(f"[+] {post_id}: HTML fallback downloaded {downloaded} file(s)")
        else:
            log_failed(post_id, permalink, "html_no_media")

        return

    if not data:
        log_failed(post_id, permalink, "json_unavailable")
        log_message(f"[!] Skip {post_id}: JSON unavailable")
        return

    try:
        post = data[0]["data"]["children"][0]["data"]
    except Exception:
        log_failed(post_id, permalink, "invalid_json_structure")
        mark_processed(post_id, permalink, "invalid_json_structure")
        log_message(f"[!] Skip {post_id}: invalid JSON structure")
        return

    subreddit = normalize_subreddit_name(post.get("subreddit"))
    if subreddit and subreddit in blacklisted_subreddits:
        log_failed(post_id, permalink, f"blacklisted_subreddit:{subreddit}")
        mark_processed(post_id, permalink, f"blacklisted_subreddit:{subreddit}", source)
        log_message(f"[!] Skip {post_id}: subreddit is blacklisted ({subreddit})")
        return

    # Removed posts
    if post.get("removed_by_category") or post.get("selftext") == "[deleted]":
        log_failed(post_id, permalink, "deleted_or_removed")
        mark_processed(post_id, permalink, "deleted_or_removed", source)
        log_message(f"[!] Skip {post_id}: deleted or removed")
        return

    post_dir = get_subreddit_dir(OUTPUT_DIR, post)
    session = cookie_session if source == "cookie" else plain_session

    downloaded = 0

    media_urls = extract_media_urls(post)
    seen_non_image_urls = set()
    seen_image_hashes = set()
    seen_redgifs_slugs = set()

    for idx, url in enumerate(media_urls):
        if is_dead_external_url(url):
            log_failed(post_id, permalink, f"dead_external_url:{url}")
            log_message(f"[!] Skip {post_id}: dead external URL {url}")
            continue

        download_url = url
        if is_redgifs_url(url):
            redgifs_slug = get_redgifs_slug(url)
            if redgifs_slug and redgifs_slug in seen_redgifs_slugs:
                log_message(f"[-] Skip {post_id}: duplicate Redgifs slug {redgifs_slug}")
                continue
            resolved_url = resolve_redgifs_media_url(url, session)
            if not resolved_url:
                log_failed(post_id, permalink, f"redgifs_resolve_failed:{url}")
                log_message(f"[!] Skip {post_id}: could not resolve Redgifs URL {url}")
                continue
            if redgifs_slug:
                seen_redgifs_slugs.add(redgifs_slug)
            download_url = resolved_url
        elif is_soundgasm_url(url):
            resolved_url = resolve_soundgasm_media_url(url, session)
            if not resolved_url:
                log_failed(post_id, permalink, f"soundgasm_resolve_failed:{url}")
                log_message(f"[!] Skip {post_id}: could not resolve Soundgasm URL {url}")
                continue
            download_url = resolved_url

        ext = guess_ext_from_media_url(download_url, post)
        canonical_url = canonical_media_url(download_url)
        should_hash = is_hash_dedup_image_ext(ext)

        if not should_hash:
            if canonical_url in seen_non_image_urls:
                log_message(f"[-] Skip {post_id}: duplicate media-URL {download_url}")
                continue
            seen_non_image_urls.add(canonical_url)

        path = os.path.join(post_dir, f"{post_id}_{idx}{ext}")
        if os.path.exists(path):
            continue
        try:
            file_hash = download_file(session, download_url, path, hash_file=should_hash)
            apply_audio_metadata(path, post)
            if should_hash:
                if file_hash in seen_image_hashes:
                    os.remove(path)
                    log_message(f"[-] Skip {post_id}: removed duplicate image {download_url}")
                    continue
                seen_image_hashes.add(file_hash)
            downloaded += 1
        except ValueError as e:
            log_failed(post_id, permalink, f"invalid_media:{download_url}:{e}")
            log_message(f"[!] Skip {post_id}: invalid media for {download_url} ({e})")
        except Exception as e:
            log_failed(post_id, permalink, f"media_download_failed:{download_url}:{e}")

    if downloaded == 0:
        log_failed(post_id, permalink, "no_media_found")
        log_message(
            f"[!] {post_id}: no media found | "
            f"post_hint={post.get('post_hint')} | "
            f"domain={post.get('domain')} | "
            f"url={post.get('url')} | "
            f"is_video={post.get('is_video')} | "
            f"is_gallery={post.get('is_gallery')} | "
            f"has_preview={bool(post.get('preview'))} | "
            f"has_crosspost={bool(post.get('crosspost_parent_list'))}"
        )
    else:
        mark_processed(post_id, permalink, f"downloaded:{downloaded}", source)
        log_message(f"[+] {post_id}: downloaded {downloaded} file(s) via {source}")


def main():
    global processed_posts, blacklisted_subreddits
    rotate_log_file()
    try:
        csv_file = resolve_input_csv_file()
        log_message(
            f"[*] Config: min_delay={MIN_REQUEST_DELAY:.2f}s | "
            f"max_delay={MAX_REQUEST_DELAY:.2f}s | "
            f"max_retries={MAX_RETRIES} | "
            f"backoff_base={BACKOFF_BASE} | "
            f"stop_on_rate_limit={int(STOP_ON_RATE_LIMIT)} | "
            f"require_valid_cookies={int(REQUIRE_VALID_COOKIES)}"
        )
        log_message(
            f"[*] Paths: input_dir={INPUT_DIR} | csv={csv_file} | output={OUTPUT_DIR} | state={STATE_DIR} | cookies={COOKIES_FILE}"
        )
        validate_cookie_file()
        processed_posts = load_processed_posts()
        blacklisted_subreddits = load_blacklisted_subreddits()
        log_message(f"[*] Loaded {len(processed_posts)} already processed posts")
        log_message(f"[*] Loaded {len(blacklisted_subreddits)} blacklisted subreddits")
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            skipped_processed = 0
            for row in reader:
                if row.get("direction") != "up":
                    continue
                if row["id"] in processed_posts:
                    skipped_processed += 1
                    continue
                sleep_with_jitter(0.5)
                process_post(row["id"], row["permalink"])
            if skipped_processed:
                log_message(f"[-] Skipped {skipped_processed} posts: already processed")
    except InputCsvNotFoundError as e:
        log_message(f"[!] Stopped: input CSV not found - {e}")
        sys.exit(1)
    except CookieValidationError as e:
        log_message(f"[!] Stopped: cookie validation failed - {e}")
        sys.exit(COOKIE_EXIT_CODE)
    except RateLimitExceeded as e:
        log_message(f"[!] Stopped: Reddit rate limit hit (429) - {e}")
        sys.exit(RATE_LIMIT_EXIT_CODE)
    except KeyboardInterrupt:
        log_message("[*] Downloader stopped cleanly by user (Ctrl+C)")


if __name__ == "__main__":
    main()
