import argparse
import csv
import json
import os
import subprocess
from dataclasses import dataclass
from collections import defaultdict

try:
    from PIL import Image
except ImportError:
    Image = None


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "downloads")
PROCESSED_FILE = os.path.join(BASE_DIR, "processed_posts.csv")
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".webm", ".mov", ".gif"}


@dataclass
class ImageCandidate:
    path: str
    width: int | None
    height: int | None
    file_size: int
    modified_at: float
    perceptual_hash: int | None

    @property
    def area(self):
        if self.width is None or self.height is None:
            return -1
        return self.width * self.height

    @property
    def longest_side(self):
        if self.width is None or self.height is None:
            return -1
        return max(self.width, self.height)


@dataclass
class VideoCandidate:
    path: str
    width: int | None
    height: int | None
    duration: float | None
    bitrate: int | None
    file_size: int
    modified_at: float
    codec_name: str

    @property
    def area(self):
        if self.width is None or self.height is None:
            return -1
        return self.width * self.height

    @property
    def quality_known(self):
        return int(
            self.width is not None
            and self.height is not None
            and self.duration is not None
        )


def has_complete_jpeg_marker(path):
    try:
        if os.path.getsize(path) < 2:
            return False
        with open(path, "rb") as f:
            f.seek(-2, os.SEEK_END)
            return f.read(2) == b"\xff\xd9"
    except OSError:
        return False


def is_valid_image_file(path):
    if Image is None:
        raise RuntimeError("Pillow is not installed")

    ext = os.path.splitext(path)[1].lower()
    if ext in {".jpg", ".jpeg"} and not has_complete_jpeg_marker(path):
        return False

    try:
        with Image.open(path) as img:
            img.verify()
        with Image.open(path) as img:
            img.load()
        return True
    except Exception:
        return False


def post_id_from_filename(path):
    filename = os.path.basename(path)
    if "_" not in filename:
        return None
    return filename.split("_", 1)[0]


def load_processed_rows():
    if not os.path.exists(PROCESSED_FILE):
        return [], []

    with open(PROCESSED_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, reader.fieldnames or []


def rewrite_processed_rows(rows, fieldnames):
    with open(PROCESSED_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def iter_invalid_jpegs(root_dir):
    for current_root, _, files in os.walk(root_dir):
        for name in files:
            lower_name = name.lower()
            if not (lower_name.endswith(".jpg") or lower_name.endswith(".jpeg")):
                continue

            path = os.path.join(current_root, name)
            if not is_valid_image_file(path):
                yield path


def iter_invalid_images(root_dir):
    for current_root, _, files in os.walk(root_dir):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext not in IMAGE_EXTENSIONS:
                continue

            path = os.path.join(current_root, name)
            if not is_valid_image_file(path):
                yield path


def iter_post_images(root_dir):
    posts = defaultdict(list)
    for current_root, _, files in os.walk(root_dir):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext not in IMAGE_EXTENSIONS:
                continue

            path = os.path.join(current_root, name)
            post_id = post_id_from_filename(path)
            if not post_id:
                continue
            posts[post_id].append(path)
    return posts


def iter_post_videos(root_dir):
    posts = defaultdict(list)
    for current_root, _, files in os.walk(root_dir):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext not in VIDEO_EXTENSIONS:
                continue

            path = os.path.join(current_root, name)
            post_id = post_id_from_filename(path)
            if not post_id:
                continue
            posts[post_id].append(path)
    return posts


def get_image_size(path):
    if Image is None:
        raise RuntimeError("Pillow is not installed")

    with Image.open(path) as img:
        return img.size


def compute_perceptual_hash(path):
    if Image is None:
        raise RuntimeError("Pillow is not installed")

    with Image.open(path) as img:
        grayscale = img.convert("L").resize((9, 8))
        pixels = list(grayscale.get_flattened_data())

    row_width = 9
    hash_value = 0
    for row in range(8):
        offset = row * row_width
        for col in range(8):
            left = pixels[offset + col]
            right = pixels[offset + col + 1]
            hash_value = (hash_value << 1) | int(left > right)
    return hash_value


def probe_video(path):
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,codec_name,avg_frame_rate,bit_rate:format=duration,size",
            "-of",
            "json",
            path,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    streams = payload.get("streams") or []
    stream = streams[0] if streams else {}
    format_info = payload.get("format") or {}

    def to_int(value):
        if value in (None, "", "N/A"):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def to_float(value):
        if value in (None, "", "N/A"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return VideoCandidate(
        path=path,
        width=to_int(stream.get("width")),
        height=to_int(stream.get("height")),
        duration=to_float(format_info.get("duration")),
        bitrate=to_int(stream.get("bit_rate")),
        file_size=to_int(format_info.get("size")) or os.path.getsize(path),
        modified_at=os.path.getmtime(path),
        codec_name=stream.get("codec_name") or "unknown",
    )


def hamming_distance(left, right):
    return (left ^ right).bit_count()


def build_image_candidate(path):
    file_size = os.path.getsize(path)
    modified_at = os.path.getmtime(path)

    try:
        width, height = get_image_size(path)
        perceptual_hash = compute_perceptual_hash(path)
    except Exception:
        width = None
        height = None
        perceptual_hash = None

    return ImageCandidate(
        path=path,
        width=width,
        height=height,
        file_size=file_size,
        modified_at=modified_at,
        perceptual_hash=perceptual_hash,
    )


def build_video_candidate(path):
    try:
        return probe_video(path)
    except Exception:
        return VideoCandidate(
            path=path,
            width=None,
            height=None,
            duration=None,
            bitrate=None,
            file_size=os.path.getsize(path),
            modified_at=os.path.getmtime(path),
            codec_name="unknown",
        )


def pick_best_duplicate(candidates):
    def sort_key(candidate):
        quality_known = int(candidate.area >= 0)
        return (
            quality_known,
            candidate.area,
            candidate.longest_side,
            candidate.file_size,
            candidate.modified_at,
            candidate.path,
        )

    return max(candidates, key=sort_key)


def cluster_duplicate_candidates(candidates, hash_threshold):
    parent = list(range(len(candidates)))

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left in range(len(candidates)):
        left_candidate = candidates[left]
        if left_candidate.perceptual_hash is None:
            continue
        for right in range(left + 1, len(candidates)):
            right_candidate = candidates[right]
            if right_candidate.perceptual_hash is None:
                continue
            if hamming_distance(left_candidate.perceptual_hash, right_candidate.perceptual_hash) <= hash_threshold:
                union(left, right)

    clusters = defaultdict(list)
    for index, candidate in enumerate(candidates):
        clusters[find(index)].append(candidate)

    return [group for group in clusters.values() if len(group) > 1]


def are_probably_duplicate_videos(left, right, duration_tolerance):
    if left.duration is None or right.duration is None:
        return False

    if abs(left.duration - right.duration) > duration_tolerance:
        return False

    if left.codec_name != right.codec_name:
        return False

    if left.area > 0 and right.area > 0:
        smaller_area = min(left.area, right.area)
        larger_area = max(left.area, right.area)
        if smaller_area / larger_area < 0.35:
            return False

    return True


def cluster_duplicate_videos(candidates, duration_tolerance):
    parent = list(range(len(candidates)))

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left in range(len(candidates)):
        for right in range(left + 1, len(candidates)):
            if are_probably_duplicate_videos(candidates[left], candidates[right], duration_tolerance):
                union(left, right)

    clusters = defaultdict(list)
    for index, candidate in enumerate(candidates):
        clusters[find(index)].append(candidate)

    return [group for group in clusters.values() if len(group) > 1]


def pick_best_video_duplicate(candidates):
    def sort_key(candidate):
        bitrate = candidate.bitrate if candidate.bitrate is not None else -1
        return (
            candidate.quality_known,
            candidate.area,
            bitrate,
            candidate.file_size,
            candidate.modified_at,
            candidate.path,
        )

    return max(candidates, key=sort_key)


def find_duplicate_images(root_dir, hash_threshold):
    if Image is None:
        raise RuntimeError(
            "Pillow is not installed. Install Pillow to use duplicate image checks."
        )

    duplicates_by_post = {}
    post_images = iter_post_images(root_dir)

    for post_id, paths in post_images.items():
        if len(paths) < 2:
            continue

        candidates = []
        for path in sorted(paths):
            candidates.append(build_image_candidate(path))

        duplicate_groups = []
        for cluster in cluster_duplicate_candidates(candidates, hash_threshold):
            keeper = pick_best_duplicate(cluster)
            removals = sorted(
                [candidate for candidate in cluster if candidate.path != keeper.path],
                key=lambda candidate: candidate.path,
            )
            if removals:
                duplicate_groups.append(
                    {
                        "keep": keeper,
                        "remove": removals,
                    }
                )

        if duplicate_groups:
            duplicates_by_post[post_id] = duplicate_groups

    return duplicates_by_post


def find_duplicate_videos(root_dir, duration_tolerance):
    duplicates_by_post = {}
    post_videos = iter_post_videos(root_dir)

    for post_id, paths in post_videos.items():
        if len(paths) < 2:
            continue

        candidates = []
        for path in sorted(paths):
            candidates.append(build_video_candidate(path))

        duplicate_groups = []
        for cluster in cluster_duplicate_videos(candidates, duration_tolerance):
            keeper = pick_best_video_duplicate(cluster)
            removals = sorted(
                [candidate for candidate in cluster if candidate.path != keeper.path],
                key=lambda candidate: candidate.path,
            )
            if removals:
                duplicate_groups.append(
                    {
                        "keep": keeper,
                        "remove": removals,
                    }
                )

        if duplicate_groups:
            duplicates_by_post[post_id] = duplicate_groups

    return duplicates_by_post


def find_mixed_resolution_files(root_dir, area_threshold, side_threshold, min_images):
    if Image is None:
        raise RuntimeError(
            "Pillow is not installed. Install Pillow to use mixed-resolution checks."
        )

    suspicious = defaultdict(list)
    post_images = iter_post_images(root_dir)

    for post_id, paths in post_images.items():
        if len(paths) < min_images:
            continue

        image_info = []
        for path in sorted(paths):
            try:
                width, height = get_image_size(path)
            except Exception:
                continue

            area = width * height
            longest_side = max(width, height)
            image_info.append(
                {
                    "path": path,
                    "width": width,
                    "height": height,
                    "area": area,
                    "longest_side": longest_side,
                }
            )

        if len(image_info) < min_images:
            continue

        max_area = max(info["area"] for info in image_info)
        max_side = max(info["longest_side"] for info in image_info)

        if max_area <= 0 or max_side <= 0:
            continue

        for info in image_info:
            area_ratio = info["area"] / max_area
            side_ratio = info["longest_side"] / max_side
            if area_ratio < area_threshold and side_ratio < side_threshold:
                suspicious[post_id].append(
                    {
                        **info,
                        "area_ratio": area_ratio,
                        "side_ratio": side_ratio,
                    }
                )

    return suspicious


def print_invalid_jpegs(invalid_by_post):
    total_posts = len(invalid_by_post)
    total_files = sum(len(paths) for paths in invalid_by_post.values())

    if total_files == 0:
        print("No corrupted JPEG files found.")
        return False

    print(f"Found {total_files} corrupted JPEG file(s) across {total_posts} post(s).")
    for post_id in sorted(invalid_by_post):
        print(f"- {post_id}: {len(invalid_by_post[post_id])} file(s)")
        for path in invalid_by_post[post_id]:
            print(f"  {path}")
    return True


def print_mixed_resolution(suspicious_by_post):
    total_posts = len(suspicious_by_post)
    total_files = sum(len(items) for items in suspicious_by_post.values())

    if total_files == 0:
        print("No suspicious mixed-resolution posts found.")
        return False

    print(f"Found {total_files} suspicious file(s) across {total_posts} post(s).")
    for post_id in sorted(suspicious_by_post):
        print(f"- {post_id}: {len(suspicious_by_post[post_id])} suspicious file(s)")
        for item in suspicious_by_post[post_id]:
            print(
                "  "
                f"{item['path']} | "
                f"{item['width']}x{item['height']} | "
                f"area_ratio={item['area_ratio']:.2f} | "
                f"side_ratio={item['side_ratio']:.2f}"
            )
    return True


def format_candidate(candidate):
    if candidate.width is None or candidate.height is None:
        resolution = "unknown"
    else:
        resolution = f"{candidate.width}x{candidate.height}"

    return (
        f"{candidate.path} | "
        f"{resolution} | "
        f"{candidate.file_size} bytes | "
        f"mtime={int(candidate.modified_at)}"
    )


def format_video_candidate(candidate):
    if candidate.width is None or candidate.height is None:
        resolution = "unknown"
    else:
        resolution = f"{candidate.width}x{candidate.height}"

    duration = "unknown" if candidate.duration is None else f"{candidate.duration:.2f}s"
    bitrate = "unknown" if candidate.bitrate is None else f"{candidate.bitrate} bps"

    return (
        f"{candidate.path} | "
        f"{resolution} | "
        f"duration={duration} | "
        f"bitrate={bitrate} | "
        f"{candidate.file_size} bytes | "
        f"mtime={int(candidate.modified_at)}"
    )


def print_duplicate_images(duplicates_by_post):
    total_posts = len(duplicates_by_post)
    total_groups = sum(len(groups) for groups in duplicates_by_post.values())
    total_removals = sum(
        len(group["remove"])
        for groups in duplicates_by_post.values()
        for group in groups
    )

    if total_removals == 0:
        print("No suspicious duplicate images found.")
        return False

    print(
        f"Found {total_removals} duplicate image(s) to remove "
        f"in {total_groups} group(s) across {total_posts} post(s)."
    )
    for post_id in sorted(duplicates_by_post):
        print(f"- {post_id}: {len(duplicates_by_post[post_id])} duplicate group(s)")
        for index, group in enumerate(duplicates_by_post[post_id], start=1):
            print(f"  group {index}: keep")
            print(f"    {format_candidate(group['keep'])}")
            for candidate in group["remove"]:
                print(f"  group {index}: remove")
                print(f"    {format_candidate(candidate)}")
    return True


def print_duplicate_videos(duplicates_by_post):
    total_posts = len(duplicates_by_post)
    total_groups = sum(len(groups) for groups in duplicates_by_post.values())
    total_removals = sum(
        len(group["remove"])
        for groups in duplicates_by_post.values()
        for group in groups
    )

    if total_removals == 0:
        print("No suspicious duplicate videos found.")
        return False

    print(
        f"Found {total_removals} duplicate video(s) to remove "
        f"in {total_groups} group(s) across {total_posts} post(s)."
    )
    for post_id in sorted(duplicates_by_post):
        print(f"- {post_id}: {len(duplicates_by_post[post_id])} duplicate group(s)")
        for index, group in enumerate(duplicates_by_post[post_id], start=1):
            print(f"  group {index}: keep")
            print(f"    {format_video_candidate(group['keep'])}")
            for candidate in group["remove"]:
                print(f"  group {index}: remove")
                print(f"    {format_video_candidate(candidate)}")
    return True


def apply_duplicate_removal(duplicates_by_post):
    removed_files = 0
    for groups in duplicates_by_post.values():
        for group in groups:
            for candidate in group["remove"]:
                if os.path.exists(candidate.path):
                    os.remove(candidate.path)
                    removed_files += 1

    print("")
    print(f"Removed {removed_files} duplicate image(s).")
    print("processed_posts.csv was left unchanged because the best variant was kept.")


def apply_duplicate_video_removal(duplicates_by_post):
    removed_files = 0
    for groups in duplicates_by_post.values():
        for group in groups:
            for candidate in group["remove"]:
                if os.path.exists(candidate.path):
                    os.remove(candidate.path)
                    removed_files += 1

    print("")
    print(f"Removed {removed_files} duplicate video(s).")
    print("processed_posts.csv was left unchanged because the best variant was kept.")


def apply_file_removal_only(target_paths_by_post, label):
    removed_files = 0
    for paths in target_paths_by_post.values():
        for path in paths:
            if os.path.exists(path):
                os.remove(path)
                removed_files += 1

    print("")
    print(f"Removed {removed_files} {label}.")
    print("processed_posts.csv was left unchanged.")


def apply_reset(target_paths_by_post):
    removed_files = 0
    for paths in target_paths_by_post.values():
        for path in paths:
            if os.path.exists(path):
                os.remove(path)
                removed_files += 1

    rows, fieldnames = load_processed_rows()
    if rows and fieldnames:
        target_post_ids = set(target_paths_by_post.keys())
        kept_rows = [row for row in rows if row.get("id") not in target_post_ids]
        removed_rows = len(rows) - len(kept_rows)
        rewrite_processed_rows(kept_rows, fieldnames)
    else:
        removed_rows = 0

    print("")
    print(f"Removed {removed_files} file(s).")
    print(f"Reset {removed_rows} row(s) in processed_posts.csv.")
    print("The next run of main.py can download those posts again.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find corrupted downloads, suspicious mixed-resolution posts, or "
            "duplicate media and optionally clean them up."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["invalid-jpeg", "invalid-images", "mixed-resolution", "duplicate-images", "duplicate-videos"],
        default="invalid-jpeg",
        help="Which scan to run.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the selected cleanup.",
    )
    parser.add_argument(
        "--root-dir",
        default=OUTPUT_DIR,
        help=f"Directory to scan for downloads. Default: {OUTPUT_DIR}",
    )
    parser.add_argument(
        "--area-threshold",
        type=float,
        default=0.60,
        help="For mixed-resolution: minimum area ratio compared to the largest image in a post.",
    )
    parser.add_argument(
        "--side-threshold",
        type=float,
        default=0.75,
        help="For mixed-resolution: minimum longest-side ratio compared to the largest image in a post.",
    )
    parser.add_argument(
        "--min-images",
        type=int,
        default=4,
        help="For mixed-resolution: minimum number of images in a post before it is checked.",
    )
    parser.add_argument(
        "--hash-threshold",
        type=int,
        default=4,
        help="For duplicate-images: maximum Hamming distance between perceptual hashes.",
    )
    parser.add_argument(
        "--duration-tolerance",
        type=float,
        default=0.35,
        help="For duplicate-videos: maximum allowed duration difference in seconds.",
    )
    args = parser.parse_args()

    if args.mode == "invalid-jpeg":
        invalid_paths = sorted(iter_invalid_jpegs(args.root_dir))
        invalid_by_post = defaultdict(list)
        for path in invalid_paths:
            post_id = post_id_from_filename(path) or "unknown"
            invalid_by_post[post_id].append(path)

        found_any = print_invalid_jpegs(invalid_by_post)
        if not found_any:
            return

        if not args.apply:
            print("")
            print("Dry run complete. Use --apply to remove these files.")
            print("processed_posts.csv will be left unchanged.")
            return

        apply_file_removal_only(invalid_by_post, "corrupted JPEG file(s)")
        return

    if args.mode == "invalid-images":
        invalid_paths = sorted(iter_invalid_images(args.root_dir))
        invalid_by_post = defaultdict(list)
        for path in invalid_paths:
            post_id = post_id_from_filename(path) or "unknown"
            invalid_by_post[post_id].append(path)

        found_any = print_invalid_jpegs(invalid_by_post)
        if not found_any:
            return

        if not args.apply:
            print("")
            print("Dry run complete. Use --apply to remove these corrupted images.")
            print("processed_posts.csv will be left unchanged.")
            return

        apply_file_removal_only(invalid_by_post, "corrupted image(s)")
        return

    if args.mode == "mixed-resolution":
        suspicious_by_post = find_mixed_resolution_files(
            args.root_dir,
            area_threshold=args.area_threshold,
            side_threshold=args.side_threshold,
            min_images=args.min_images,
        )

        found_any = print_mixed_resolution(suspicious_by_post)
        if not found_any:
            return

        if not args.apply:
            print("")
            print("Dry run complete. Use --apply to remove these suspicious files")
            print("and make the related posts downloadable again.")
            return

        target_paths_by_post = {
            post_id: [item["path"] for item in items]
            for post_id, items in suspicious_by_post.items()
        }
        apply_reset(target_paths_by_post)
        return

    if args.mode == "duplicate-videos":
        duplicates_by_post = find_duplicate_videos(
            args.root_dir,
            duration_tolerance=args.duration_tolerance,
        )

        found_any = print_duplicate_videos(duplicates_by_post)
        if not found_any:
            return

        if not args.apply:
            print("")
            print("Dry run complete. Use --apply to remove the lower-quality duplicate videos.")
            print("The best variant in each duplicate group is kept, with mtime as a fallback.")
            return

        apply_duplicate_video_removal(duplicates_by_post)
        return

    duplicates_by_post = find_duplicate_images(
        args.root_dir,
        hash_threshold=args.hash_threshold,
    )

    found_any = print_duplicate_images(duplicates_by_post)
    if not found_any:
        return

    if not args.apply:
        print("")
        print("Dry run complete. Use --apply to remove the lower-quality duplicate images.")
        print("The best variant in each duplicate group is kept, with mtime as a fallback.")
        return

    apply_duplicate_removal(duplicates_by_post)


if __name__ == "__main__":
    main()
