"""CLI script to download YouTube comments for a given video.

Reads an API key from ``token.txt`` located alongside the script and
writes comments (including top-level and first-degree replies) to a
JSON Lines file. Each JSON object contains the comment ID, optional
parent ID, author, text, publication timestamp, and like count.
"""

from __future__ import annotations

import argparse
import json
import shutil
import multiprocessing
import tempfile
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, Optional, Sequence
from urllib.error import HTTPError
from urllib.parse import ParseResult, parse_qs, urlencode, urlparse
from urllib.request import urlopen

API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_BUFFER_SIZE = 1000
DEFAULT_PARALLELISM = 8
DEFAULT_MAX_RPS = 25


class RateLimiter:
    """A lightweight per-process rate limiter using a sliding window."""

    def __init__(self, max_requests_per_second: float) -> None:
        self.max_requests_per_second = max_requests_per_second
        self._timestamps: deque[float] = deque()

    def acquire(self) -> None:
        if self.max_requests_per_second <= 0:
            return

        now = time.monotonic()
        window_start = now - 1.0

        while self._timestamps and self._timestamps[0] < window_start:
            self._timestamps.popleft()

        if len(self._timestamps) >= self.max_requests_per_second:
            earliest = self._timestamps[0]
            sleep_for = max(0.0, 1.0 - (now - earliest))
            if sleep_for:
                time.sleep(sleep_for)

        self._timestamps.append(time.monotonic())


def load_api_key(token_path: Path) -> str:
    """Load the YouTube Data API key from the provided path."""
    if not token_path.exists():
        raise FileNotFoundError(
            f"No API token found at {token_path}. Provide a token.txt file with your GCP API key."
        )

    api_key = token_path.read_text(encoding="utf-8").strip()
    if not api_key:
        raise ValueError("token.txt is empty; populate it with your YouTube Data API key.")

    return api_key


def extract_video_id(url_or_id: str) -> str:
    """Extract the YouTube video ID from a URL or return the input if it already looks like an ID."""

    # Heuristic: if the string has no URL components, treat it as an ID
    parsed: ParseResult = urlparse(url_or_id)
    if not parsed.scheme and not parsed.netloc and len(url_or_id) == 11:
        return url_or_id

    if parsed.netloc in {"youtu.be", "www.youtu.be"}:
        # Short link: https://youtu.be/<id>
        return parsed.path.lstrip("/")

    if parsed.netloc in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        if parsed.path.startswith("/watch"):
            query = parse_qs(parsed.query)
            if "v" in query:
                return query["v"][0]
        # Embedded form: https://www.youtube.com/embed/<id>
        if parsed.path.startswith("/embed/"):
            return parsed.path.split("/", maxsplit=2)[2]
        # Shorts: https://www.youtube.com/shorts/<id>
        if parsed.path.startswith("/shorts/"):
            return parsed.path.split("/", maxsplit=2)[2]

    raise ValueError(f"Unable to extract video ID from '{url_or_id}'. Provide a standard YouTube URL or video ID.")


def _perform_get(endpoint: str, params: Dict[str, str], *, rate_limiter: RateLimiter) -> Dict:
    """Perform a GET request against the YouTube Data API and parse JSON response."""

    rate_limiter.acquire()
    encoded = urlencode(params)
    url = f"{API_BASE}/{endpoint}?{encoded}"
    try:
        with urlopen(url) as response:
            return json.loads(response.read())
    except HTTPError as err:
        error_detail = err.read().decode("utf-8", errors="ignore") if err.fp else err.reason
        raise RuntimeError(f"API request failed ({err.code}): {error_detail}")


def iter_comment_threads(
    video_id: str, api_key: str, *, rate_limiter: RateLimiter
) -> Iterator[tuple[Dict, Optional[int]]]:
    """Iterate over all comment threads (top-level comments) for a video.

    The first yielded item will include the total number of comment threads
    reported by the API (when available) to facilitate progress estimation.
    """

    page_token: Optional[str] = None
    total_threads_reported: Optional[int] = None
    reported_total = False

    while True:
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": "100",
            "textFormat": "plainText",
            "pageToken": page_token or "",
            # Request only the fields needed for building the payload to reduce payload size.
            "fields": "items(id,snippet/topLevelComment/id,snippet/topLevelComment/snippet(authorDisplayName,likeCount,publishedAt,textOriginal),snippet/totalReplyCount),nextPageToken,pageInfo/totalResults",
            "key": api_key,
        }
        data = _perform_get("commentThreads", params, rate_limiter=rate_limiter)

        if total_threads_reported is None:
            total_threads_reported = data.get("pageInfo", {}).get("totalResults")

        for thread in data.get("items", []):
            yield thread, None if reported_total else total_threads_reported
            reported_total = True

        page_token = data.get("nextPageToken")
        if not page_token:
            break


def iter_replies(parent_id: str, api_key: str, *, rate_limiter: RateLimiter) -> Iterator[Dict]:
    """Iterate over all first-degree replies to a top-level comment."""
    page_token: Optional[str] = None

    while True:
        params = {
            "part": "snippet",
            "parentId": parent_id,
            "maxResults": "100",
            "textFormat": "plainText",
            "pageToken": page_token or "",
            # Partial response for reply payload construction only.
            "fields": "items(id,snippet/authorDisplayName,snippet/likeCount,snippet/publishedAt,snippet/textOriginal),nextPageToken",
            "key": api_key,
        }
        data = _perform_get("comments", params, rate_limiter=rate_limiter)

        for item in data.get("items", []):
            yield item

        page_token = data.get("nextPageToken")
        if not page_token:
            break


def build_comment_payload(item: Dict, *, parent_id: Optional[str] = None) -> Dict[str, object]:
    snippet = item["snippet"]
    return {
        "id": item.get("id"),
        "parent_id": parent_id,
        "author": snippet.get("authorDisplayName"),
        "text": snippet.get("textOriginal"),
        "published_at": snippet.get("publishedAt"),
        "like_count": snippet.get("likeCount", 0),
    }


def collect_comments(
    video_id: str,
    api_key: str,
    *,
    rate_limiter: RateLimiter,
    progress_callback: Optional[Callable[[int, Optional[int]], None]] = None,
) -> Iterable[Dict[str, object]]:
    """Yield all comments (top-level and first-degree replies) for the video."""

    total_estimated: Optional[int] = None
    processed = 0

    for thread, thread_total in iter_comment_threads(video_id, api_key, rate_limiter=rate_limiter):
        if total_estimated is None and thread_total is not None:
            total_estimated = thread_total

        top_comment = thread["snippet"]["topLevelComment"]
        processed += 1
        if progress_callback:
            progress_callback(processed, total_estimated)
        yield build_comment_payload(top_comment, parent_id=None)

        total_replies = thread["snippet"].get("totalReplyCount", 0)
        if total_replies and total_estimated is not None:
            total_estimated += total_replies

        if total_replies:
            parent_id = top_comment.get("id")
            for reply in iter_replies(parent_id, api_key, rate_limiter=rate_limiter):
                processed += 1
                if progress_callback:
                    progress_callback(processed, total_estimated)
                yield build_comment_payload(reply, parent_id=parent_id)


def print_progress(processed: int, total: Optional[int]) -> None:
    if total and total > 0:
        percent = (processed / total) * 100
        line = f"Progress: {percent:.1f}% ({processed}/{total})"
    else:
        line = f"Progress: {processed} processed"
    print(f"\r{line}", end="", flush=True)


def _write_buffer(outfile, buffer: list[Dict[str, object]]) -> None:
    if not buffer:
        return
    for comment in buffer:
        outfile.write(json.dumps(comment, ensure_ascii=False) + "\n")
    buffer.clear()


def download_video_comments(
    video_input: str,
    api_key: str,
    temp_dir: Path,
    buffer_size: int,
    max_rps: float,
    *,
    show_progress: bool,
) -> tuple[str, Path, int]:
    video_id = extract_video_id(video_input)
    temp_path = temp_dir / f"{video_id}.jsonl"
    rate_limiter = RateLimiter(max_requests_per_second=max_rps)

    progress_cb: Optional[Callable[[int, Optional[int]], None]] = print_progress if show_progress else None
    written = 0
    buffer: list[Dict[str, object]] = []
    with temp_path.open("w", encoding="utf-8") as outfile:
        for comment in collect_comments(
            video_id,
            api_key,
            rate_limiter=rate_limiter,
            progress_callback=progress_cb,
        ):
            buffer.append(comment)
            written += 1
            if len(buffer) >= buffer_size:
                _write_buffer(outfile, buffer)
        _write_buffer(outfile, buffer)

    if show_progress:
        print()

    return video_id, temp_path, written


def merge_temp_files(temp_files: Sequence[Path], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as outfile:
        for path in temp_files:
            with path.open("r", encoding="utf-8") as infile:
                shutil.copyfileobj(infile, outfile)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all YouTube comments (top-level and first replies) for a given video.",
    )
    parser.add_argument(
        "videos",
        nargs="+",
        help="One or more YouTube video URLs or 11-character video IDs",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="comments.jsonl",
        help="Path to write JSON Lines output (default: comments.jsonl)",
    )
    parser.add_argument(
        "--token",
        type=Path,
        default=Path(__file__).resolve().parent / "token.txt",
        help="Path to token.txt containing YouTube Data API key (default: alongside this script)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=DEFAULT_PARALLELISM,
        help="Maximum number of videos to download in parallel (default: %(default)s)",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=DEFAULT_BUFFER_SIZE,
        help="Number of comments to buffer in memory before flushing to disk (default: %(default)s)",
    )
    parser.add_argument(
        "--max-rps",
        type=float,
        default=DEFAULT_MAX_RPS,
        help="Soft limit on requests per second per worker (default: %(default)s)",
    )
    return parser.parse_args()




def main() -> None:
    args = parse_args()

    api_key = load_api_key(args.token)
    output_path = Path(args.output)

    temp_dir = Path(tempfile.mkdtemp(prefix="yt-comments-"))
    temp_files: list[Path] = []
    total_videos = len(args.videos)
    processed = 0

    try:
        if args.parallel > 1 and total_videos > 1:
            try:
                ctx = multiprocessing.get_context("fork")
            except ValueError:
                ctx = multiprocessing.get_context()

            with ProcessPoolExecutor(max_workers=args.parallel, mp_context=ctx) as executor:
                futures = {
                    executor.submit(
                        download_video_comments,
                        video,
                        api_key,
                        temp_dir,
                        args.buffer_size,
                        args.max_rps,
                        show_progress=False,
                    ): video
                    for video in args.videos
                }

                for future in as_completed(futures):
                    _, temp_path, _ = future.result()
                    temp_files.append(temp_path)
                    processed += 1
                    print(
                        f"\rVideos processed: {processed}/{total_videos}",
                        end="",
                        flush=True,
                    )
        else:
            for idx, video in enumerate(args.videos, start=1):
                _, temp_path, _ = download_video_comments(
                    video,
                    api_key,
                    temp_dir,
                    args.buffer_size,
                    args.max_rps,
                    show_progress=True,
                )
                temp_files.append(temp_path)
                processed = idx
                print(f"\rVideos processed: {processed}/{total_videos}", end="", flush=True)

        print()
        merge_temp_files(temp_files, output_path)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Wrote comments to {output_path.resolve()}")

if __name__ == "__main__":
    main()
