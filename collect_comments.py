"""CLI script to download YouTube comments for a given video.

Reads an API key from ``token.txt`` located alongside the script and
writes comments (including top-level and first-degree replies) to a
JSON Lines file. Each JSON object contains the comment ID, optional
parent ID, author, text, publication timestamp, and like count.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional
from urllib.error import HTTPError
from urllib.parse import ParseResult, parse_qs, urlencode, urlparse
from urllib.request import urlopen

API_BASE = "https://www.googleapis.com/youtube/v3"


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


def _perform_get(endpoint: str, params: Dict[str, str]) -> Dict:
    """Perform a GET request against the YouTube Data API and parse JSON response."""
    encoded = urlencode(params)
    url = f"{API_BASE}/{endpoint}?{encoded}"
    try:
        with urlopen(url) as response:
            return json.loads(response.read())
    except HTTPError as err:
        error_detail = err.read().decode("utf-8", errors="ignore") if err.fp else err.reason
        raise RuntimeError(f"API request failed ({err.code}): {error_detail}")


def iter_comment_threads(video_id: str, api_key: str) -> Iterator[Dict]:
    """Iterate over all comment threads (top-level comments) for a video."""
    page_token: Optional[str] = None

    while True:
        params = {
            "part": "snippet,replies",
            "videoId": video_id,
            "maxResults": "100",
            "textFormat": "plainText",
            "pageToken": page_token or "",
            "key": api_key,
        }
        data = _perform_get("commentThreads", params)

        for thread in data.get("items", []):
            yield thread

        page_token = data.get("nextPageToken")
        if not page_token:
            break


def iter_replies(parent_id: str, api_key: str) -> Iterator[Dict]:
    """Iterate over all first-degree replies to a top-level comment."""
    page_token: Optional[str] = None

    while True:
        params = {
            "part": "snippet",
            "parentId": parent_id,
            "maxResults": "100",
            "textFormat": "plainText",
            "pageToken": page_token or "",
            "key": api_key,
        }
        data = _perform_get("comments", params)

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


def collect_comments(video_id: str, api_key: str) -> Iterable[Dict[str, object]]:
    """Yield all comments (top-level and first-degree replies) for the video."""
    for thread in iter_comment_threads(video_id, api_key):
        top_comment = thread["snippet"]["topLevelComment"]
        yield build_comment_payload(top_comment, parent_id=None)

        total_replies = thread["snippet"].get("totalReplyCount", 0)
        if total_replies:
            parent_id = top_comment.get("id")
            for reply in iter_replies(parent_id, api_key):
                yield build_comment_payload(reply, parent_id=parent_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all YouTube comments (top-level and first replies) for a given video."
    )
    parser.add_argument("video", help="YouTube video URL or 11-character video ID")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    api_key = load_api_key(args.token)
    video_id = extract_video_id(args.video)

    output_path = Path(args.output)
    with output_path.open("w", encoding="utf-8") as outfile:
        for comment in collect_comments(video_id, api_key):
            outfile.write(json.dumps(comment, ensure_ascii=False) + "\n")

    print(f"Wrote comments to {output_path.resolve()}")


if __name__ == "__main__":
    main()
