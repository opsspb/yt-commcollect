# yt-commcollect

A one-command CLI that retrieves all comments for one or more specified YouTube videos.

## Prerequisites

1. Enable the YouTube Data API v3 in your Google Cloud project.
2. Create an API key and save it in a `token.txt` file placed in the same
   directory as `collect_comments.py` (or point to it via `--token`).

## Usage

```bash
python collect_comments.py <youtube_video_url_or_id> [<more_video_urls_or_ids> ...] \
  [-o OUTPUT] [--token /path/to/token.txt] [--parallel N] [--buffer-size N] [--max-rps N]
```

- The script writes results as JSON Lines (`.jsonl`) **and** CSV (`.csv`), one
  comment per line/row, to `comments_<video_id>.jsonl` and
  `comments_<video_id>.csv` by default when a single video is downloaded.
  Provide `--output` to choose a filename when downloading multiple videos; the
  CSV will share the same basename with a `.csv` extension.
- Multiple videos can be processed at once; set `--parallel` to control the
  number of worker processes (default: 8).
- `--buffer-size` controls how many comments are buffered before flushing to
  disk, and `--max-rps` sets a soft per-worker rate limit to stay within quota.
- Both top-level comments and first-degree replies are captured. Nested replies
  beyond the first level are not included.
- Each comment entry includes the comment ID, optional parent ID, author
  display name, plain-text body, publication timestamp, and like count.

### Examples

```bash
python collect_comments.py https://youtu.be/dQw4w9WgXcQ
python collect_comments.py https://www.youtube.com/watch?v=dQw4w9WgXcQ -o rick_comments.jsonl
python collect_comments.py dQw4w9WgXcQ --token /path/to/token.txt
```
