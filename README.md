# yt-commcollect

A one-command CLI that retrieves all comments for a specified YouTube video.

## Prerequisites

1. Enable the YouTube Data API v3 in your Google Cloud project.
2. Create an API key and save it in a `token.txt` file placed in the same
   directory as `collect_comments.py` (or point to it via `--token`).

## Usage

```bash
python collect_comments.py <youtube_video_url_or_id> [-o OUTPUT] [--token /path/to/token.txt]
```

- The script writes results as JSON Lines (`.jsonl`), one comment object per
  line, to `comments.jsonl` by default.
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
