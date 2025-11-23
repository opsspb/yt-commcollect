import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Dict, Tuple
from unittest import mock
from urllib.error import HTTPError

import collect_comments


def _mock_perform_get_factory():
    thread_pages: Dict[Tuple[str, str], Dict] = {}
    reply_pages: Dict[Tuple[str, str], Dict] = {}

    def add_video(video_id: str) -> None:
        thread_pages[(video_id, "")] = {
            "items": [
                {
                    "id": f"thread-{video_id}-1",
                    "snippet": {
                        "topLevelComment": {
                            "id": f"top-{video_id}-1",
                            "snippet": {
                                "authorDisplayName": "Author 1",
                                "likeCount": 1,
                                "publishedAt": "2020-01-01T00:00:00Z",
                                "textOriginal": "First",
                            },
                        },
                        "totalReplyCount": 1,
                    },
                },
                {
                    "id": f"thread-{video_id}-2",
                    "snippet": {
                        "topLevelComment": {
                            "id": f"top-{video_id}-2",
                            "snippet": {
                                "authorDisplayName": "Author 2",
                                "likeCount": 0,
                                "publishedAt": "2020-01-02T00:00:00Z",
                                "textOriginal": "Second",
                            },
                        },
                        "totalReplyCount": 0,
                    },
                },
            ],
            "nextPageToken": None,
            "pageInfo": {"totalResults": 2},
        }

        reply_pages[(f"top-{video_id}-1", "")] = {
            "items": [
                {
                    "id": f"reply-{video_id}-1",
                    "snippet": {
                        "authorDisplayName": "Replier",
                        "likeCount": 0,
                        "publishedAt": "2020-01-03T00:00:00Z",
                        "textOriginal": "Reply",
                    },
                }
            ],
            "nextPageToken": None,
        }

    def fake_perform_get(endpoint: str, params: Dict[str, str], *, rate_limiter):
        if endpoint == "commentThreads":
            key = (params.get("videoId"), params.get("pageToken", ""))
            return thread_pages[key]
        if endpoint == "comments":
            key = (params.get("parentId"), params.get("pageToken", ""))
            return reply_pages[key]
        raise AssertionError(f"Unexpected endpoint {endpoint}")

    add_video("vid00000001")
    add_video("vid00000002")
    return fake_perform_get


class CollectCommentsTests(unittest.TestCase):
    def test_collect_comments_single_video(self):
        fake_perform_get = _mock_perform_get_factory()
        limiter = collect_comments.RateLimiter(max_requests_per_second=100)

        with mock.patch("collect_comments._perform_get", side_effect=fake_perform_get):
            comments = list(
                collect_comments.collect_comments(
                    "vid00000001", "token", rate_limiter=limiter, progress_callback=None
                )
            )

        self.assertEqual(len(comments), 3)
        self.assertEqual(comments[0]["id"], "top-vid00000001-1")
        self.assertEqual(comments[1]["parent_id"], "top-vid00000001-1")
        self.assertEqual(comments[2]["id"], "top-vid00000001-2")

    def test_parallel_download_merges_and_cleans(self):
        fake_perform_get = _mock_perform_get_factory()
        out_dir = Path(tempfile.mkdtemp())
        out_path = out_dir / "output.jsonl"
        temp_root = out_dir / "temp"
        temp_root.mkdir(parents=True, exist_ok=True)

        try:
            with mock.patch(
                "collect_comments._perform_get", side_effect=fake_perform_get
            ), mock.patch("collect_comments.load_api_key", return_value="token"), mock.patch(
                "tempfile.mkdtemp", return_value=str(temp_root)
            ):
                argv = [
                    "collect_comments.py",
                    "vid00000001",
                    "vid00000002",
                    "--parallel",
                    "2",
                    "--output",
                    str(out_path),
                    "--buffer-size",
                    "1",
                ]
                with mock.patch.object(sys, "argv", argv):
                    collect_comments.main()

            self.assertTrue(out_path.exists())
            with out_path.open() as infile:
                lines = [json.loads(line) for line in infile]

            ids = {comment["id"] for comment in lines}
            self.assertEqual(
                ids,
                {
                    "top-vid00000001-1",
                    "reply-vid00000001-1",
                    "top-vid00000001-2",
                    "top-vid00000002-1",
                    "reply-vid00000002-1",
                    "top-vid00000002-2",
                },
            )
            self.assertFalse(temp_root.exists())
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)

    def test_quota_error_is_reported_cleanly(self):
        quota_response = {
            "error": {
                "code": 403,
                "message": "The request cannot be completed because you have exceeded your quota.",
                "errors": [
                    {
                        "message": "The request cannot be completed because you have exceeded your quota.",
                        "domain": "youtube.quota",
                        "reason": "quotaExceeded",
                    }
                ],
            }
        }

        response_body = json.dumps(quota_response).encode("utf-8")
        quota_error = HTTPError(
            "https://www.googleapis.com/youtube/v3/comments", 403, "Forbidden", hdrs=None, fp=io.BytesIO(response_body)
        )

        limiter = collect_comments.RateLimiter(max_requests_per_second=100)

        with mock.patch("collect_comments.urlopen", side_effect=quota_error):
            with self.assertRaises(collect_comments.QuotaExceededError):
                collect_comments._perform_get("comments", {"key": "value"}, rate_limiter=limiter)
