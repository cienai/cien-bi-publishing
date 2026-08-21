import hashlib
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import requests

import bi_publishing


class IntegrationHubDownloadTests(unittest.TestCase):
    def test_requires_github_token(self):
        with self.assertRaisesRegex(ValueError, "github_token is required"):
            bi_publishing.download_file_from_integration_hub(
                "staging",
                "Dataset.pbix",
                "Dataset.pbix",
                github_token=" ",
            )

    def test_resolves_private_ref_with_bearer_token(self):
        response = MagicMock()
        response.json.return_value = [{"sha": "abc123"}]

        with patch("bi_publishing.requests.get", return_value=response) as get:
            commit = bi_publishing.get_integration_hub_commit(
                "release/tag",
                github_token=" test-token ",
            )

        self.assertEqual(commit, "abc123")
        get.assert_called_once_with(
            "https://api.github.com/repos/cienai/IntegrationHub/commits",
            headers={
                "Authorization": "Bearer test-token",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            params={"sha": "release/tag", "per_page": 1},
            timeout=(10, 60),
        )
        response.raise_for_status.assert_called_once_with()

    def test_downloads_and_verifies_private_lfs_asset(self):
        payload = b"firstsecond"
        oid = hashlib.sha256(payload).hexdigest()
        pointer_response = MagicMock()
        pointer_response.content = (
            "version https://git-lfs.github.com/spec/v1\n"
            f"oid sha256:{oid}\n"
            f"size {len(payload)}\n"
        ).encode()
        download_response = MagicMock()
        download_response.__enter__.return_value = download_response
        download_response.iter_content.return_value = [b"first", b"", b"second"]
        batch_response = MagicMock()
        batch_response.json.return_value = {
            "objects": [{
                "oid": oid,
                "size": len(payload),
                "actions": {
                    "download": {
                        "href": "https://objects.example/download",
                        "header": {"X-Test": "download-header"},
                    }
                },
            }]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "Dataset.pbix")
            with patch(
                "bi_publishing.requests.get",
                side_effect=[pointer_response, download_response],
            ) as get, patch(
                "bi_publishing.requests.post",
                return_value=batch_response,
            ) as post:
                bi_publishing.download_file_from_integration_hub(
                    "release/tag",
                    "Dataset & Report.pbix",
                    destination,
                    github_token="test-token",
                )

            with open(destination, "rb") as downloaded_file:
                self.assertEqual(downloaded_file.read(), payload)

        self.assertEqual(get.call_count, 2)
        pointer_call, download_call = get.call_args_list
        self.assertEqual(
            pointer_call.args[0],
            "https://api.github.com/repos/cienai/IntegrationHub/contents/"
            "powerbi/Dataset%20%26%20Report.pbix",
        )
        self.assertEqual(pointer_call.kwargs["params"], {"ref": "release/tag"})
        self.assertEqual(download_call.args[0], "https://objects.example/download")
        self.assertEqual(download_call.kwargs["headers"], {"X-Test": "download-header"})
        post.assert_called_once()
        pointer_response.raise_for_status.assert_called_once_with()
        batch_response.raise_for_status.assert_called_once_with()
        download_response.raise_for_status.assert_called_once_with()

    def test_failed_download_does_not_replace_existing_file(self):
        pointer_response = MagicMock()
        pointer_response.raise_for_status.side_effect = requests.HTTPError("not found")

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "Dataset.pbix")
            with open(destination, "wb") as existing_file:
                existing_file.write(b"existing")

            with patch("bi_publishing.requests.get", return_value=pointer_response):
                with self.assertRaises(requests.HTTPError):
                    bi_publishing.download_file_from_integration_hub(
                        "missing",
                        "Dataset.pbix",
                        destination,
                        github_token="test-token",
                    )

            with open(destination, "rb") as existing_file:
                self.assertEqual(existing_file.read(), b"existing")
            self.assertFalse(os.path.exists(f"{destination}.part"))


if __name__ == "__main__":
    unittest.main()
