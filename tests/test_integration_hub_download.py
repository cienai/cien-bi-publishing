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

    def test_downloads_private_asset_with_bearer_token(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_content.return_value = [b"first", b"", b"second"]

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "Dataset.pbix")
            with patch("bi_publishing.requests.get", return_value=response) as get:
                bi_publishing.download_file_from_integration_hub(
                    "release/tag",
                    "Dataset & Report.pbix",
                    destination,
                    github_token="test-token",
                )

            with open(destination, "rb") as downloaded_file:
                self.assertEqual(downloaded_file.read(), b"firstsecond")

        get.assert_called_once_with(
            "https://api.github.com/repos/cienai/IntegrationHub/contents/"
            "powerbi/Dataset%20%26%20Report.pbix",
            headers={
                "Authorization": "Bearer test-token",
                "Accept": "application/vnd.github.raw+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            params={"ref": "release/tag"},
            stream=True,
            timeout=(10, 300),
        )
        response.raise_for_status.assert_called_once_with()

    def test_failed_download_does_not_replace_existing_file(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.raise_for_status.side_effect = requests.HTTPError("not found")

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = os.path.join(temp_dir, "Dataset.pbix")
            with open(destination, "wb") as existing_file:
                existing_file.write(b"existing")

            with patch("bi_publishing.requests.get", return_value=response):
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
