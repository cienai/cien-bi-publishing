import sys
import unittest
from unittest.mock import Mock, patch

sys.modules.setdefault("msal", Mock())

import bi_publishing


class RefreshDatasetTests(unittest.TestCase):
    @patch("bi_publishing.requests.post")
    @patch("bi_publishing._get_headers", return_value={"Authorization": "Bearer token"})
    def test_uses_serial_enhanced_refresh_and_returns_status_url(
        self, _mock_headers, mock_post
    ):
        response = Mock(
            status_code=202,
            headers={"Location": "https://api.powerbi.com/refreshes/request-id"},
            text="",
        )
        mock_post.return_value = response

        result = bi_publishing.refresh_dataset_in_group(
            Mock(), "workspace-id", "dataset-id"
        )

        self.assertEqual(result, response.headers["Location"])
        mock_post.assert_called_once_with(
            f"{bi_publishing.POWERBI_BASE_URL}/groups/workspace-id/datasets/dataset-id/refreshes",
            headers={"Authorization": "Bearer token"},
            json={
                "type": "Full",
                "commitMode": "transactional",
                "maxParallelism": 1,
                "retryCount": 0,
            },
        )


if __name__ == "__main__":
    unittest.main()
