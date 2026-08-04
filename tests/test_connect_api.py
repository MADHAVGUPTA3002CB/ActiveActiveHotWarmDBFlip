import unittest
from unittest.mock import Mock, patch

from flipbench.connect_api import ConnectClient, ConnectError, redact_error_detail


class ConnectClientTests(unittest.TestCase):
    def test_request_timeout_can_be_capped_by_the_caller(self) -> None:
        response = Mock()
        response.read.return_value = b'{"connector":{"state":"RUNNING"},"tasks":[]}'
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        client = ConnectClient("http://connect:8083", timeout_seconds=10.0)

        with patch("urllib.request.urlopen", return_value=response) as urlopen:
            client.status("source", timeout_seconds=0.125)

        self.assertEqual(urlopen.call_args.kwargs["timeout"], 0.125)

    def test_error_detail_redacts_credentials_and_is_bounded(self) -> None:
        detail = '{"database.password":"secret-value","url":"postgresql://user:pass@hot/cards"}' + "x" * 5000
        redacted = redact_error_detail(detail)
        self.assertNotIn("secret-value", redacted)
        self.assertNotIn("user:pass@", redacted)
        self.assertLessEqual(len(redacted), 2048)

    def test_wait_state_retries_transient_missing_status(self) -> None:
        running = {
            "connector": {"state": "RUNNING"},
            "tasks": ({"state": "RUNNING"},),
        }
        client = ConnectClient("http://connect:8083")
        with patch.object(
            ConnectClient,
            "status",
            side_effect=(ConnectError("HTTP 404: no status"), running),
        ) as status:
            client.wait_state("new-connector", "RUNNING", 1.0)
        self.assertEqual(status.call_count, 2)

    def test_wait_state_does_not_hide_non_404_errors(self) -> None:
        client = ConnectClient("http://connect:8083")
        with patch.object(
            ConnectClient,
            "status",
            side_effect=ConnectError("HTTP 500: worker failed"),
        ), self.assertRaises(ConnectError):
            client.wait_state("broken-connector", "RUNNING", 1.0)


if __name__ == "__main__":
    unittest.main()
