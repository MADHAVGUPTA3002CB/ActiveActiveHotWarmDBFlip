from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping


class ConnectError(RuntimeError):
    pass


_CREDENTIAL_FIELD = re.compile(
    r'(?i)("?(?:database\.password|connection\.password|password|authorization)"?\s*[:=]\s*)("[^"]*"|[^,\s}]+)'
)
_URL_CREDENTIALS = re.compile(r"(://[^:/\s]+:)[^@/\s]+@")


def redact_error_detail(detail: str) -> str:
    bounded = detail[:2048]
    bounded = _CREDENTIAL_FIELD.sub(r'\1"[REDACTED]"', bounded)
    return _URL_CREDENTIALS.sub(r"\1[REDACTED]@", bounded)[:2048]


@dataclass(frozen=True, slots=True)
class ConnectClient:
    base_url: str
    timeout_seconds: float = 10.0

    def _request(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> Any:
        encoded = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=encoded,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            detail = redact_error_detail(error.read().decode("utf-8", errors="replace"))
            raise ConnectError(f"Connect {method} {path} failed: HTTP {error.code}: {detail}") from error
        except urllib.error.URLError as error:
            raise ConnectError(f"Connect {method} {path} failed: {error.reason}") from error
        return None if not payload else json.loads(payload)

    def plugins(self) -> tuple[str, ...]:
        payload = self._request("GET", "/connector-plugins")
        return tuple(sorted(item["class"] for item in payload))

    def put_config(self, connector: str, config: Mapping[str, Any]) -> None:
        self._request("PUT", f"/connectors/{connector}/config", config)

    def delete_if_exists(self, connector: str) -> None:
        try:
            self._request("DELETE", f"/connectors/{connector}")
        except ConnectError as error:
            if "HTTP 404" not in str(error):
                raise

    def status(self, connector: str) -> Mapping[str, Any]:
        return self._request("GET", f"/connectors/{connector}/status")

    def config(self, connector: str) -> Mapping[str, str]:
        return self._request("GET", f"/connectors/{connector}/config")

    def set_paused(self, connector: str, paused: bool) -> None:
        action = "pause" if paused else "resume"
        self._request("PUT", f"/connectors/{connector}/{action}")

    def wait_state(self, connector: str, expected: str, deadline_seconds: float = 60.0) -> None:
        deadline = time.monotonic() + deadline_seconds
        last: Mapping[str, Any] | None = None
        while time.monotonic() < deadline:
            try:
                last = self.status(connector)
            except ConnectError as error:
                # Connect can acknowledge PUT /config before the distributed
                # status backing store exposes the new connector.
                if "HTTP 404" not in str(error):
                    raise
                time.sleep(0.25)
                continue
            connector_state = str(last.get("connector", {}).get("state", ""))
            task_states = tuple(str(task.get("state", "")) for task in last.get("tasks", ()))
            tasks_match = bool(task_states) and all(state == expected for state in task_states)
            if connector_state == expected and tasks_match:
                return
            time.sleep(0.25)
        raise ConnectError(f"connector {connector} did not reach {expected}; last status={last!r}")
