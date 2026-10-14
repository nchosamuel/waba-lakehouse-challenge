"""Local NiFi API adapter. Self-signed TLS is confined to localhost in the demo."""

import json
import ssl
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def private_env():
    return dict(
        line.split("=", 1)
        for line in (ROOT / ".env").read_text().splitlines()
        if "=" in line
    )


class NiFi:
    def __init__(self):
        self.url = "https://localhost:8443/nifi-api"
        self.context = ssl._create_unverified_context()
        env = private_env()
        request = urllib.request.Request(
            self.url + "/access/token",
            data=urllib.parse.urlencode(
                {
                    "username": env["NIFI_ADMIN_USER"],
                    "password": env["NIFI_ADMIN_PASSWORD"],
                }
            ).encode(),
        )
        with urllib.request.urlopen(
            request, context=self.context, timeout=30
        ) as response:
            self.token = response.read().decode()

    def call(self, path, method="GET", body=None):
        request = urllib.request.Request(
            self.url + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(
            request, context=self.context, timeout=30
        ) as response:
            data = response.read().decode()
            return json.loads(data) if data else {}
