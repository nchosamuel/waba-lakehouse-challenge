"""Install the matching kubectl locally with the official SHA256 checksum."""

import hashlib
import platform
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
tools = root / ".tools"
tools.mkdir(exist_ok=True)
os_name = {"Darwin": "darwin", "Linux": "linux"}[platform.system()]
arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
url = f"https://dl.k8s.io/release/v1.32.3/bin/{os_name}/{arch}/kubectl"
target = tools / "kubectl"
subprocess.run(
    [
        "curl",
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        url,
        "-o",
        str(target),
    ],
    check=True,
)
checksum = subprocess.check_output(
    ["curl", "--fail", "--silent", "--show-error", "--location", url + ".sha256"],
    text=True,
).strip()
if hashlib.sha256(target.read_bytes()).hexdigest() != checksum:
    raise RuntimeError("kubectl checksum mismatch; do not execute the download")
target.chmod(0o755)
print("Matching kubectl installed in .tools (ignored by Git).")
