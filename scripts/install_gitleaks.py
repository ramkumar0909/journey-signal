"""Install the pinned scanner into ignored artifacts, verifying published SHA-256."""

import hashlib
import io
import platform
import tarfile
import urllib.request
from pathlib import Path

VERSION = "8.30.1"
CHECKSUMS = {
    ("Darwin", "arm64"): (
        "darwin_arm64",
        "b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5",
    ),
    ("Linux", "x86_64"): (
        "linux_x64",
        "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb",
    ),
}


def main() -> None:
    target, expected = CHECKSUMS[(platform.system(), platform.machine())]
    url = f"https://github.com/gitleaks/gitleaks/releases/download/v{VERSION}/gitleaks_{VERSION}_{target}.tar.gz"
    with urllib.request.urlopen(url, timeout=30) as response:  # nosec B310: fixed HTTPS origin
        data = response.read(32 * 1024 * 1024)
    if hashlib.sha256(data).hexdigest() != expected:
        raise RuntimeError("Scanner checksum mismatch")
    directory = Path("artifacts/tools")
    directory.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        member = archive.getmember("gitleaks")
        archive.extract(member, directory, filter="data")
    (directory / "gitleaks").chmod(0o755)
    print(f"Installed verified gitleaks {VERSION}")


if __name__ == "__main__":
    main()
