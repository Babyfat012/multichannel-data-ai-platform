"""Tạo khóa Fernet một lần trên data disk, giữ nguyên khóa khi chạy lại."""

import base64
import os
import secrets
import sys
from pathlib import Path


def main():
    """Ghi độc quyền nếu chưa có khóa và kiểm khóa cũ, tuyệt đối không in giá trị."""
    target = Path(sys.argv[1])
    try:
        with target.open("x") as handle:
            os.chmod(target, 0o600)
            handle.write(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode() + "\n")
        print("created")
    except FileExistsError:
        if len(base64.urlsafe_b64decode(target.read_text().strip())) != 32:
            raise ValueError("Khóa Fernet có sẵn không hợp lệ; không ghi đè") from None
        print("existing")
    os.chown(target, int(sys.argv[2]), 0)
    os.chmod(target, 0o600)


if __name__ == "__main__":
    main()
