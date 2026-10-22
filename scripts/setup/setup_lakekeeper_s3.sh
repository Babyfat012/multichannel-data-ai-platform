#!/usr/bin/env bash
# Khởi tạo warehouse S3 bằng IAM role của EC2; không dùng access key, không sửa warehouse cũ.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
set -a
source .env
set +a
: "${LAKEHOUSE_BUCKET:?Cần LAKEHOUSE_BUCKET}"
: "${AWS_REGION:?Cần AWS_REGION}"
python3 - <<'PY'
# Gọi Management API cục bộ, lỗi HTTP phải dừng triển khai.
import json
import os
import urllib.request


def request(path, body=None):
    """Đọc hoặc gửi JSON tới Lakekeeper trên loopback VM."""
    url = 'http://127.0.0.1:' + os.environ.get('LAKEKEEPER_PORT', '8181')
    req = urllib.request.Request(url + '/management/v1/' + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=120) as response:
        raw = response.read()
        return json.loads(raw) if raw else {}


if not request('info')['bootstrapped']:
    request('bootstrap', {'accept-terms-of-use': True})
name = os.environ.get('WAREHOUSE_NAME', 'fsds-aws')
expected = {'type': 's3', 'bucket': os.environ['LAKEHOUSE_BUCKET'], 'key-prefix': 'lakehouse',
            'region': os.environ['AWS_REGION']}
warehouses = request('warehouse')['warehouses']
existing = next((w for w in warehouses if w['name'] == name), None)
if existing:
    actual = request('warehouse/' + existing['id'])['storage-profile']
    if any(actual.get(k) != v for k, v in expected.items()):
        raise SystemExit('Warehouse cùng tên có storage profile khác; từ chối thay đổi.')
    print('Warehouse đã đúng cấu hình; bỏ qua.')
else:
    # sts-enabled=False: client tự dùng IAM role của EC2, Lakekeeper không vend credential.
    request('warehouse', {'warehouse-name': name,
        'storage-profile': {**expected, 'flavor': 'aws', 'sts-enabled': False},
        'storage-credential': {'type': 's3', 'credential-type': 'aws-system-identity'}})
    print('Đã tạo warehouse S3.')
PY
