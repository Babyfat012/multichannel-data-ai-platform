"""Điều khiển restart unbounded: checkpoint có state, restore, output commit và REST cancel."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


TERMINAL = ('FINISHED', 'FAILED', 'CANCELED')


def get(base, path):
    """Đọc REST JSON với timeout ngắn để harness không treo vô hạn."""
    with urlopen(base + path, timeout=3) as response:
        return json.load(response)


def cancel(base, job):
    """Yêu cầu Flink cancel job qua REST và đóng response dù body rỗng."""
    request = Request(base + f'/jobs/{job}?mode=cancel', method='PATCH')
    with urlopen(request, timeout=3):
        pass


def count_output(root, epoch):
    """Đếm dòng đúng epoch trong file commit, bỏ file/thư mục ẩn và metadata."""
    root = Path(root)
    count = 0
    for path in root.rglob('*'):
        if not path.is_file() or any(p.startswith(('.', '_')) for p in path.relative_to(root).parts):
            continue
        for line in path.read_text().splitlines():
            if line.strip() and json.loads(line)['epoch'] == epoch:
                count += 1
    return count


def wait_for(step, timeout, probe):
    """Poll với hạn riêng từng bước, ghi lỗi REST gần nhất khi hết hạn."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except (URLError, TimeoutError, ConnectionError) as error:
            last_error = str(error)
        time.sleep(.1)
    raise TimeoutError(f'{step}: timeout after {timeout}s; last REST error={last_error}')


def control(base, taskmanager, evidence, oracle, output, timeout=300):
    """Chỉ công nhận restart sau restore và checkpoint commit đủ kept đúng oracle."""
    expected = json.loads(Path(oracle).read_text())
    epoch = expected['epoch']
    expected_kept = sum(row['kept'] for row in expected['dedup'])
    job_name = f'order-lifecycle-stream-dedup-{epoch}'
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    timings = {}

    def stateful_checkpoint():
        """Tìm đúng job và checkpoint completed chứa state trước khi restart."""
        jobs = get(base, '/jobs/overview')['jobs']
        for candidate in jobs:
            if candidate['name'] != job_name:
                continue
            if candidate['state'] in TERMINAL:
                raise RuntimeError(f'stateful checkpoint: dedup ended {candidate["state"]}; no restart proof')
            if candidate['state'] == 'RUNNING':
                checkpoint = get(base, f'/jobs/{candidate["jid"]}/checkpoints')
                completed = checkpoint.get('latest', {}).get('completed')
                if completed and completed.get('state_size', 0) > 0:
                    return candidate['jid'], checkpoint
        return None

    job, before = wait_for('stateful checkpoint', timeout, stateful_checkpoint)
    timings['stateful_checkpoint_seconds'] = time.monotonic() - started
    restart_started = time.monotonic()
    subprocess.run(['docker', 'stop', '-t', '1', taskmanager], check=True, timeout=timeout)
    subprocess.run(['docker', 'start', taskmanager], check=True, timeout=timeout)

    def running_checkpoints(step):
        """Fail rõ bước nếu job kết thúc trước khi controller chủ động cancel."""
        state = get(base, f'/jobs/{job}')['state']
        if state in TERMINAL:
            raise RuntimeError(f'{step}: job ended {state} before REST cancel')
        return get(base, f'/jobs/{job}/checkpoints')

    def restored_checkpoint():
        """Bắt buộc restore checkpoint ít nhất mới bằng checkpoint có state đã thấy."""
        checkpoint = running_checkpoints('restore')
        restored = checkpoint.get('latest', {}).get('restored')
        if restored and restored['id'] >= before['latest']['completed']['id']:
            return checkpoint
        return None

    restored = wait_for('restore checkpoint', timeout, restored_checkpoint)
    restored_id = restored['latest']['restored']['id']
    timings['restore_seconds'] = time.monotonic() - restart_started
    output_started = time.monotonic()
    kept_path = Path(output)/'dedup'/str(epoch)/'kept'

    def committed_output():
        """Đợi checkpoint mới sau restore và số kept commit bằng oracle, thừa là fail."""
        checkpoint = running_checkpoints('committed kept output')
        count = count_output(kept_path, epoch)
        if count > expected_kept:
            raise RuntimeError(f'committed kept output: got {count}, expected exactly {expected_kept}')
        completed = checkpoint.get('latest', {}).get('completed')
        if count == expected_kept and completed and completed['id'] > restored_id:
            return checkpoint, count
        return None

    after, kept_count = wait_for(f'committed kept output (expected {expected_kept})', timeout, committed_output)
    timings['committed_output_seconds'] = time.monotonic() - output_started
    cancel_started = time.monotonic()
    cancel(base, job)

    def canceled():
        """Chờ xác nhận CANCELED thay vì coi HTTP accepted là đã kết thúc."""
        state = get(base, f'/jobs/{job}')['state']
        if state in ('FINISHED', 'FAILED'):
            raise RuntimeError(f'REST cancel: job ended {state}, expected CANCELED')
        return state == 'CANCELED'

    wait_for('REST cancel confirmation', timeout, canceled)
    # Đọc lại sau cancel để không bỏ qua file commit trong lúc gửi yêu cầu.
    final_count = count_output(kept_path, epoch)
    if final_count != expected_kept:
        raise RuntimeError(f'after REST cancel: kept {final_count}, expected exactly {expected_kept}')
    timings['cancel_seconds'] = time.monotonic() - cancel_started
    timings['total_seconds'] = time.monotonic() - started
    Path(evidence).write_text(json.dumps(dict(
        job_id=job, epoch=epoch, before=before, restored=restored, after=after,
        restored_id=restored_id, expected_kept=expected_kept, kept_count=kept_count,
        final_kept_count=final_count, final_state='CANCELED', started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat(), timings=timings), indent=2)+'\n')


def main():
    """CLI chỉ do planner gọi trong mạng và container riêng của harness."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True)
    parser.add_argument('--taskmanager', required=True)
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--oracle', required=True)
    parser.add_argument('--output', required=True, help='Thư mục host tương ứng /opt/flink/output/order_lifecycle')
    parser.add_argument('--timeout', type=float, default=300, help='Hạn giây riêng mỗi bước poll')
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    control(**vars(args))


if __name__ == '__main__':
    main()
