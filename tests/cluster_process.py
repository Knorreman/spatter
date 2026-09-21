"""Real-process regression: repeated runs and SIGKILL during active dispatch.

Run after cargo build --release --example wordcount (Linux).
"""
import os
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time


def run(path, kill_worker=False):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {k: v for k, v in os.environ.items() if not k.startswith("SPATTER_")}
    env.update(SPATTER_PORT=str(port), SPATTER_CLUSTER_TIMEOUT_MS="2000",
               SPATTER_TASK_LOG="1", SPATTER_METRICS_ADDR="127.0.0.1:0")
    proc = subprocess.Popen(
        ["target/release/examples/wordcount", "--cluster", "3", str(path)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    try:
        if kill_worker:
            deadline = time.monotonic() + 5
            children_file = Path(f"/proc/{proc.pid}/task/{proc.pid}/children")
            while time.monotonic() < deadline:
                children = children_file.read_text().split() if children_file.exists() else []
                if children:
                    worker = int(children[0])
                    # Wait until this worker has opened the input, rather than
                    # killing it during startup/handshake.
                    try:
                        active = any(p.resolve() == path for p in Path(f"/proc/{worker}/fd").iterdir())
                    except FileNotFoundError:
                        active = False
                    if active:
                        os.kill(worker, signal.SIGKILL)
                        break
                time.sleep(0.001)
            else:
                raise AssertionError("did not observe active worker input")
        _, stderr = proc.communicate(timeout=20)
        events = [json.loads(line) for line in stderr.splitlines() if line.startswith('{"event":')]
        assert events, stderr
        assert all({"job", "partition", "attempt", "rank", "scope"} <= event.keys() for event in events)
        finished = [e for e in events if e["event"] == "task_finished"]
        assert finished and all({"duration_us", "success"} <= event.keys() for event in finished)
        assert "METRICS_LISTEN" in stderr, stderr
        if proc.returncode == 0:
            assert "keys=2 sum=6000000 " in stderr, stderr
        else:
            assert kill_worker and "Error:" in stderr, stderr
        print(f"kill_worker={kill_worker}: exit={proc.returncode}")
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "input.txt"
    with path.open("w") as data:
        for _ in range(2000):
            data.write("hello world hello\n" * 1000)
    run(path)
    run(path)
    run(path, kill_worker=True)
