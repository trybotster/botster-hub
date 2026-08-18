#!/usr/bin/env python3
"""Test-only session-worker wrapper for natural-exit windows.

Core binds readiness and welcome worker_pid to the spawned child. This process
is that child: it prints the ready line, proxies the control socket, and rewrites
welcome worker_pid to its own pid so the real worker can still run the session.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

WELCOME_MAGIC = b"SPA1"


def parse_args(argv: list[str]) -> tuple[list[str], Path | None]:
    rewritten: list[str] = []
    control_socket: Path | None = None
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--control-socket" and index + 1 < len(argv):
            control_socket = Path(argv[index + 1])
            index += 2
            continue
        rewritten.append(arg)
        index += 1
    return rewritten, control_socket


def wait_for_socket(path: Path, timeout_s: float = 2.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise SystemExit(f"inner worker socket did not appear: {path}")


def rewrite_welcome(sock: socket.socket, worker_pid: int, public_socket: Path) -> bytes:
    header = recvall(sock, 9)
    if header[:4] != WELCOME_MAGIC:
        return header
    length = int.from_bytes(header[5:9], "little")
    payload = recvall(sock, length)
    metadata = json.loads(payload.decode("utf-8"))
    identity = metadata.get("recovery_identity") or {}
    identity["worker_pid"] = worker_pid
    identity["worker_control_socket"] = str(public_socket)
    metadata["recovery_identity"] = identity
    encoded = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    return header[:5] + len(encoded).to_bytes(4, "little") + encoded


def recvall(sock: socket.socket, count: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < count:
        piece = sock.recv(count - len(chunks))
        if not piece:
            break
        chunks.extend(piece)
    return bytes(chunks)


def copy_stream(source: socket.socket, dest: socket.socket) -> None:
    try:
        while True:
            chunk = source.recv(65536)
            if not chunk:
                break
            dest.sendall(chunk)
    except OSError:
        pass
    finally:
        try:
            dest.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def main() -> None:
    real = os.environ.get("BOTSTER_HUB_TEST_REAL_SESSION_WORKER")
    kind = os.environ.get("BOTSTER_HUB_TEST_WORKER_WRAPPER_KIND", "w1")
    delay_s = float(os.environ.get("BOTSTER_HUB_TEST_WORKER_WRAPPER_DELAY_SECS", "3"))
    if not real:
        raise SystemExit("BOTSTER_HUB_TEST_REAL_SESSION_WORKER is required")

    forwarded, public_socket = parse_args(sys.argv[1:])
    if public_socket is None:
        raise SystemExit("wrapper requires --control-socket")

    public_socket.parent.mkdir(parents=True, exist_ok=True)
    if public_socket.exists():
        public_socket.unlink()
    inner_socket = public_socket.with_name(f"inner-{os.getpid()}.sock")
    if inner_socket.exists():
        inner_socket.unlink()

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(public_socket))
    listener.listen(1)

    child = subprocess.Popen(
        [real, *forwarded, "--control-socket", str(inner_socket)],
        stdout=subprocess.DEVNULL,
    )
    try:
        wait_for_socket(inner_socket)
        sys.stdout.write(f"botster-session-worker-ready {os.getpid()}\n")
        sys.stdout.flush()

        listener.settimeout(2.0)
        hub, _ = listener.accept()
        inner = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        inner.settimeout(2.0)
        inner.connect(str(inner_socket))
        hub.settimeout(None)
        inner.settimeout(None)

        # Core writes hello + spawn first. The inner worker then writes welcome.
        hub_to_inner = threading.Thread(target=copy_stream, args=(hub, inner), daemon=True)
        hub_to_inner.start()
        first = rewrite_welcome(inner, os.getpid(), public_socket)
        if first:
            hub.sendall(first)
        inner_to_hub = threading.Thread(target=copy_stream, args=(inner, hub), daemon=True)
        inner_to_hub.start()
        status = child.wait()
        hub_to_inner.join(timeout=1)
        inner_to_hub.join(timeout=1)
    finally:
        try:
            listener.close()
        except OSError:
            pass
        if inner_socket.exists():
            inner_socket.unlink()

    if kind == "w1":
        time.sleep(delay_s)
        raise SystemExit(status)
    raise SystemExit(1 if status == 0 else status)


if __name__ == "__main__":
    main()
