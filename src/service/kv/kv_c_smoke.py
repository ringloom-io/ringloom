#!/usr/bin/env python3
"""Run the standalone C KV smoke check after `zig build`; retain broker/client logs."""
import pathlib
import random
import socket
import subprocess
import tempfile
import time


def main():
    project = pathlib.Path(__file__).resolve().parents[3]
    artifacts = pathlib.Path(tempfile.mkdtemp(prefix="ringloom-k07-smoke-"))
    print(f"Artifacts: {artifacts}", flush=True)
    executable = artifacts / "kv-c-smoke"
    subprocess.run([
        "cc", "-std=c11", "-Wall", "-Wextra", "-Werror",
        f"-I{project / 'include'}", str(project / "src/service/kv/kv_c_smoke.c"),
        f"-L{project / 'zig-out/lib'}", "-lringloom_service",
        f"-Wl,-rpath,{project / 'zig-out/lib'}", "-o", str(executable),
    ], check=True)
    for _ in range(100):
        port = random.randrange(20000, 30000)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                break
            except OSError:
                continue
    else:
        raise RuntimeError("no available broker port")
    config = artifacts / "broker.properties"
    config.write_text(f"""broker.node.id=1
broker.local.host.port=127.0.0.1:{port}
broker.group.name=k07-smoke
broker.storage.path={artifacts}/storage
broker.aeron.directory={artifacts}/aeron
broker.topics.enabled=true
broker.kv.enabled=true
broker.aeron.sparse.files=true
broker.aeron.ipc.term.length=1048576
broker.aeron.udp.term.length=1048576
broker.kv.max.stores=4
broker.kv.map.size.max=67108864
""")
    broker_log = artifacts / "broker.log"
    with broker_log.open("w") as log:
        broker = subprocess.Popen([
            str(project / "zig-out/bin/ringloom-broker"), "--config", str(config),
        ], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while "became the topic leader" not in broker_log.read_text():
                if broker.poll() is not None:
                    raise RuntimeError(f"broker exited: {broker_log.read_text()}")
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"broker election timeout: {broker_log.read_text()}")
                time.sleep(0.05)
            client = subprocess.run([
                str(executable), str(artifacts / "storage"),
            ], capture_output=True, text=True, timeout=90)
            (artifacts / "client.log").write_text(client.stdout + client.stderr)
            print(client.stdout, end="")
            print(client.stderr, end="")
            client.check_returncode()
        finally:
            broker.terminate()
            try:
                broker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait()


if __name__ == "__main__":
    main()
