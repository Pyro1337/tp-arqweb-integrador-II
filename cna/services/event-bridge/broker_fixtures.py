"""Private, bounded child-process fixtures. No daemon discovery or external URLs.

Set BROKER_NATS_SERVER to an explicitly owned private executable path.
with NatsBroker() as broker: await nats.connect(broker.url)
restart() retains the private data directory, but clients must reconnect.
Every context has a hard 30s lifetime including restarts. close() is idempotent.
"""
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path


class OwnedProcess:
    """Only ever signal/reap the Popen child created by this instance."""
    def __init__(self, command, output=subprocess.DEVNULL):
        self.command = command
        self.output = output
        self.process = None
        self._lock = threading.Lock()

    def __enter__(self):
        self.process = subprocess.Popen(self.command, stdin=subprocess.DEVNULL,
                                        stdout=self.output, stderr=subprocess.STDOUT,
                                        close_fds=True)
        return self

    def close(self):
        with self._lock:
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        self.process.wait(timeout=1)
                else:
                    self.process.wait(timeout=1)

    def __exit__(self, *exc):
        self.close()


class PrivateBroker:
    """Private temporary storage and a lifetime watchdog; no port preallocation."""
    def __init__(self, max_lifetime=30):
        if not 2 < max_lifetime <= 30:
            raise ValueError('max_lifetime must be >2 and <=30 seconds (2s cleanup reserve)')
        self._temp = tempfile.TemporaryDirectory(prefix='broker-v2-')
        self.root = Path(self._temp.name)
        self.owned = None
        self._closed = False
        self._lock = threading.RLock()
        self.deadline = time.monotonic() + max_lifetime - 2
        self._log = (self.root / 'broker.log').open('ab', buffering=0)
        self._timer = threading.Timer(max_lifetime - 2, self.close)
        self._timer.daemon = True
        self._timer.start()

    @property
    def process(self):
        return self.owned.process

    def __enter__(self):
        try:
            self.start()
            return self
        except BaseException:
            self.close()
            raise

    def _spawn(self, command, ready):
        with self._lock:
            if self._closed or time.monotonic() >= self.deadline:
                raise RuntimeError('fixture lifetime expired')
            self.owned = OwnedProcess(command, self._log).__enter__()
            deadline = min(time.monotonic() + 4, self.deadline)
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError((self.root / 'broker.log').read_text())
                if ready():
                    return
                time.sleep(0.025)
            raise TimeoutError('private broker startup exceeded 4 seconds: ' +
                               (self.root / 'broker.log').read_text())

    def stop(self):
        with self._lock:
            if self.owned is not None:
                self.owned.close()

    def restart(self):
        self.stop()
        try:
            self.start()
        except BaseException:
            self.close()
            raise

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._timer.cancel()
            try:
                self.stop()
            finally:
                self._log.close()
                self._temp.cleanup()

    def __exit__(self, *exc):
        self.close()

    def tcp_listeners(self):
        """Inspect only this child's socket inodes; include IPv6, not host-wide claims."""
        inodes = set()
        for fd in Path(f'/proc/{self.process.pid}/fd').iterdir():
            try:
                target = os.readlink(fd)
            except FileNotFoundError:
                continue
            if target.startswith('socket:['):
                inodes.add(target[8:-1])
        result = []
        for table, family in [('tcp', socket.AF_INET), ('tcp6', socket.AF_INET6)]:
            for line in Path('/proc/net/' + table).read_text().splitlines()[1:]:
                columns = line.split()
                if columns[3] == '0A' and columns[9] in inodes:
                    host, port = columns[1].split(':')
                    raw = bytes.fromhex(host)
                    if family == socket.AF_INET6:
                        raw = b''.join(raw[i:i+4][::-1] for i in range(0, 16, 4))
                    else:
                        raw = raw[::-1]
                    result.append((socket.inet_ntop(family, raw), int(port, 16)))
        return result


class NatsBroker(PrivateBroker):
    def __init__(self):
        super().__init__()
        self.binary = os.environ['BROKER_NATS_SERVER']
        self.port = None
        self.url = None
        self.protocol_info = None
        self.config_text = '\n'.join([
            'host: "127.0.0.1"', 'port: -1', 'max_payload: 512KB',
            'jetstream {', f'  store_dir: "{self.root / "jetstream"}"',
            '  max_memory_store: 16MB', '  max_file_store: 64MB', '}', ''])
        self.config = self.root / 'nats.conf'
        self.config.write_text(self.config_text)

    def start(self):
        # Fail closed rather than replacing unsupported flags with a public port.
        help_run = subprocess.run([self.binary, '--help'], capture_output=True,
                                  text=True, timeout=1, check=True)
        if '--ports_file_dir' not in help_run.stdout:
            raise RuntimeError('nats-server lacks --ports_file_dir; no fallback allowed')

        def ready():
            ports_file = self.root / f'nats-server_{self.process.pid}.ports'
            if not ports_file.exists():
                return False
            ports = json.loads(ports_file.read_text())
            from urllib.parse import urlsplit
            parsed = urlsplit(ports['nats'][0])
            if parsed.hostname != '127.0.0.1' or not parsed.port:
                raise RuntimeError('NATS advertised unexpected/non-loopback address')
            try:
                with socket.create_connection(('127.0.0.1', parsed.port), timeout=0.15) as sock:
                    info = sock.recv(4096)
                    if not info.startswith(b'INFO '):
                        raise RuntimeError('NATS readiness did not return INFO')
                    self.protocol_info = json.loads(info.split(b'\r\n')[0][5:])
            except (ConnectionError, TimeoutError):
                return False
            self.port, self.url = parsed.port, ports['nats'][0]
            return True
        self._spawn([self.binary, '-c', str(self.config), '-p', '-1',
                     '--ports_file_dir', str(self.root)], ready)
