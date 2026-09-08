"""Harbor's native Kubernetes backend constrained to the workspace user.

Harbor uses user='root' for ordinary setup operations such as chmod of its own
log directory. Eruun's prepared task images make those paths writable by UID
1000, so all operations run as that fixed UID without invoking su or sudo.
Commands which actually require root fail normally in the restricted Pod.
"""

import asyncio
from pathlib import Path
import shlex
import tarfile
import tempfile

from harbor.environments.ack import ACKEnvironment
from kubernetes.stream import stream

MAX_TRANSFER_BYTES = 2 * 1024 * 1024 * 1024


class WorkspaceEnvironment(ACKEnvironment):
    @staticmethod
    def type():
        return "eruun-kubernetes"

    def _resolve_user(self, user):
        # The generated Pod's runAsUser is authoritative; returning None avoids
        # ACK's `su` wrapper, including BaseEnvironment.default_user fallback.
        return None

    def _download_tar(self, command, destination, filename=None):
        # ACK 0.22.0 uses text websocket frames for tar downloads. Kubernetes'
        # UTF-8 replacement decoding irreversibly corrupts binary artifacts.
        response = stream(
            self._exec_api.connect_get_namespaced_pod_exec,
            self.pod_name, self.namespace, command=command,
            stderr=True, stdin=False, stdout=True, tty=False,
            _preload_content=False, binary=True,
        )
        with tempfile.TemporaryFile() as data:
            size = 0
            try:
                while response.is_open():
                    response.update(timeout=1)
                    if response.peek_stdout():
                        chunk = response.read_stdout()
                        if not isinstance(chunk, bytes):
                            raise RuntimeError("sandbox transfer must use binary websocket frames")
                        size += len(chunk)
                        if size > MAX_TRANSFER_BYTES:
                            raise RuntimeError("sandbox transfer exceeds 2 GiB")
                        data.write(chunk)
                    if response.peek_stderr():
                        response.read_stderr()  # Do not retain an unbounded stderr buffer.
                    # Kubernetes 32's public stream wrapper does not accept
                    # capture_all=False. Clear its duplicate aggregate buffer,
                    # preserving separate stdout/stderr/exit-status channels.
                    response._all.seek(0)
                    response._all.truncate(0)
                if response.returncode != 0:
                    raise RuntimeError("sandbox tar command failed")
            finally:
                response.close()
            data.seek(0)
            destination.mkdir(parents=True, exist_ok=True)
            with tarfile.open(fileobj=data, mode="r:") as archive:
                members = archive.getmembers()
                if len(members) > 10_000 or sum(member.size for member in members) > MAX_TRANSFER_BYTES:
                    raise RuntimeError("expanded sandbox transfer exceeds collection limits")
                if filename is not None:
                    if len(members) != 1 or not members[0].isfile():
                        raise RuntimeError("sandbox file transfer must contain one regular file")
                    members[0].name = filename
                    archive.extract(members[0], destination, filter="data")
                else:
                    archive.extractall(destination, members=members, filter="data")

    async def download_file(self, source_path, target_path):
        await self._ensure_client()
        target = Path(target_path)
        await asyncio.to_thread(self._download_tar, ["tar", "cf", "-", source_path], target.parent, target.name)

    async def download_dir(self, source_dir, target_dir):
        await self._ensure_client()
        await asyncio.to_thread(self._download_tar,
                                ["sh", "-c", f"cd {shlex.quote(source_dir)} && tar cf - ."], Path(target_dir))
