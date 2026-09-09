"""Harbor's native Kubernetes backend constrained to the workspace user.

Harbor uses user='root' for ordinary setup operations such as chmod of its own
log directory. Eruun's prepared task images make those paths writable by UID
1000, so all operations run as that fixed UID without invoking su or sudo.
Commands which actually require root fail normally in the restricted Pod.
"""

import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import shlex
import tarfile
import tempfile
import uuid

from harbor.environments.ack import ACKEnvironment
from kubernetes.stream import stream

MAX_TRANSFER_BYTES = 2 * 1024 * 1024 * 1024


class WorkspaceEnvironment(ACKEnvironment):
    def __init__(self, *, collection_state_dir, **kwargs):
        super().__init__(**kwargs)
        # This directory is outside every task and downloaded trial directory.
        # Persist before each operation so a killed process or failed write
        # cannot turn an unfinished transfer into complete collection.
        directory = Path(collection_state_dir)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._collection_path = directory / (uuid.uuid4().hex + ".json")
        self._collection = {"podName": self.pod_name, "namespace": self.namespace,
                            "trialDirectory": str(self.trial_paths.trial_dir),
                            "started": False, "stopped": False, "pending": 0,
                            "errorCount": 0, "errors": []}
        self._save_collection()

    def _save_collection(self):
        temporary = self._collection_path.with_suffix(".tmp")
        try:
            temporary.write_text(json.dumps(self._collection))
            temporary.replace(self._collection_path)
        except OSError:
            self._collection["errorCount"] += 1
            if len(self._collection["errors"]) < 100:
                self._collection["errors"].append({"path": "collection", "reason": "state_write_failed"})
            raise

    @asynccontextmanager
    async def _collecting(self, source):
        self._collection["pending"] += 1
        self._save_collection()
        try:
            yield
        except BaseException as exc:
            self._collection["errorCount"] += 1
            if len(self._collection["errors"]) < 100:
                self._collection["errors"].append({"path": str(source)[:1024], "reason": type(exc).__name__})
            raise
        finally:
            self._collection["pending"] -= 1
            self._save_collection()

    async def start(self, force_build):
        await super().start(force_build)
        # Harbor's implicit artifact source is optional. An empty convention
        # directory is a successful collection, including task images which
        # only pre-create /logs; absent user-declared artifacts still fail.
        result = await self.exec("mkdir -p /logs/artifacts")
        if result.return_code != 0:
            raise RuntimeError("cannot prepare sandbox artifact directory")
        self._collection["started"] = True
        self._save_collection()

    async def stop(self, delete):
        # Free completed trials promptly so a large dataset cannot exhaust the
        # namespace's running-Pod quota. A failed transfer retains its source
        # Pod; successful transfers retain all original bytes in the Runner
        # even if the later final archive or API upload fails.
        complete = self._collection["started"] and self._collection["pending"] == 0 and self._collection["errorCount"] == 0
        try:
            manifest = self.trial_paths.artifacts_dir / "manifest.json"
            if manifest.stat().st_size > 1024 * 1024:
                raise ValueError("oversized artifact manifest")
            entries = json.loads(manifest.read_text())
            if not isinstance(entries, list) or not entries:
                raise ValueError("invalid artifact manifest")
            complete = complete and all(entry["status"] in {"ok", "empty"} for entry in entries)
        except (OSError, ValueError, KeyError, TypeError):
            complete = False
        await super().stop(delete=complete)
        self._collection["stopped"] = True
        self._save_collection()

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
        async with self._collecting(source_path):
            await self._ensure_client()
            target = Path(target_path)
            await asyncio.to_thread(self._download_tar, ["tar", "cf", "-", source_path], target.parent, target.name)

    async def download_dir(self, source_dir, target_dir):
        async with self._collecting(source_dir):
            await self._ensure_client()
            await asyncio.to_thread(self._download_tar,
                                    ["sh", "-c", f"cd {shlex.quote(source_dir)} && tar cf - ."], Path(target_dir))

    async def download_dir_with_exclusions(self, *, source_dir, target_dir, exclude):
        # Include archive creation/extraction failures which happen outside
        # download_file in Harbor's higher-level transfer implementation.
        async with self._collecting(source_dir):
            await super().download_dir_with_exclusions(source_dir=source_dir, target_dir=target_dir, exclude=exclude)

    async def download_dir_filtered(self, *, source_dir, target_dir, include=None, exclude=None, protect=None):
        async with self._collecting(source_dir):
            await super().download_dir_filtered(source_dir=source_dir, target_dir=target_dir,
                                                 include=include, exclude=exclude, protect=protect)
