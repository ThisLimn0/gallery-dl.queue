#!/usr/bin/env python3
"""gdl.queue.py – Interactive gallery-dl queue"""

from __future__ import annotations

import cmd
import hashlib
import itertools
import json
import os
import queue
import shlex
import shutil
import subprocess
import sys
import signal
import threading
import time
import codecs
import re
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import (
    BinaryIO,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# ---------------------------------------------------------------------------
# Optional readline support (tab completion)
#
# `cmd.Cmd` tab completion requires a `readline`-compatible module.
# On Windows this is typically provided by `pyreadline3`.
# ---------------------------------------------------------------------------
HAS_READLINE = False
try:
    import readline as _readline  # type: ignore  # noqa: F401

    # Python 3.13+ `cmd` expects `readline.backend`.
    if not hasattr(_readline, "backend"):
        setattr(_readline, "backend", "pyreadline")

    HAS_READLINE = True
except Exception:
    if sys.platform.startswith("win"):
        try:
            import pyreadline3  # type: ignore  # noqa: F401
            import readline as _readline  # type: ignore  # noqa: F401

            if not hasattr(_readline, "backend"):
                setattr(_readline, "backend", "pyreadline")

            HAS_READLINE = True
        except Exception:
            HAS_READLINE = False

try:
    from colorama import Fore, Style, init as colorama_init

    colorama_init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    # Fallback when colorama not available
    class _DummyColor:
        def __getattr__(self, name):
            return ""

    Fore = Style = _DummyColor()
    HAS_COLOR = False

# ---------------------------------------------------------------------------
# gallery-dl auto-update helpers
# ---------------------------------------------------------------------------
UPDATE_STATE_FILE = Path("gallery_dl_update_state.json")
UPDATE_CHECK_INTERVAL = timedelta(days=1)
REPO_API_URL = "https://api.github.com/repos/mikf/gallery-dl/releases/latest"
DOWNLOAD_BASE_URL = "https://github.com/mikf/gallery-dl/releases/download/"
DEFAULT_BINARY_NAME = (
    "gallery-dl.exe" if sys.platform.startswith("win") else "gallery-dl"
)

# Strip ANSI color codes so log pattern matching remains stable even when
# gallery-dl writes colored output.
ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
FILENAME_SAFE_PATTERN = re.compile(r"[^0-9A-Za-z._-]+")

# ---------------------------------------------------------------------------
# Tiny-video detection (known-bad placeholder downloads)
#
# Some sites intermittently serve a bogus "tiny video" file of a fixed size.
# Configure the inclusive byte range here so the queue can detect and scrub
# those files reliably.
#
# Current observed size: 322509 bytes
# ---------------------------------------------------------------------------
TINY_VIDEO_MIN_BYTES = 322_509
TINY_VIDEO_MAX_BYTES = 322_509

# Known bogus file hashes (lowercase hex). If a detected bogus/tiny file matches
# one of these hashes, it is treated as invalid regardless of filename.
TINY_FILE_SHA256_BLACKLIST: Set[str] = {
    "d503b7265dc8331c058e7e2fe17a2389f41266d181ca5218e189bdaf4784c113",
}

# ---------------------------------------------------------------------------
# HTTP error handling (parsed from gallery-dl output)
# ---------------------------------------------------------------------------
# Weighting: a single occurrence of these statuses counts as N occurrences.
HTTP_5XX_WEIGHT = 10
HTTP_429_WEIGHT = 10

# If the weighted 5xx counter reaches this threshold within a worker, the
# current URL is considered failed and is reappended.
HTTP_5XX_FAIL_THRESHOLD = 10

# Exponential backoff for HTTP 429 Too Many Requests (per worker thread).
RATE_LIMIT_BACKOFF_BASE_SECONDS = 30
RATE_LIMIT_BACKOFF_MAX_SECONDS = 60 * 60


def _format_bytes(num_bytes: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    unit_index = 0
    while value >= 1024.0 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.1f} {units[unit_index]}"


def _sanitize_filename(value: str, max_length: int = 120) -> str:
    slug = FILENAME_SAFE_PATTERN.sub("_", value)
    slug = re.sub(r"_+", "_", slug).strip("._")
    if not slug:
        slug = "url"
    if len(slug) > max_length:
        suffix = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        prefix_len = max(1, max_length - len(suffix) - 1)
        slug = f"{slug[:prefix_len]}_{suffix}"
    return slug


def _strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def _write_json_atomic_if_changed(path: str | Path, payload: object) -> bool:
    """Write JSON to disk only when it changed.

    Returns True when the file was written, False when skipped.
    """

    target = Path(path)
    text = json.dumps(payload, indent=2)

    try:
        existing = target.read_text(encoding="utf-8")
        if existing == text:
            return False
    except FileNotFoundError:
        pass
    except Exception:
        # If we cannot read/compare, proceed with writing.
        pass

    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, target)
    return True


class UpdateError(Exception):
    """Raised when the updater cannot complete its work."""


class UpdateRateLimit(UpdateError):
    """Raised when the updater is rate limited by the remote service."""


class UpdatePermissionError(UpdateError):
    """Raised when the updater cannot write to the target binary location."""


def _get_remote_version() -> str:
    """Return the latest gallery-dl release tag (e.g. 'v1.27.0')."""

    req = Request(REPO_API_URL, headers={"User-Agent": "gallery-dl-queue/1.0"})
    with urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    tag = data.get("tag_name")
    if not tag:
        raise UpdateError("GitHub API response missing tag_name")
    return tag


def _get_local_version(binary_path: Path) -> Optional[str]:
    """Inspect a gallery-dl binary for its '--version' output."""

    if not binary_path.exists():
        return None

    try:
        output = subprocess.check_output(
            [str(binary_path), "--version"],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None

    if not output.startswith("v"):
        output = "v" + output
    return output


def _download_file(url: str, dest: Path) -> None:
    """Download a file from URL to the given destination path."""

    req = Request(url, headers={"User-Agent": "gallery-dl-queue/1.0"})
    with urlopen(req, timeout=60) as resp, open(dest, "wb") as handle:
        while True:
            chunk = resp.read(8192)
            if not chunk:
                break
            handle.write(chunk)


def update_gallery_dl(
    base_dir: Path | str | None = None,
    binary_name: str = DEFAULT_BINARY_NAME,
    remove_old: bool = True,
    *,
    verbose: bool = True,
) -> Tuple[Optional[str], str, bool]:
    """Check for a newer gallery-dl release and download it when needed."""

    if base_dir is None:
        base_dir = Path(__file__).resolve().parent
    else:
        base_dir = Path(base_dir)

    binary_path = base_dir / binary_name
    old_binary_path = binary_path.with_suffix(binary_path.suffix + ".old")

    if not binary_path.exists() and verbose:
        print(f"{binary_name} does not exist.")

    local_version = _get_local_version(binary_path)
    remote_version = _get_remote_version()

    if local_version is not None and local_version == remote_version:
        if verbose:
            print(f"{binary_name} is up to date ({local_version}).")
        return local_version, remote_version, False

    if local_version is None:
        if verbose:
            print(f"{binary_name}: remote {remote_version}, local version not found.")
    else:
        if verbose:
            print(
                f"{binary_name}: remote {remote_version} <> {local_version} --> local is outdated."
            )

    if remove_old:
        if old_binary_path.exists():
            old_binary_path.unlink()
    else:
        if binary_path.exists():
            if old_binary_path.exists():
                old_binary_path.unlink()
            binary_path.rename(old_binary_path)

    download_url = f"{DOWNLOAD_BASE_URL}{remote_version}/{binary_name}"
    if verbose:
        print(f"Downloading new version from {download_url} ...")
    _download_file(download_url, binary_path)

    try:
        binary_path.chmod(binary_path.stat().st_mode | 0o111)
    except PermissionError:
        pass

    new_version = _get_local_version(binary_path)
    if verbose:
        print(f"Updated to version: {new_version}")
    return new_version, remote_version, True


def _read_update_state() -> Optional[datetime]:
    try:
        with open(UPDATE_STATE_FILE) as handle:
            payload = json.load(handle)
        stamp = payload.get("last_check")
        if stamp:
            return datetime.fromisoformat(stamp)
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return None


def _write_update_state(timestamp: datetime, **extra) -> None:
    payload = {"last_check": timestamp.isoformat()}
    payload.update(extra)
    try:
        with open(UPDATE_STATE_FILE, "w") as handle:
            json.dump(payload, handle, indent=2)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class Config:
    def __init__(self):
        self.workers = 4
        self.max_retries = 2
        self.retry_delay = 60  # seconds
        self.download_dir = None
        self.gallery_dl_path = None
        self.default_args = []
        self.auto_save = True
        self.session_file = "gallery_dl_session.json"
        self.config_file = "gallery_dl_config.json"
        self.auto_status = False
        self.flat_directories = True
        self.directory_template: Optional[str] = None
        self.keep_failed = True
        self.dump_json = False
        # Optional metadata precheck via `gallery-dl -j --no-download`.
        # This is used as a second validation layer and is enabled by default.
        self.precheck_no_download = True
        # If the precheck indicates bogus/tiny/0-byte downloads, skip the actual
        # download attempt for this URL (no-download fail-fast).
        self.precheck_skip_download_on_fail = True
        # Try to probe remote sizes without downloading full content (HEAD or
        # Range: bytes=0-0 fallback). Limited to avoid too many requests.
        self.precheck_probe_remote_sizes = True
        self.precheck_probe_limit = 10

        # Second validation: if we can map a downloaded filename to an expected
        # size from precheck, treat mismatches as failure.
        self.validate_expected_sizes = True
        self.expected_size_tolerance = 0  # bytes

    def to_dict(self) -> dict:
        return {
            "workers": self.workers,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "download_dir": str(self.download_dir) if self.download_dir else None,
            "gallery_dl_path": str(self.gallery_dl_path)
            if self.gallery_dl_path
            else None,
            "default_args": self.default_args,
            "auto_save": self.auto_save,
            "session_file": self.session_file,
            "auto_status": self.auto_status,
            "flat_directories": self.flat_directories,
            "directory_template": self.directory_template,
            "keep_failed": self.keep_failed,
            "dump_json": self.dump_json,
            "precheck_no_download": self.precheck_no_download,
            "precheck_skip_download_on_fail": self.precheck_skip_download_on_fail,
            "precheck_probe_remote_sizes": self.precheck_probe_remote_sizes,
            "precheck_probe_limit": self.precheck_probe_limit,
            "validate_expected_sizes": self.validate_expected_sizes,
            "expected_size_tolerance": self.expected_size_tolerance,
        }

    def from_dict(self, data: dict):
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)


def _resolve_gallery_dl_binary(config: "Config") -> Optional[Path]:
    if config.gallery_dl_path:
        return Path(config.gallery_dl_path).expanduser()

    candidate = shutil.which("gallery-dl") or shutil.which("gallery-dl.exe")
    if candidate:
        return Path(candidate)
    return None


def maybe_check_gallery_dl_update(
    config: "Config",
) -> Optional[Tuple[Optional[str], str, bool]]:
    now = datetime.now()
    last_check = _read_update_state()
    if last_check and now - last_check < UPDATE_CHECK_INTERVAL:
        return None

    binary_path = _resolve_gallery_dl_binary(config)
    if binary_path is None:
        _write_update_state(now, status="binary-not-found")
        return None

    target_for_access = binary_path if binary_path.exists() else binary_path.parent
    if target_for_access and not os.access(target_for_access, os.W_OK):
        _write_update_state(
            now,
            status="no-permission",
            path=str(binary_path),
        )
        raise UpdatePermissionError(
            f"Cannot update gallery-dl at {binary_path} (insufficient permissions)"
        )

    base_dir = binary_path.parent
    binary_name = binary_path.name or DEFAULT_BINARY_NAME

    try:
        result = update_gallery_dl(
            base_dir=base_dir,
            binary_name=binary_name,
            verbose=False,
        )
    except HTTPError as exc:
        status = "rate-limited" if exc.code in {403, 429} else "http-error"
        _write_update_state(
            now,
            status=status,
            code=exc.code,
            reason=getattr(exc, "reason", ""),
        )
        if status == "rate-limited":
            raise UpdateRateLimit(f"GitHub API rate limited (HTTP {exc.code})") from exc
        raise UpdateError(f"GitHub API error (HTTP {exc.code}: {exc.reason})") from exc
    except UpdateError as exc:
        _write_update_state(now, status="error", message=str(exc))
        raise
    except Exception as exc:
        _write_update_state(now, status="error", message=str(exc))
        raise UpdateError(str(exc)) from exc

    local_version, remote_version, updated = result
    _write_update_state(
        now,
        status="updated" if updated else "current",
        local_version=local_version,
        remote_version=remote_version,
    )
    return result


class PendingEntry(NamedTuple):
    url: str
    directory: Optional[Path]


class FailedEntry(NamedTuple):
    url: str
    directory: Optional[Path]


class UnsupportedSessionFormatError(ValueError):
    """Raised when a session file uses an unsupported on-disk format."""


class WorkerTransferState(NamedTuple):
    directory: Path
    start_time: float
    start_bytes: int
    last_time: float
    last_bytes: int


class WorkerLog:
    """Ring buffer that keeps a bounded stream of worker output."""

    def __init__(self, maxlen: int = 2000):
        self.buffer: deque[str] = deque()
        self.maxlen = maxlen
        self.offset = 0

    def append(self, message: str):
        if len(self.buffer) >= self.maxlen:
            self.buffer.popleft()
            self.offset += 1
        self.buffer.append(message)

    def get_since(self, index: int) -> Tuple[List[str], int, bool]:
        """Return entries after index, next index, and truncation flag."""
        truncated = index < self.offset
        start = max(index, self.offset)
        skip = start - self.offset
        lines = list(itertools.islice(self.buffer, skip, None))
        next_index = self.offset + len(self.buffer)
        return lines, next_index, truncated

    def snapshot(self) -> Tuple[List[str], int, int]:
        """Return a copy of the buffer and index bounds."""
        buf_copy = list(self.buffer)
        start_index = self.offset
        end_index = self.offset + len(self.buffer)
        return buf_copy, start_index, end_index


# ---------------------------------------------------------------------------
# Download tracking and statistics
# ---------------------------------------------------------------------------
class DownloadStats:
    def __init__(self):
        self._lock = threading.Lock()
        self.completed = 0
        self.failed = 0
        self.retried = 0
        self.reappended = 0
        self.total_time = 0.0
        self.history: deque = deque(maxlen=1000)  # Keep last 1000 entries
        self.current_downloads: Dict[int, Tuple[str, Optional[Path]]] = {}

    def add_completion(self, url: str, success: bool, duration: float, worker_id: int):
        with self._lock:
            if success:
                self.completed += 1
            else:
                self.failed += 1

            self.total_time += duration
            self.history.append(
                {
                    "url": url,
                    "success": success,
                    "duration": duration,
                    "timestamp": datetime.now().isoformat(),
                    "worker_id": worker_id,
                }
            )

            # Remove from active downloads
            self.current_downloads.pop(worker_id, None)

    def set_current_download(self, worker_id: int, url: str, directory: Optional[Path]):
        with self._lock:
            self.current_downloads[worker_id] = (url, directory)

    def record_retry(self):
        with self._lock:
            self.retried += 1

    def record_reappend(self):
        with self._lock:
            self.reappended += 1

    def snapshot_active(self) -> Dict[int, Tuple[str, Optional[Path]]]:
        with self._lock:
            return dict(self.current_downloads)

    def has_active_downloads(self) -> bool:
        with self._lock:
            return bool(self.current_downloads)

    def is_worker_active(self, worker_id: int) -> bool:
        with self._lock:
            return worker_id in self.current_downloads

    def history_snapshot(self) -> List[dict]:
        with self._lock:
            return list(self.history)

    def get_summary(self) -> str:
        with self._lock:
            total = self.completed + self.failed
            if total == 0:
                return "No downloads completed yet"

            success_rate = (self.completed / total) * 100
            avg_time = self.total_time / total if total > 0 else 0
            fail_color = Fore.RED if self.failed else Fore.WHITE

            return (
                f"{Fore.WHITE}STATS: {total} downloads "
                f"({Fore.GREEN}{self.completed} success{Fore.WHITE}, "
                f"{fail_color}{self.failed} failed{Fore.WHITE}) - "
                f"{success_rate:.1f}% success rate, avg {avg_time:.1f}s per download, "
                f"{self.reappended} reappends"
                f"{Style.RESET_ALL}"
            )


# ---------------------------------------------------------------------------
# Threaded downloader
# ---------------------------------------------------------------------------
class GDLQueue:
    def __init__(
        self,
        config: Config,
        stats: DownloadStats,
        failed_seed: Optional[List[FailedEntry]] = None,
    ):
        self.config = config
        self.stats = stats

        # Find gallery-dl executable
        binary_path = _resolve_gallery_dl_binary(config)
        if not binary_path:
            raise RuntimeError(
                "gallery-dl executable not found, install it or set gallery_dl_path"
            )
        self.exe = str(binary_path)

        # Thread-safe structures
        self.q: "queue.Queue[Tuple[str, int, Optional[Path]]]" = queue.Queue()
        self.stop_event = threading.Event()
        self.paused_event = threading.Event()

        # Desired worker count (supports downsizing without restarting).
        # Workers with id >= desired count will finish their current job (if any)
        # and then retire without taking new queue items.
        self._desired_workers_lock = threading.Lock()
        self._desired_workers = int(self.config.workers)

        # Worker management
        self.threads: List[threading.Thread] = []
        self.next_worker_id = 0
        self._create_workers(count=int(self.config.workers))

        # URL tracking
        self.queued_urls: Set[str] = set()
        self.completed_urls: Set[str] = set()

        # Worker output tracking
        self.worker_logs: Dict[int, WorkerLog] = {}
        self.worker_log_lock = threading.Lock()
        self.worker_log_events: Dict[int, threading.Event] = {}
        self.worker_processes: Dict[int, Optional[subprocess.Popen]] = {}
        self.worker_process_lock = threading.Lock()
        self.worker_download_dirs: Dict[int, Optional[Path]] = {}
        self.worker_dir_lock = threading.Lock()
        self.worker_progress: Dict[int, str] = {}
        self.worker_progress_lock = threading.Lock()
        self.failed_downloads_lock = threading.Lock()
        self.failed_downloads: Dict[str, FailedEntry] = {}
        self.worker_active_urls: Dict[int, str] = {}
        self.worker_active_lock = threading.Lock()

        # Approximate per-worker transfer stats (based on download directory growth)
        self.worker_transfer_lock = threading.Lock()
        self.worker_transfer: Dict[int, WorkerTransferState] = {}
        self.unsupported_lock = threading.Lock()
        self.unsupported_details: Dict[str, str] = {}

        # Broken URL tracking (e.g. server-side 0-byte items confirmed by download)
        self.broken_lock = threading.Lock()
        self.broken_details: Dict[str, str] = {}

        # HTTP error tracking parsed from gallery-dl logs (per worker thread)
        self.http_lock = threading.Lock()
        self.worker_http_429_score: Dict[int, int] = {}
        self.worker_http_5xx_score: Dict[int, int] = {}
        self.worker_http_action: Dict[int, Tuple[str, str]] = {}
        self.worker_rate_limit_backoff_exp: Dict[int, int] = {}

        # Worker restart scheduling (for 429 backoff)
        self.worker_restart_lock = threading.Lock()
        self.worker_restart_pending: Set[int] = set()
        self.idle_callback: Optional[Callable[[], None]] = None
        self.multi_active_since_idle = False
        if failed_seed:
            self.import_failed_entries(failed_seed, replace=True)

    def _note_worker_http_status(self, worker_id: int, status_code: int, detail: str) -> None:
        """Record an HTTP status from logs and set worker actions when needed.

        - 429: mark worker as rate-limited (kill current process, restart worker with backoff)
        - 5xx: accumulate score; once threshold reached, force current URL to fail+reappend
        """
        action: Optional[Tuple[str, str]] = None
        with self.http_lock:
            if status_code == 429:
                self.worker_http_429_score[worker_id] = self.worker_http_429_score.get(worker_id, 0) + int(HTTP_429_WEIGHT)
                action = ("rate_limit", detail)
                self.worker_http_action[worker_id] = action
            elif 500 <= int(status_code) <= 599:
                self.worker_http_5xx_score[worker_id] = self.worker_http_5xx_score.get(worker_id, 0) + int(HTTP_5XX_WEIGHT)
                score = self.worker_http_5xx_score[worker_id]
                if score >= int(HTTP_5XX_FAIL_THRESHOLD):
                    action = ("server_5xx", detail)
                    self.worker_http_action[worker_id] = action

        if action is not None:
            # Best-effort: terminate the currently running gallery-dl process for this worker.
            with self.worker_process_lock:
                proc = self.worker_processes.get(worker_id)
            if proc is not None and proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass

    def _consume_worker_http_action(self, worker_id: int) -> Optional[Tuple[str, str]]:
        with self.http_lock:
            action = self.worker_http_action.pop(worker_id, None)
            if action is None:
                return None
            kind, _detail = action
            if kind == "rate_limit":
                self.worker_http_429_score[worker_id] = 0
            elif kind == "server_5xx":
                self.worker_http_5xx_score[worker_id] = 0
            return action

    def _compute_rate_limit_backoff(self, worker_id: int) -> int:
        with self.http_lock:
            exp = int(self.worker_rate_limit_backoff_exp.get(worker_id, 0))
            self.worker_rate_limit_backoff_exp[worker_id] = exp + 1
        delay = int(RATE_LIMIT_BACKOFF_BASE_SECONDS) * (2 ** exp)
        return min(int(RATE_LIMIT_BACKOFF_MAX_SECONDS), delay)

    def _schedule_worker_restart(self, worker_id: int, delay_seconds: int) -> None:
        delay_seconds = max(0, int(delay_seconds))

        with self.worker_restart_lock:
            if worker_id in self.worker_restart_pending:
                return
            self.worker_restart_pending.add(worker_id)

        def _restart() -> None:
            try:
                if delay_seconds:
                    time.sleep(delay_seconds)
                thread = threading.Thread(
                    target=self._worker,
                    args=(worker_id,),
                    daemon=True,
                    name=f"gdl-worker-{worker_id}",
                )
                thread.start()
                self.threads.append(thread)
            finally:
                with self.worker_restart_lock:
                    self.worker_restart_pending.discard(worker_id)

        threading.Thread(target=_restart, daemon=True, name=f"gdl-worker-restart-{worker_id}").start()

    def _terminate_process_tree(self, proc: subprocess.Popen) -> None:
        """Terminate a worker's process tree best-effort.

        On Windows, gallery-dl can spawn child processes; `taskkill /T` ensures
        the full tree is terminated.
        """
        try:
            if proc.poll() is not None:
                return
        except Exception:
            pass

        try:
            if sys.platform.startswith("win"):
                subprocess.run(
                    ["taskkill", "/PID", str(int(proc.pid)), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return

            # POSIX: kill the whole process group (we start gallery-dl in a new
            # session when possible).
            killpg = getattr(os, "killpg", None)
            sigkill = getattr(signal, "SIGKILL", None)
            if callable(killpg) and sigkill is not None:
                try:
                    killpg(int(proc.pid), sigkill)
                    return
                except Exception:
                    pass

            proc.kill()
        except Exception:
            pass

    def interrupt_now(self) -> None:
        """Hard stop: terminate running gallery-dl processes and stop workers.

        This is intended for global Ctrl+C handling.
        """
        self.stop_event.set()
        self.paused_event.clear()

        # Kill any active worker processes.
        with self.worker_process_lock:
            procs = list(self.worker_processes.values())
        for proc in procs:
            if proc is None:
                continue
            self._terminate_process_tree(proc)

        # Unblock workers and let them exit.
        alive_threads = [t for t in self.threads if t.is_alive()]
        for _ in alive_threads:
            try:
                self.q.put(("__QUIT__", 0, None))
            except Exception:
                pass
        for t in alive_threads:
            try:
                t.join(timeout=2)
            except Exception:
                pass

    def _flag_broken(self, url: str, detail: str) -> None:
        if not url:
            return
        with self.broken_lock:
            self.broken_details[url] = detail

    def is_broken(self, url: str) -> bool:
        if not url:
            return False
        with self.broken_lock:
            return url in self.broken_details

    def get_broken_detail(self, url: str) -> Optional[str]:
        if not url:
            return None
        with self.broken_lock:
            return self.broken_details.get(url)

    def _create_workers(self, *, count: int):
        """Create additional worker threads."""
        count = max(0, int(count))
        for _ in range(count):
            worker_id = self.next_worker_id
            self.next_worker_id += 1

            thread = threading.Thread(
                target=self._worker,
                args=(worker_id,),
                daemon=True,
                name=f"gdl-worker-{worker_id}",
            )
            thread.start()
            self.threads.append(thread)

    def _get_desired_workers(self) -> int:
        with self._desired_workers_lock:
            return int(self._desired_workers)

    def resize_workers(self, new_count: int) -> Tuple[int, int]:
        """Resize workers at runtime.

        - Upsize: spawn additional workers immediately.
        - Downsize: workers above the desired count retire after finishing
          their current job (they will not pick up new queue items).

        Returns (old_count, new_count).
        """
        new_count = int(new_count)
        if new_count < 1:
            raise ValueError("workers must be >= 1")

        with self._desired_workers_lock:
            old_count = int(self._desired_workers)
            self._desired_workers = new_count

        # Upsize: create new threads immediately.
        if new_count > self.next_worker_id:
            self._create_workers(count=new_count - self.next_worker_id)

        return old_count, new_count

    def set_idle_callback(self, callback: Optional[Callable[[], None]]) -> None:
        self.idle_callback = callback

    # Worker tracking --------------------------------------------------
    def _ensure_worker_tracking(self, worker_id: int):
        with self.worker_log_lock:
            if worker_id not in self.worker_logs:
                self.worker_logs[worker_id] = WorkerLog()
                self.worker_log_events[worker_id] = threading.Event()
        self._ensure_worker_dir_entry(worker_id)

    def _ensure_worker_dir_entry(self, worker_id: int):
        with self.worker_dir_lock:
            if worker_id not in self.worker_download_dirs:
                self.worker_download_dirs[worker_id] = None

    def _record_worker_output(self, worker_id: int, message: str):
        self._ensure_worker_tracking(worker_id)
        with self.worker_log_lock:
            self.worker_logs[worker_id].append(message)
        self.worker_log_events[worker_id].set()

    def _maybe_notify_all_idle(self) -> None:
        if self.multi_active_since_idle and not self.stats.has_active_downloads():
            self.multi_active_since_idle = False
            if self.idle_callback:
                try:
                    self.idle_callback()
                except Exception:
                    pass

    def _set_worker_active_url(self, worker_id: int, url: Optional[str]) -> None:
        with self.worker_active_lock:
            if url is None:
                self.worker_active_urls.pop(worker_id, None)
            else:
                self.worker_active_urls[worker_id] = url

    def _get_worker_active_url(self, worker_id: int) -> Optional[str]:
        with self.worker_active_lock:
            return self.worker_active_urls.get(worker_id)

    def _flag_unsupported(self, url: str, detail: str):
        if not url:
            return
        with self.unsupported_lock:
            self.unsupported_details[url] = detail

    def _consume_unsupported(self, url: str) -> Optional[str]:
        if not url:
            return None
        with self.unsupported_lock:
            return self.unsupported_details.pop(url, None)

    def _clear_unsupported(self, url: str) -> None:
        if not url:
            return
        with self.unsupported_lock:
            self.unsupported_details.pop(url, None)

    def _set_worker_progress(self, worker_id: int, text: str) -> None:
        self._ensure_worker_tracking(worker_id)
        with self.worker_progress_lock:
            self.worker_progress[worker_id] = text
        self.worker_log_events[worker_id].set()

    def clear_worker_progress(self, worker_id: int) -> None:
        self._ensure_worker_tracking(worker_id)
        with self.worker_progress_lock:
            removed = self.worker_progress.pop(worker_id, None)
        if removed is not None:
            self.worker_log_events[worker_id].set()

    def _directory_size_bytes(self, root: Path) -> int:
        total = 0
        try:
            for dirpath, _, filenames in os.walk(root):
                for name in filenames:
                    try:
                        total += (Path(dirpath) / name).stat().st_size
                    except Exception:
                        continue
        except Exception:
            return 0
        return total

    def _init_worker_transfer(self, worker_id: int, directory: Path) -> None:
        now = time.time()
        base = self._directory_size_bytes(directory)
        state = WorkerTransferState(
            directory=directory,
            start_time=now,
            start_bytes=base,
            last_time=now,
            last_bytes=base,
        )
        with self.worker_transfer_lock:
            self.worker_transfer[worker_id] = state

    def _clear_worker_transfer(self, worker_id: int) -> None:
        with self.worker_transfer_lock:
            self.worker_transfer.pop(worker_id, None)

    def get_worker_transfer_snapshot(
        self, worker_id: int
    ) -> Optional[Tuple[int, float, float]]:
        """Return (downloaded_bytes, bytes_per_sec, elapsed_sec) or None.

        This is an approximation based on how much the worker's base download
        directory grew since the start of the current job.
        """
        with self.worker_transfer_lock:
            state = self.worker_transfer.get(worker_id)
        if state is None:
            return None

        now = time.time()
        current = self._directory_size_bytes(state.directory)
        downloaded = max(0, current - state.start_bytes)
        elapsed = max(0.0, now - state.start_time)

        dt = max(0.001, now - state.last_time)
        delta = max(0, current - state.last_bytes)
        rate = float(delta) / dt

        new_state = state._replace(last_time=now, last_bytes=current)
        with self.worker_transfer_lock:
            self.worker_transfer[worker_id] = new_state
        return downloaded, rate, elapsed

    def get_worker_progress(self, worker_id: int) -> Optional[str]:
        with self.worker_progress_lock:
            return self.worker_progress.get(worker_id)

    def set_worker_download_dir(
        self, worker_id: int, directory: Optional[str]
    ) -> Optional[Path]:
        if worker_id < 0 or worker_id >= self.next_worker_id:
            raise ValueError(f"Worker {worker_id} does not exist")

        self._ensure_worker_dir_entry(worker_id)

        if directory is None:
            with self.worker_dir_lock:
                self.worker_download_dirs[worker_id] = None
            return None

        path = Path(directory).expanduser()
        path.mkdir(parents=True, exist_ok=True)

        with self.worker_dir_lock:
            self.worker_download_dirs[worker_id] = path
        return path

    def get_worker_download_dir(self, worker_id: int) -> Optional[Path]:
        if worker_id < 0:
            return None
        self._ensure_worker_dir_entry(worker_id)
        with self.worker_dir_lock:
            return self.worker_download_dirs.get(worker_id)

    def get_worker_log_tail(
        self, worker_id: int, tail_lines: int
    ) -> Tuple[List[str], int]:
        self._ensure_worker_tracking(worker_id)
        with self.worker_log_lock:
            buffer, _, end_index = self.worker_logs[worker_id].snapshot()
        if tail_lines <= 0:
            return [], end_index
        return buffer[-tail_lines:], end_index

    def get_worker_log_since(
        self, worker_id: int, index: int
    ) -> Tuple[List[str], int, bool]:
        self._ensure_worker_tracking(worker_id)
        with self.worker_log_lock:
            lines, next_index, truncated = self.worker_logs[worker_id].get_since(index)
        return lines, next_index, truncated

    def get_worker_event(self, worker_id: int) -> threading.Event:
        self._ensure_worker_tracking(worker_id)
        return self.worker_log_events[worker_id]

    def import_failed_entries(
        self, entries: Iterable[FailedEntry], *, replace: bool = False
    ) -> None:
        if not self.config.keep_failed and not replace:
            return
        with self.failed_downloads_lock:
            if replace:
                self.failed_downloads.clear()
            for entry in entries:
                self.failed_downloads[entry.url] = entry

    def _record_failed_download(self, url: str, directory: Optional[Path]) -> None:
        if not self.config.keep_failed:
            return
        entry = FailedEntry(url, directory)
        with self.failed_downloads_lock:
            self.failed_downloads[url] = entry

    def _dump_json_metadata(self, worker_id: int, url: str, target_dir: Path) -> None:
        safe_name = _sanitize_filename(url)
        json_path = target_dir / f"{safe_name}.json"
        cmd = [self.exe] + self.config.default_args + ["-J", url]
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                timeout=300,
            )
        except Exception as exc:
            self._record_worker_output(
                worker_id,
                f"[JSON] Failed to dump metadata for {url}: {exc}",
            )
            return

        if result.returncode != 0:
            stderr_snippet = (result.stderr or "").strip().splitlines()
            detail = f" (stderr: {stderr_snippet[0][:200]})" if stderr_snippet else ""
            self._record_worker_output(
                worker_id,
                f"[JSON] gallery-dl -J exited with {result.returncode}{detail}",
            )
            return

        content = result.stdout
        if not content.strip():
            self._record_worker_output(
                worker_id,
                f"[JSON] No metadata returned for {url}",
            )
            return

        existing_text: Optional[str] = None
        if json_path.exists():
            try:
                existing_text = json_path.read_text(encoding="utf-8")
            except Exception:
                existing_text = None

        if existing_text == content:
            self._record_worker_output(
                worker_id,
                f"[JSON] Metadata unchanged for {json_path.name}",
            )
            return

        try:
            json_path.write_text(content, encoding="utf-8")
            if existing_text is None:
                status = "written"
            else:
                status = "updated"
            self._record_worker_output(
                worker_id,
                f"[JSON] Metadata {status}: {json_path}",
            )
        except Exception as exc:
            self._record_worker_output(
                worker_id,
                f"[JSON] Failed writing metadata to {json_path}: {exc}",
            )

    def _clear_failed_download(self, url: str) -> None:
        with self.failed_downloads_lock:
            self.failed_downloads.pop(url, None)

    def get_failed_downloads(self) -> List[FailedEntry]:
        with self.failed_downloads_lock:
            return list(self.failed_downloads.values())

    def failed_count(self) -> int:
        with self.failed_downloads_lock:
            return len(self.failed_downloads)

    def retry_failed(self) -> int:
        with self.failed_downloads_lock:
            entries = list(self.failed_downloads.values())
            self.failed_downloads.clear()

        if not entries:
            return 0

        requeue: List[FailedEntry] = []
        added_count = 0
        for entry in entries:
            if self.add(entry.url, entry.directory):
                added_count += 1
            else:
                requeue.append(entry)

        if requeue and self.config.keep_failed:
            with self.failed_downloads_lock:
                for entry in requeue:
                    self.failed_downloads[entry.url] = entry

        return added_count

    def _register_worker_process(self, worker_id: int, proc: subprocess.Popen):
        with self.worker_process_lock:
            self.worker_processes[worker_id] = proc

    def _clear_worker_process(self, worker_id: int):
        with self.worker_process_lock:
            self.worker_processes.pop(worker_id, None)
        self._ensure_worker_tracking(worker_id)
        self.clear_worker_progress(worker_id)
        self.worker_log_events[worker_id].set()

    def is_worker_active(self, worker_id: int) -> bool:
        if self.stats.is_worker_active(worker_id):
            return True
        with self.worker_process_lock:
            proc = self.worker_processes.get(worker_id)
        if proc is None:
            return False
        return proc.poll() is None

    def _stream_pipe(self, worker_id: int, pipe: Optional[BinaryIO], stream_name: str):
        if pipe is None:
            return
        decoder_cls = codecs.getincrementaldecoder("utf-8")
        decoder = decoder_cls(errors="replace")
        buffer: List[str] = []
        tag = f"[{stream_name}]"
        in_progress = False

        def _emit_buffer_as_log() -> None:
            nonlocal in_progress
            if buffer and not in_progress:
                line = "".join(buffer)
                clean_line = _strip_ansi(line)
                lowered = clean_line.lower()

                # HTTP status parsing (best-effort) from gallery-dl output.
                # Common patterns include:
                # - "HTTP Error 429: Too Many Requests"
                # - "HTTP Error 503: Service Unavailable"
                # - "429 Too Many Requests"
                status_code: Optional[int] = None
                try:
                    m = re.search(r"\bhttp\s*error\s*(\d{3})\b", lowered)
                    if not m:
                        m = re.search(r"\b(\d{3})\b\s*[:\-]?\s*too\s+many\s+requests\b", lowered)
                    if not m:
                        m = re.search(r"\bhttp/\d\.\d\s+(\d{3})\b", lowered)
                    if not m:
                        m = re.search(r"\bstatus\s*code\s*[:=]?\s*(\d{3})\b", lowered)
                    # gallery-dl often logs HTTP issues as downloader warnings, e.g.
                    # "[downloader.http][warning] '502 Bad Gateway'"
                    if not m and "downloader.http" in lowered:
                        m = re.search(r"['\"]\s*(\d{3})\b", clean_line)
                    if not m:
                        # Best-effort patterns for common messages without explicit "HTTP Error" prefix.
                        if any(
                            token in lowered
                            for token in (
                                "bad gateway",
                                "service unavailable",
                                "gateway timeout",
                                "internal server error",
                                "too many requests",
                            )
                        ):
                            m = re.search(r"\b(\d{3})\b", lowered)
                    if m:
                        status_code = int(m.group(1))
                except Exception:
                    status_code = None

                if status_code is not None:
                    if status_code == 429 or "too many requests" in lowered:
                        self._note_worker_http_status(
                            worker_id,
                            429,
                            clean_line.strip() or line.strip(),
                        )
                    elif 500 <= status_code <= 599:
                        self._note_worker_http_status(
                            worker_id,
                            status_code,
                            clean_line.strip() or line.strip(),
                        )

                if "unsupported url" in lowered or "no suitable extractor" in lowered:
                    current_url = self._get_worker_active_url(worker_id)
                    detail = clean_line.strip() or line.strip()
                    self._flag_unsupported(current_url or "", detail)
                self._record_worker_output(worker_id, f"{tag} {line}")
                buffer.clear()
            in_progress = False

        def _process_char(ch: str) -> None:
            nonlocal in_progress
            if ch == "\r" and os.name != "nt":
                if buffer:
                    self._set_worker_progress(worker_id, "".join(buffer))
                buffer.clear()
                in_progress = True
                return

            if ch == "\n":
                if buffer:
                    _emit_buffer_as_log()
                elif not in_progress:
                    self._record_worker_output(worker_id, tag)
                self.clear_worker_progress(worker_id)
                in_progress = False
                buffer.clear()
                return

            buffer.append(ch)
            if in_progress:
                self._set_worker_progress(worker_id, "".join(buffer))

        try:
            while True:
                chunk = pipe.read(1024)
                if chunk == b"":
                    remaining = decoder.decode(b"", final=True)
                    if remaining:
                        for char in remaining:
                            _process_char(char)
                    _emit_buffer_as_log()
                    self.clear_worker_progress(worker_id)
                    break

                text = decoder.decode(chunk, final=False)
                if text:
                    for char in text:
                        _process_char(char)
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    def _snapshot_video_files(self, directory: Path) -> Dict[Path, Tuple[float, int]]:
        """Snapshot video files in a directory tree: path -> (mtime, size)."""
        video_extensions = {
            ".mp4",
            ".webm",
            ".mkv",
            ".avi",
            ".mov",
            ".flv",
            ".wmv",
            ".m4v",
            ".mpg",
            ".mpeg",
            ".3gp",
            ".ts",
        }

        snapshot: Dict[Path, Tuple[float, int]] = {}
        for root, _dirs, files in os.walk(directory):
            for name in files:
                file_path = Path(root) / name
                if file_path.suffix.lower() not in video_extensions:
                    continue
                try:
                    stat = file_path.stat()
                except Exception:
                    continue
                snapshot[file_path] = (float(stat.st_mtime), int(stat.st_size))
        return snapshot

    def _snapshot_job_files(self, directory: Path) -> Dict[Path, Tuple[float, int]]:
        """Snapshot all files in a directory tree: path -> (mtime, size).

        This is used for job-scoped "tiny/bogus" file detection so fragments and
        intermediate downloads are included regardless of file extension.
        """
        snapshot: Dict[Path, Tuple[float, int]] = {}
        for root, _dirs, files in os.walk(directory):
            for name in files:
                file_path = Path(root) / name
                try:
                    stat = file_path.stat()
                except Exception:
                    continue
                snapshot[file_path] = (float(stat.st_mtime), int(stat.st_size))
        return snapshot

    def _sha256_file(self, path_obj: Path) -> Optional[str]:
        try:
            hasher = hashlib.sha256()
            with open(path_obj, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None

    def _precheck_no_download_info(
        self, worker_id: int, url: str
    ) -> Tuple[Dict[str, int], List[str], int, int, List[str]]:
        """Return (expected_size_by_filename, file_urls, expected_item_count, zero_item_count, zero_file_urls).

        Uses `gallery-dl -j --no-download` to obtain extractor metadata.
        This is optional and best-effort; failures only log a warning.
        """
        cmd = [self.exe] + self.config.default_args + ["-j", "--no-download", url]
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                timeout=300,
            )
        except Exception as exc:
            self._record_worker_output(
                worker_id,
                f"[PRECHECK] Failed running precheck: {exc}",
            )
            return {}, [], 0, 0, []

        if result.returncode != 0:
            stderr_snippet = (result.stderr or "").strip().splitlines()
            detail = f" (stderr: {stderr_snippet[0][:200]})" if stderr_snippet else ""
            self._record_worker_output(
                worker_id,
                f"[PRECHECK] gallery-dl precheck exited with {result.returncode}{detail}",
            )
            return {}, [], 0, 0, []

        raw = (result.stdout or "").strip()
        if not raw:
            return {}, [], 0, 0, []

        try:
            payload = json.loads(raw)
        except Exception as exc:
            self._record_worker_output(
                worker_id,
                f"[PRECHECK] Could not parse JSON from -j output: {exc}",
            )
            return {}, [], 0, 0, []

        expected: Dict[str, int] = {}
        file_urls: List[str] = []
        item_count = 0
        zero_item_count = 0
        zero_file_urls: List[str] = []
        missing_size = 0
        missing_name = 0
        if isinstance(payload, list):
            for item in payload:
                # Expected structure: [level, url, metadata]
                if not (isinstance(item, list) and len(item) >= 3):
                    continue
                _level, file_url, meta = item[0], item[1], item[2]
                if not isinstance(meta, dict):
                    continue

                # Count items even if some fields are missing.
                item_count += 1

                if isinstance(file_url, str) and file_url:
                    file_urls.append(file_url)

                size = meta.get("size")
                try:
                    size_int = int(size) if size is not None else None
                except Exception:
                    size_int = None
                if size_int is None or size_int < 0:
                    # Size may be missing for some extractors; best-effort.
                    missing_size += 1
                    size_int = None

                if size_int == 0:
                    zero_item_count += 1
                    if isinstance(file_url, str) and file_url:
                        zero_file_urls.append(file_url)

                candidates: List[str] = []
                ext = meta.get("extension") or meta.get("ext")
                if isinstance(ext, str) and ext:
                    for key in ("uuid", "filename", "name"):
                        base = meta.get(key)
                        if isinstance(base, str) and base:
                            candidates.append(f"{base}.{ext}")

                if isinstance(file_url, str) and file_url:
                    try:
                        parsed = urlparse(file_url)
                        name = Path(parsed.path).name
                        if name:
                            candidates.append(name)
                    except Exception:
                        pass

                if not candidates:
                    missing_name += 1

                # Store size for candidates.
                if size_int is not None:
                    for cand in candidates:
                        expected.setdefault(cand, size_int)

        if missing_size:
            self._record_worker_output(
                worker_id,
                f"[PRECHECK][WARN] {missing_size} item(s) had no usable 'size' field (extractor change or missing metadata?)",
            )
        if missing_name:
            self._record_worker_output(
                worker_id,
                f"[PRECHECK][WARN] {missing_name} item(s) had no usable filename keys (uuid/filename/name/path). Matching may fail.",
            )

        return expected, file_urls, item_count, zero_item_count, zero_file_urls

    def _probe_remote_content_length(self, file_url: str) -> Optional[int]:
        """Try to get remote content length without downloading the file."""
        # HEAD first
        try:
            req = Request(file_url, method="HEAD", headers={"User-Agent": "gallery-dl-queue/1.0"})
            with urlopen(req, timeout=15) as resp:
                length = resp.headers.get("Content-Length")
                if length:
                    return int(length)
        except Exception:
            pass

        # Fallback: GET a single byte using Range and parse Content-Range.
        try:
            req = Request(
                file_url,
                method="GET",
                headers={
                    "Range": "bytes=0-0",
                    "User-Agent": "gallery-dl-queue/1.0",
                },
            )
            with urlopen(req, timeout=15) as resp:
                content_range = resp.headers.get("Content-Range")
                # Example: "bytes 0-0/12345"
                if content_range and "/" in content_range:
                    total = content_range.split("/", 1)[1].strip()
                    if total.isdigit():
                        return int(total)
        except Exception:
            pass

        return None

    def _scrub_files(self, worker_id: int, files: List[Path]) -> None:
        if not files:
            return
        deleted = 0
        failed = 0
        for path_obj in files:
            try:
                path_obj.unlink(missing_ok=True)
                deleted += 1
            except Exception as exc:
                failed += 1
                self._record_worker_output(worker_id, f"[WARN] Could not delete {path_obj}: {exc}")

        if deleted:
            self._record_worker_output(worker_id, f"[CLEANUP] Deleted {deleted} tiny file(s)")
        if failed:
            self._record_worker_output(worker_id, f"[WARN] Failed to delete {failed} tiny file(s)")

    def _check_video_files_size(
        self,
        worker_id: int,
        directory: Path,
        url: str,
        before_snapshot: Dict[Path, Tuple[float, int]],
        job_start_time: float,
        expected_size_by_name: Optional[Dict[str, int]] = None,
    ) -> Tuple[bool, List[Path], List[Path]]:
        """Return (should_fail, tiny_files) for job-scoped bogus/tiny files.

        "Job-scoped" means new or changed files created by this URL run.
        This avoids other URLs in the same download directory masking the check.
        """
        min_size = int(TINY_VIDEO_MIN_BYTES)
        max_size = int(TINY_VIDEO_MAX_BYTES)
        if min_size > max_size:
            min_size, max_size = max_size, min_size

        try:
            after_snapshot = self._snapshot_job_files(directory)

            job_files: List[Tuple[Path, int]] = []
            for path_obj, (mtime, size) in after_snapshot.items():
                before = before_snapshot.get(path_obj)
                is_new_or_changed = before is None or before != (mtime, size)
                is_recent = mtime >= (job_start_time - 1.0)
                if is_new_or_changed or is_recent:
                    job_files.append((path_obj, size))

            if not job_files:
                return False, [], []

            tolerance = 0
            try:
                tolerance = int(getattr(self.config, "expected_size_tolerance", 0) or 0)
            except Exception:
                tolerance = 0
            validate_expected = bool(getattr(self.config, "validate_expected_sizes", True))

            stem_expected: Dict[str, Set[int]] = {}
            if expected_size_by_name:
                for name, expected in expected_size_by_name.items():
                    try:
                        stem = Path(name).stem.lower()
                    except Exception:
                        continue
                    stem_expected.setdefault(stem, set()).add(int(expected))

            def _match_expected(fp: Path) -> Optional[int]:
                if not expected_size_by_name:
                    return None
                direct = expected_size_by_name.get(fp.name)
                if direct is not None:
                    return int(direct)
                # Conservative fallback: match by stem only when unique
                stem = fp.stem.lower()
                candidates = stem_expected.get(stem)
                if candidates and len(candidates) == 1:
                    return int(next(iter(candidates)))
                return None

            bad_files: List[Tuple[Path, int, str]] = []
            for fp, sz in job_files:
                if sz == 0:
                    bad_files.append((fp, sz, "zero"))
                elif min_size <= sz <= max_size:
                    bad_files.append((fp, sz, "tiny"))
                elif validate_expected and expected_size_by_name:
                    expected = _match_expected(fp)
                    if expected is not None and abs(int(sz) - int(expected)) > tolerance:
                        bad_files.append((fp, sz, "mismatch"))

            if bad_files:
                bad_with_hash: List[Tuple[Path, int, str, Optional[str], Optional[int]]] = []
                for fp, sz, reason in bad_files:
                    digest = self._sha256_file(fp)
                    digest_norm = digest.lower() if isinstance(digest, str) else None
                    if digest_norm and digest_norm in TINY_FILE_SHA256_BLACKLIST:
                        reason = "sha256-blacklist" if reason == "tiny" else f"{reason}+sha256-blacklist"
                    expected_size = None
                    if expected_size_by_name:
                        expected_size = _match_expected(fp)
                    bad_with_hash.append((fp, sz, reason, digest, expected_size))

                def _format_detail(
                    fp: Path,
                    sz: int,
                    reason: str,
                    digest: Optional[str],
                    expected_size: Optional[int],
                ) -> str:
                    extra_parts: List[str] = [f"sha256={digest or 'n/a'}"]
                    if digest and digest.lower() in TINY_FILE_SHA256_BLACKLIST:
                        extra_parts.append("blacklisted")
                    if expected_size is not None:
                        extra_parts.append(f"expected={expected_size}")
                    return f"{fp.name} ({sz} bytes, {reason}, " + ", ".join(extra_parts) + ")"

                bad_details = ", ".join(
                    _format_detail(fp, sz, reason, digest, expected)
                    for fp, sz, reason, digest, expected in bad_with_hash[:20]
                )
                extra = "" if len(bad_files) <= 20 else f" (+{len(bad_files) - 20} more)"

                if len(bad_files) == len(job_files):
                    msg = (
                        f"[WARN] Found {len(job_files)} job file(s), all within {min_size}..{max_size} bytes: "
                        f"{bad_details}{extra}"
                    )
                else:
                    msg = (
                        f"[WARN] Found {len(bad_files)}/{len(job_files)} job file(s) that are 0 bytes or within {min_size}..{max_size} bytes: "
                        f"{bad_details}{extra}"
                    )

                self._record_worker_output(worker_id, msg)
                zero_files = [fp for fp, _sz, reason in bad_files if reason == "zero"]
                return True, [fp for fp, _sz, _reason in bad_files], zero_files

            return False, [], []

        except Exception as exc:
            self._record_worker_output(worker_id, f"[WARN] Could not check job file sizes: {exc}")
            return False, [], []

    def add(self, url: str, directory: Optional[Path]) -> bool:
        """Add URL to queue. Returns True if added, False if duplicate."""
        if self.is_broken(url):
            return False
        if url in self.queued_urls or url in self.completed_urls:
            return False

        self.queued_urls.add(url)
        self.q.put((url, 0, directory))
        return True

    def add_force(self, url: str, directory: Optional[Path]) -> bool:
        """Force-add a URL back to the queue, even if it was completed.

        Returns True if enqueued, False if it is already queued/active.
        """
        if self.is_broken(url):
            return False
        if url in self.queued_urls:
            return False

        try:
            active_urls = {u for (u, _d) in self.stats.snapshot_active().values()}
        except Exception:
            active_urls = set()
        if url in active_urls:
            return False

        self.completed_urls.discard(url)
        self.queued_urls.add(url)
        self.q.put((url, 0, directory))
        return True

    def pause(self):
        """Pause all workers."""
        self.paused_event.set()

    def resume(self):
        """Resume all workers."""
        self.paused_event.clear()

    def is_paused(self) -> bool:
        return self.paused_event.is_set()

    def get_queue_size(self) -> int:
        return self.q.qsize()

    def get_active_downloads(self) -> Dict[int, Tuple[str, Optional[Path]]]:
        return self.stats.snapshot_active()

    def wait_empty(self):
        """Wait for queue to be empty."""
        self.q.join()

    def shutdown(self):
        """Gracefully shutdown all workers."""
        self.wait_empty()
        self.stop_event.set()

        # Send quit signals only to threads that are still alive.
        alive_threads = [t for t in self.threads if t.is_alive()]
        for _ in alive_threads:
            self.q.put(("__QUIT__", 0, None))

        # Wait for threads to finish
        for thread in alive_threads:
            thread.join(timeout=10)

    def _worker(self, worker_id: int):
        """Worker thread that processes downloads."""
        while not self.stop_event.is_set():
            try:
                # Wait if paused
                while self.paused_event.is_set() and not self.stop_event.is_set():
                    if worker_id >= self._get_desired_workers():
                        # Retire even while paused (no new work should be picked up).
                        return
                    time.sleep(0.1)

                if self.stop_event.is_set():
                    break

                # Downsize support: do not pick up new work if this worker is above
                # the current desired worker count.
                if worker_id >= self._get_desired_workers():
                    self._record_worker_output(
                        worker_id,
                        f"[INFO] Worker retiring due to worker downsize (desired={self._get_desired_workers()})",
                    )
                    break

                url, retry_count, url_directory = self.q.get(timeout=1)

                if url == "__QUIT__":
                    self.q.task_done()
                    break

                if self.is_broken(url):
                    detail = self.get_broken_detail(url) or "marked as broken"
                    self._record_worker_output(
                        worker_id,
                        f"[BROKEN] Skipping queued URL (marked as broken): {detail}",
                    )
                    print(
                        f"{Fore.RED}BROKEN: Worker {worker_id}: Skipping queued URL {url}\n  -> {detail}{Style.RESET_ALL}"
                    )
                    self.queued_urls.discard(url)
                    self.q.task_done()
                    continue

                self._set_worker_active_url(worker_id, url)
                self._clear_unsupported(url)

                # Determine directory resolution for command and status reporting
                worker_dir = self.get_worker_download_dir(worker_id)
                download_base = (
                    url_directory
                    or worker_dir
                    or self.config.download_dir
                    or Path.cwd()
                )

                if isinstance(download_base, Path):
                    active_dir = download_base
                else:
                    active_dir = Path(download_base).expanduser()

                try:
                    active_dir.mkdir(parents=True, exist_ok=True)
                except Exception as exc:
                    self._record_worker_output(
                        worker_id,
                        f"[WARN] Could not prepare directory {active_dir}: {exc}",
                    )

                active_dir = active_dir.resolve()
                self._record_worker_output(
                    worker_id,
                    f"[INFO] Using download base directory: {active_dir}",
                )

                if self.config.dump_json:
                    self._dump_json_metadata(worker_id, url, active_dir)

                expected_size_by_name: Optional[Dict[str, int]] = None
                precheck_file_urls: List[str] = []
                expected_item_count = 0
                precheck_zero_item_count = 0
                precheck_zero_file_urls: List[str] = []
                if self.config.precheck_no_download:
                    (
                        expected_size_by_name,
                        precheck_file_urls,
                        expected_item_count,
                        precheck_zero_item_count,
                        precheck_zero_file_urls,
                    ) = self._precheck_no_download_info(worker_id, url)
                    if expected_item_count:
                        self._record_worker_output(
                            worker_id,
                            f"[PRECHECK] Reported {expected_item_count} item(s)",
                        )

                # Track current download with resolved directory
                self.stats.set_current_download(worker_id, url, active_dir)
                self._init_worker_transfer(worker_id, active_dir)
                if not self.multi_active_since_idle:
                    active_now = self.stats.snapshot_active()
                    if len(active_now) >= 2:
                        self.multi_active_since_idle = True

                # Reset any previous progress indicator before starting a new task
                self.clear_worker_progress(worker_id)

                # Build command
                cmd = [self.exe] + self.config.default_args
                cmd.extend(["-o", f"base-directory={str(active_dir)}"])
                if self.config.flat_directories:
                    cmd.extend(["-o", "extractor.directory=[]"])
                elif self.config.directory_template:
                    cmd.extend(
                        [
                            "-o",
                            f"extractor.directory={self.config.directory_template}",
                        ]
                    )

                has_output_mode = False
                for idx, value in enumerate(cmd[1:], start=1):
                    if value.startswith("output.mode="):
                        has_output_mode = True
                        break
                    if value == "output.mode" and idx + 1 < len(cmd):
                        has_output_mode = True
                        break
                    if value == "-o" and idx + 1 < len(cmd):
                        next_value = cmd[idx + 1]
                        if (
                            next_value.startswith("output.mode=")
                            or next_value == "output.mode"
                        ):
                            has_output_mode = True
                            break

                if not has_output_mode:
                    cmd.extend(["-o", "output.mode=pipe"])

                cmd.append(url)
                # Execute download
                start_time = time.time()
                before_video_snapshot: Dict[Path, Tuple[float, int]] = {}
                try:
                    before_video_snapshot = self._snapshot_job_files(active_dir)
                except Exception:
                    before_video_snapshot = {}
                proc: Optional[subprocess.Popen] = None
                stdout_thread: Optional[threading.Thread] = None
                stderr_thread: Optional[threading.Thread] = None
                timed_out = False
                force_reappend = False
                rate_limited_restart = False
                rate_limit_detail: Optional[str] = None
                stop_worker_after_task = False
                try:
                    self._record_worker_output(worker_id, f"--- Start download: {url}")

                    precheck_skipped = False
                    return_code = 0
                    success = False
                    unsupported_detail: Optional[str] = None
                    broken_logged = False

                    # Precheck fail-fast: if metadata (or remote probe) suggests bogus files,
                    # optionally skip this download attempt entirely.
                    if self.config.precheck_no_download and self.config.precheck_skip_download_on_fail:
                        min_size = int(TINY_VIDEO_MIN_BYTES)
                        max_size = int(TINY_VIDEO_MAX_BYTES)
                        if min_size > max_size:
                            min_size, max_size = max_size, min_size

                        suspicious: List[str] = []
                        suspicious_zero = False

                        tolerance = 0
                        try:
                            tolerance = int(getattr(self.config, "expected_size_tolerance", 0) or 0)
                        except Exception:
                            tolerance = 0

                        stem_expected: Dict[str, Set[int]] = {}
                        if expected_size_by_name:
                            for name, expected in expected_size_by_name.items():
                                try:
                                    stem = Path(name).stem.lower()
                                except Exception:
                                    continue
                                stem_expected.setdefault(stem, set()).add(int(expected))

                        def _match_expected_name(name: str) -> Optional[int]:
                            if not expected_size_by_name:
                                return None
                            direct = expected_size_by_name.get(name)
                            if direct is not None:
                                return int(direct)
                            stem = Path(name).stem.lower()
                            candidates = stem_expected.get(stem)
                            if candidates and len(candidates) == 1:
                                return int(next(iter(candidates)))
                            return None

                        if expected_size_by_name:
                            for name, expected in expected_size_by_name.items():
                                try:
                                    expected_int = int(expected)
                                except Exception:
                                    continue
                                if expected_int == 0:
                                    suspicious_zero = True
                                    suspicious.append(f"expected {name}={expected_int}")
                                    break
                                if min_size <= expected_int <= max_size:
                                    suspicious.append(f"expected {name}={expected_int}")
                                    break

                        if not suspicious and self.config.precheck_probe_remote_sizes and precheck_file_urls:
                            try:
                                limit = int(self.config.precheck_probe_limit)
                            except Exception:
                                limit = 10
                            limit = max(0, limit)
                            for file_url in precheck_file_urls[:limit]:
                                remote_name: Optional[str] = None
                                try:
                                    remote_name = Path(urlparse(file_url).path).name or None
                                except Exception:
                                    remote_name = None
                                length = self._probe_remote_content_length(file_url)
                                if length is None:
                                    continue
                                if length == 0 or (min_size <= int(length) <= max_size):
                                    if length == 0:
                                        suspicious_zero = True
                                    suspicious.append(f"remote size={length} url={file_url}")
                                    break

                                if remote_name and expected_size_by_name:
                                    expected_len = _match_expected_name(remote_name)
                                    if expected_len is not None and abs(int(length) - int(expected_len)) > tolerance:
                                        suspicious.append(
                                            f"remote mismatch name={remote_name} remote={length} expected={expected_len} url={file_url}"
                                        )
                                        break
                            if len(precheck_file_urls) > limit:
                                self._record_worker_output(
                                    worker_id,
                                    f"[PRECHECK] Probed {limit}/{len(precheck_file_urls)} item(s) for remote size",
                                )

                        if suspicious:
                            detail = ", ".join(suspicious[:3])
                            if suspicious_zero:
                                # For suspected 0-byte items, do not fail-fast skip; run a real download
                                # once so we can confirm and then mark as broken (no retry).
                                self._record_worker_output(
                                    worker_id,
                                    f"[PRECHECK] 0-byte item(s) suspected, continuing with download to confirm: {detail}",
                                )
                            else:
                                self._record_worker_output(
                                    worker_id,
                                    f"[PRECHECK-FAIL] Skipping download (no-download) due to suspicious item(s): {detail}",
                                )
                                # Treat as failure so normal retry logic can handle it.
                                return_code = 98
                                success = False
                                unsupported_detail = self._consume_unsupported(url)
                                timed_out = False
                                precheck_skipped = True

                    if not precheck_skipped:
                        popen_kwargs: Dict[str, object] = {
                            "stdout": subprocess.PIPE,
                            "stderr": subprocess.PIPE,
                            "bufsize": 0,
                        }

                        if sys.platform.startswith("win"):
                            # On Windows, allow sending CTRL-BREAK / controlling process group.
                            # `subprocess.CREATE_NEW_PROCESS_GROUP` exists on supported Python versions.
                            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
                        else:
                            # On POSIX, start a new session if available.
                            setsid = getattr(os, "setsid", None)
                            if callable(setsid):
                                popen_kwargs["preexec_fn"] = setsid

                        proc = subprocess.Popen(cmd, **popen_kwargs)  # type: ignore[arg-type]
                        self._register_worker_process(worker_id, proc)

                        stdout_thread = threading.Thread(
                            target=self._stream_pipe,
                            args=(worker_id, proc.stdout, "STDOUT"),
                            daemon=True,
                        )
                        stderr_thread = threading.Thread(
                            target=self._stream_pipe,
                            args=(worker_id, proc.stderr, "STDERR"),
                            daemon=True,
                        )
                        stdout_thread.start()
                        stderr_thread.start()

                        try:
                            return_code = proc.wait(timeout=86400)
                        except subprocess.TimeoutExpired:
                            timed_out = True
                            self._record_worker_output(
                                worker_id,
                                "[TIMEOUT] Download exceeded 24h, terminating process",
                            )
                            proc.kill()
                            return_code = proc.wait()

                        if stdout_thread is not None:
                            stdout_thread.join(timeout=1)
                        if stderr_thread is not None:
                            stderr_thread.join(timeout=1)

                        success = (return_code == 0) and not timed_out
                        unsupported_detail = self._consume_unsupported(url)

                        # Consume any HTTP actions detected while streaming logs.
                        http_action = self._consume_worker_http_action(worker_id)
                        if http_action is not None:
                            kind, detail = http_action
                            if kind == "rate_limit":
                                rate_limited_restart = True
                                rate_limit_detail = detail
                                success = False
                            elif kind == "server_5xx":
                                force_reappend = True
                                success = False

                    # Check for known-bad bogus/tiny/0-byte artifacts (job-scoped)
                    if not timed_out:
                        video_files_too_small, tiny_video_files, zero_byte_files = self._check_video_files_size(
                            worker_id,
                            active_dir,
                            url,
                            before_video_snapshot,
                            start_time,
                            expected_size_by_name,
                        )
                        if video_files_too_small:
                            success = False
                            self._scrub_files(worker_id, tiny_video_files)
                            self._record_worker_output(
                                worker_id,
                                f"[FAIL] Found bogus file(s) (0 bytes or within {TINY_VIDEO_MIN_BYTES}..{TINY_VIDEO_MAX_BYTES} bytes), considering download failed",
                            )

                            # Broken suppression: if precheck indicated server-side 0-byte items and
                            # a real download attempt confirmed 0-byte artifacts, mark as broken and
                            # do not retry.
                            if precheck_zero_item_count > 0 and zero_byte_files:
                                all_items_zero = (
                                    expected_item_count > 0
                                    and precheck_zero_item_count >= expected_item_count
                                )
                                if all_items_zero:
                                    detail = (
                                        f"precheck reported {precheck_zero_item_count}/{expected_item_count} items as 0 bytes, "
                                        f"download produced {len(zero_byte_files)} 0-byte file(s)"
                                    )
                                    self._flag_broken(url, detail)
                                    self._record_worker_output(
                                        worker_id,
                                        f"[BROKEN] {detail} -> will not retry this URL",
                                    )
                                    print(
                                        f"{Fore.RED}BROKEN: Worker {worker_id}: {url}\n  -> {detail}{Style.RESET_ALL}"
                                    )
                                    broken_logged = True
                                else:
                                    broken_targets = list(dict.fromkeys(precheck_zero_file_urls))
                                    if broken_targets:
                                        detail = (
                                            f"precheck reported {precheck_zero_item_count}/{expected_item_count} item(s) as 0 bytes, "
                                            f"confirmed {len(zero_byte_files)} 0-byte file(s)"
                                        )
                                        for target in broken_targets:
                                            self._flag_broken(target, detail)
                                        self._record_worker_output(
                                            worker_id,
                                            f"[BROKEN] Marked {len(broken_targets)} item URL(s) as broken (0 bytes) -> will not requeue them",
                                        )
                                        print(
                                            f"{Fore.RED}BROKEN: Worker {worker_id}: Marked {len(broken_targets)} item URL(s) as broken (0 bytes) for parent {url}{Style.RESET_ALL}"
                                        )

                    should_retry = False
                    if (
                        not success
                        and not timed_out
                        and retry_count < self.config.max_retries
                    ):
                        if not unsupported_detail:
                            # Pipelines may flush "Unsupported URL" slightly after process exit.
                            time.sleep(0.2)
                            unsupported_detail = self._consume_unsupported(url)
                        # Never retry broken URLs.
                        if self.is_broken(url):
                            should_retry = False
                        else:
                            should_retry = not unsupported_detail

                    # Force reappend when 5xx threshold hit, even if retries exhausted.
                    if force_reappend and not self.is_broken(url):
                        should_retry = True

                    # For 429 rate-limit: stop this worker thread and restart it with exponential backoff.
                    if rate_limited_restart and not self.is_broken(url):
                        should_retry = True

                    if should_retry:
                        attempt = retry_count + 1
                        if force_reappend:
                            # Reappend without consuming retry budget.
                            attempt = retry_count
                        self.stats.record_retry()

                        # Track how often we actually reappend/requeue work.
                        self.stats.record_reappend()

                        if rate_limited_restart:
                            delay = self._compute_rate_limit_backoff(worker_id)
                            detail = rate_limit_detail or "HTTP 429 Too Many Requests"
                            self._record_worker_output(
                                worker_id,
                                f"[RATE-LIMIT] {detail} -> restarting worker with backoff {delay}s, reappending URL",
                            )
                            print(
                                f"{Fore.YELLOW}RATE-LIMIT: Worker {worker_id}: {url}\n  -> backoff {delay}s, worker will restart{Style.RESET_ALL}"
                            )
                            # Delay both requeue and restart to reduce hammering.
                            def _delayed_requeue() -> None:
                                try:
                                    time.sleep(delay)
                                    self.q.put((url, attempt, url_directory))
                                except Exception:
                                    pass

                            threading.Thread(
                                target=_delayed_requeue,
                                daemon=True,
                                name=f"gdl-requeue-429-{worker_id}",
                            ).start()
                            self._schedule_worker_restart(worker_id, delay_seconds=delay)
                            stop_worker_after_task = True

                        if force_reappend:
                            self._record_worker_output(
                                worker_id,
                                f"[REQUEUE] HTTP 5xx threshold reached -> reappending URL in {self.config.retry_delay}s",
                            )
                            print(
                                f"{Fore.YELLOW}REQUEUE: Worker {worker_id}: Reappending after HTTP 5xx threshold: {url}{Style.RESET_ALL}"
                            )
                            time.sleep(self.config.retry_delay)
                            self.q.put((url, attempt, url_directory))
                        else:
                            self._record_worker_output(
                                worker_id,
                                f"[RETRY] Attempt {attempt}/{self.config.max_retries} scheduled in {self.config.retry_delay}s",
                            )
                            print(
                                f"{Fore.YELLOW}WARNING: Worker {worker_id}: Retrying {url} "
                                f"(attempt {attempt}/{self.config.max_retries})"
                            )
                            time.sleep(self.config.retry_delay)
                            self.q.put((url, attempt, url_directory))

                        if stop_worker_after_task:
                            self._record_worker_output(
                                worker_id,
                                "[INFO] Worker stopping due to rate-limit restart",
                            )
                            break
                    else:
                        duration = time.time() - start_time
                        self.stats.add_completion(url, success, duration, worker_id)
                        self._maybe_notify_all_idle()

                        if success:
                            self._clear_unsupported(url)
                            self.completed_urls.add(url)
                            self._clear_failed_download(url)
                            self._record_worker_output(
                                worker_id,
                                f"[SUCCESS] Completed in {duration:.1f}s",
                            )
                            print(
                                f"{Fore.GREEN}SUCCESS: Worker {worker_id}: Completed {url} "
                                f"({duration:.1f}s)"
                            )
                        else:
                            broken_detail = self.get_broken_detail(url)
                            if broken_detail and not broken_logged:
                                self._record_worker_output(
                                    worker_id,
                                    f"[BROKEN] {broken_detail}",
                                )
                                print(
                                    f"{Fore.RED}BROKEN: Worker {worker_id}: {url}\n  -> {broken_detail}{Style.RESET_ALL}"
                                )
                            elif unsupported_detail:
                                self._record_worker_output(
                                    worker_id,
                                    f"[UNSUPPORTED] {unsupported_detail}",
                                )
                                print(
                                    f"{Fore.RED}UNSUPPORTED: Worker {worker_id}: gallery-dl rejected {url}\n  -> {unsupported_detail}"
                                )
                            elif timed_out:
                                self._record_worker_output(
                                    worker_id,
                                    "[FAIL] Download timed out after 86400s",
                                )
                                print(
                                    f"{Fore.RED}TIMEOUT: Worker {worker_id}: Timeout on {url}"
                                )
                            else:
                                self._record_worker_output(
                                    worker_id,
                                    f"[FAIL] gallery-dl exited with code {return_code}",
                                )
                                print(
                                    f"{Fore.RED}FAILED: Worker {worker_id}: Failed {url} "
                                    f"after {retry_count + 1} attempts"
                                )
                            self.queued_urls.discard(url)
                            self._record_failed_download(url, active_dir)

                        if success:
                            self.queued_urls.discard(url)

                except Exception as e:
                    if proc and proc.poll() is None:
                        proc.kill()
                    self._record_worker_output(
                        worker_id,
                        f"[ERROR] Unexpected failure: {e}",
                    )
                    print(f"{Fore.RED}ERROR: Worker {worker_id}: Error on {url}: {e}")
                    self.stats.add_completion(
                        url, False, time.time() - start_time, worker_id
                    )
                    self._maybe_notify_all_idle()
                    self.queued_urls.discard(url)
                    self._record_failed_download(url, active_dir)

                finally:
                    if proc is not None:
                        self._clear_worker_process(worker_id)
                    if stdout_thread is not None and stdout_thread.is_alive():
                        stdout_thread.join(timeout=0.5)
                    if stderr_thread is not None and stderr_thread.is_alive():
                        stderr_thread.join(timeout=0.5)
                    self._set_worker_active_url(worker_id, None)
                    self.clear_worker_progress(worker_id)
                    self._clear_worker_transfer(worker_id)
                    self.q.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"{Fore.RED}CRASH: Worker {worker_id} crashed: {e}")
                break


# ---------------------------------------------------------------------------
# Interactive interface
# ---------------------------------------------------------------------------
class InteractiveQueue(cmd.Cmd):
    command_aliases: Dict[str, str] = {
        "run": "start",
        "go": "start",
        "begin": "start",
        "hold": "pause",
        "stop": "pause",
        "continue": "resume",
        "unpause": "resume",
        "reload": "resume",
        "recover": "resume",
        "resume-session": "resume",
        "stat": "status",
        "info": "status",
        "log": "worker",
        "tail": "worker",
        "feed": "worker",
        "workers": "worker",
        "wd": "workerdir",
        "workdir": "workerdir",
        "dir": "workerdir",
        "hist": "history",
        "recent": "history",
        "cfg": "config",
        "conf": "config",
        "settings": "config",
        "sv": "save",
        "write": "save",
        "export": "save",
        "ld": "load",
        "restore": "load",
        "open": "load",
        "queue": "pending",
        "pend": "pending",
        "list": "pending",
        "links": "urls",
        "all": "urls",
        "rm": "remove",
        "del": "remove",
        "delete": "remove",
        "clear": "cls",
        "cls": "cls",
        "clq": "clearqueue",
        "clearqueue": "clearqueue",
        "clear-queue": "clearqueue",
        "flush": "clearqueue",
        "reset": "clearqueue",
        "exit": "quit",
        "q": "quit",
        "x": "quit",
        "redo": "retry",
        "rerun": "retry",
        "retry-all": "retry",
        "replay": "retry",
        "retryall": "retryall",
        "clip": "clipboard",
        "cb": "clipboard",
    }

    intro = f"{Fore.CYAN}gallery-dl Queue v2.1{Style.RESET_ALL} – type {Fore.YELLOW}?{Style.RESET_ALL} for help"
    prompt = f"{Fore.GREEN}> {Style.RESET_ALL}"
    completekey = "tab"

    def __init__(self):
        super().__init__()
        self.config = Config()
        self.stats = DownloadStats()
        self.pending: List[PendingEntry] = []
        self.all_urls: Set[str] = set()  # Track all URLs ever added
        self.url_directory_overrides: Dict[str, Optional[str]] = {}
        self.dlq: Optional[GDLQueue] = None
        self.status_thread: Optional[threading.Thread] = None
        self.worker_dir_overrides: Dict[int, Optional[Path]] = {}
        self.session_url_directory: Optional[Path] = None
        self.async_messages: "queue.Queue[Optional[str]]" = queue.Queue()
        self._async_printer_thread: Optional[threading.Thread] = None
        self._async_last_was_message = False
        self.failed_offline: Dict[str, FailedEntry] = {}
        self.completed_offline: Set[str] = set()
        self.broken_offline_details: Dict[str, str] = {}
        self.auto_start_ready = threading.Event()
        self.auto_start_done = threading.Event()

        # Clipboard ingest automation
        self.clipboard_enabled = False
        self.clipboard_armed = False
        self.clipboard_whitelist_host: Optional[str] = None
        self._clipboard_stop_event = threading.Event()
        self._clipboard_thread: Optional[threading.Thread] = None
        self._clipboard_last_text: Optional[str] = None
        self._clipboard_seen_urls: Set[str] = set()

        # Load config if exists
        self._load_config()
        self._start_async_printer()

        if not HAS_READLINE:
            # Without a readline-compatible module, cmd's tab completion won't work.
            # Keep it actionable and non-fatal.
            self._notify_async(
                f"{Fore.YELLOW}NOTE: Tab-completion is unavailable. On Windows install it via: pip install pyreadline3{Style.RESET_ALL}"
            )

        self._start_update_check()
        self._schedule_autostart()

    # Clipboard ingest -------------------------------------------------
    def _read_clipboard_text(self) -> Optional[str]:
        """Best-effort clipboard read (Windows via PowerShell)."""
        if not sys.platform.startswith("win"):
            return None
        try:
            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "Get-Clipboard -Raw",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                errors="replace",
                timeout=2,
            )
        except Exception:
            return None
        if result.returncode != 0:
            return None
        text = (result.stdout or "").strip()
        return text or None

    def _normalize_url(self, value: str) -> Optional[str]:
        """Normalize a user-provided URL-ish string.

        - Strips whitespace and common wrappers.
        - Accepts scheme-less domains and prefixes https://.
        - Rejects non-http(s) schemes.
        """
        if not value:
            return None

        text = value.strip()

        # Strip common wrappers.
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
            text = text[1:-1].strip()
        if text.startswith("<") and text.endswith(">") and len(text) > 2:
            text = text[1:-1].strip()

        # Trim trailing punctuation that often appears when copying from chat.
        text = text.rstrip(")]}>,.\"'\n\r\t")

        # Basic sanity: no embedded whitespace/control chars.
        if any(ch.isspace() for ch in text):
            return None
        if any(ord(ch) < 32 for ch in text):
            return None

        # Prefix https:// for scheme-less domains.
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", text):
            if text.lower().startswith("www."):
                text = "https://" + text
            else:
                # Looks like domain[:port][/path]
                if re.match(
                    r"^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
                    r"[A-Za-z]{2,63}(?::\d{1,5})?(?:/.*)?$",
                    text,
                ):
                    text = "https://" + text

        try:
            parsed = urlparse(text)
        except Exception:
            return None

        scheme = (parsed.scheme or "").lower()
        if scheme not in {"http", "https"}:
            return None

        if not parsed.netloc:
            return None

        if not parsed.hostname:
            return None

        return text

    def _extract_urls_from_text(self, text: str) -> List[str]:
        if not text:
            return []
        # Keep this simple and robust; find http(s) URLs (and www.*) and trim common trailing punctuation.
        raw = re.findall(
            r"(?:https?://|www\.)[^\s<>\"']+",
            text,
            flags=re.IGNORECASE,
        )
        cleaned: List[str] = []
        for url in raw:
            normalized = self._normalize_url(url)
            if normalized:
                cleaned.append(normalized)
        return cleaned

    def _normalize_host(self, url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        host = (parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None

    def _host_matches_whitelist(self, host: Optional[str]) -> bool:
        if not host or not self.clipboard_whitelist_host:
            return False
        allowed = self.clipboard_whitelist_host
        return host == allowed or host.endswith("." + allowed)

    def _start_clipboard_monitor(self) -> None:
        if self._clipboard_thread and self._clipboard_thread.is_alive():
            return
        self._clipboard_stop_event.clear()

        def _loop() -> None:
            while not self._clipboard_stop_event.is_set():
                try:
                    text = self._read_clipboard_text()
                    if text and text != self._clipboard_last_text:
                        self._clipboard_last_text = text
                        self._process_clipboard_text(text)
                except Exception:
                    pass
                try:
                    time.sleep(0.6)
                except Exception:
                    pass

        self._clipboard_thread = threading.Thread(
            target=_loop,
            name="clipboard-monitor",
            daemon=True,
        )
        self._clipboard_thread.start()

    def _stop_clipboard_monitor(self) -> None:
        self._clipboard_stop_event.set()
        if self._clipboard_thread and self._clipboard_thread.is_alive():
            self._clipboard_thread.join(timeout=1)
        self._clipboard_thread = None

    def _process_clipboard_text(self, text: str) -> None:
        if not self.clipboard_enabled:
            return
        if self.clipboard_armed or not self.clipboard_whitelist_host:
            # Armed mode learns domain from next manual add.
            return

        urls = self._extract_urls_from_text(text)
        if not urls:
            return

        extracted = 0
        matched_domain = 0
        skipped_seen = 0
        skipped_duplicate_queue = 0
        added_count = 0

        for url in urls:
            extracted += 1
            host = self._normalize_host(url)
            if not self._host_matches_whitelist(host):
                continue
            matched_domain += 1

            if url in self._clipboard_seen_urls:
                skipped_seen += 1
                continue
            self._clipboard_seen_urls.add(url)

            if self._add_url(url, announce=False):
                added_count += 1
            else:
                skipped_duplicate_queue += 1

        # Provide feedback when clipboard contained URLs from the whitelisted domain.
        if matched_domain:
            self._notify_async(
                f"{Fore.CYAN}CLIPBOARD: {matched_domain}/{extracted} link(s) match {self.clipboard_whitelist_host} – "
                f"added {added_count}, skipped {skipped_seen + skipped_duplicate_queue}{Style.RESET_ALL}"
            )

    # Directory helpers -------------------------------------------------
    def _notify_async(self, message: str) -> None:
        self.async_messages.put(message)

    def _start_async_printer(self) -> None:
        if self._async_printer_thread and self._async_printer_thread.is_alive():
            return

        def _printer_loop():
            while True:
                message = self.async_messages.get()
                if message is None:
                    break
                if not message:
                    continue

                if self._async_last_was_message:
                    print(message)
                else:
                    print(f"\n{message}")
                self._async_last_was_message = True

        self._async_printer_thread = threading.Thread(
            target=_printer_loop,
            name="async-printer",
            daemon=True,
        )
        self._async_printer_thread.start()

    def _stop_async_printer(self) -> None:
        if not self._async_printer_thread:
            return
        self.async_messages.put(None)
        self._async_printer_thread.join(timeout=1)
        self._async_printer_thread = None
        self._async_last_was_message = False

    def _start_update_check(self):
        def _runner():
            try:
                outcome = maybe_check_gallery_dl_update(self.config)
            except UpdateRateLimit as exc:
                self._notify_async(
                    f"{Fore.YELLOW}UPDATE: gallery-dl update check rate limited, will retry later ({exc}).{Style.RESET_ALL}"
                )
                return
            except UpdatePermissionError as exc:
                self._notify_async(
                    f"{Fore.YELLOW}UPDATE: {exc}. Auto-update skipped.{Style.RESET_ALL}"
                )
                return
            except UpdateError as exc:
                self._notify_async(
                    f"{Fore.YELLOW}UPDATE: gallery-dl update check failed: {exc}{Style.RESET_ALL}"
                )
                return
            except Exception as exc:
                self._notify_async(
                    f"{Fore.YELLOW}UPDATE: Unexpected gallery-dl update error: {exc}{Style.RESET_ALL}"
                )
                return

            if not outcome:
                return

            local_version, remote_version, updated = outcome
            version_display = local_version or remote_version
            if updated:
                self._notify_async(
                    f"{Fore.GREEN}UPDATE: gallery-dl auto-updated to {version_display}{Style.RESET_ALL}"
                )
            else:
                self._notify_async(
                    f"{Fore.CYAN}UPDATE: gallery-dl already up to date ({version_display}){Style.RESET_ALL}"
                )

        threading.Thread(target=_runner, name="gallery-dl-update", daemon=True).start()

    def _print_worker_dirs(self, *, label: str = "Worker Directories"):
        print(f"\n{Fore.CYAN}{label}:{Style.RESET_ALL}")
        if self.dlq:
            for worker_id in range(self.dlq.next_worker_id):
                print(f"  Worker {worker_id}: {self._format_worker_dir(worker_id)}")
        elif self.worker_dir_overrides:
            for worker_id in sorted(self.worker_dir_overrides.keys()):
                print(f"  Worker {worker_id}: {self._format_worker_dir(worker_id)}")
        else:
            print("  (using global/default directory)")

    def _has_worker_dir_overrides(self) -> bool:
        """Return True if any per-worker directory is configured."""
        if self.dlq:
            try:
                for worker_id in range(self.dlq.next_worker_id):
                    if self.dlq.get_worker_download_dir(worker_id):
                        return True
            except Exception:
                return False
            return False
        return any(self.worker_dir_overrides.values())

    def _format_worker_dir(self, worker_id: int) -> str:
        directory = self._get_worker_dir(worker_id)
        return self._format_directory_label(directory)

    def _format_directory_label(self, directory: Optional[Path | str]) -> str:
        if not directory:
            return "(use global/default)"

        directory_path: Optional[Path]
        try:
            directory_path = Path(directory).expanduser()
        except Exception:
            directory_path = None

        free_info = ""
        if directory_path:
            free_label = self._format_free_space_label(directory_path)
            if free_label:
                free_info = f" ({free_label})"
        directory_str = str(directory)
        return f"{directory_str}{free_info}"

    def _remember_directory_for_url(
        self, url: str, directory: Optional[Path | str]
    ) -> None:
        if directory:
            self.url_directory_overrides[url] = str(directory)
        else:
            self.url_directory_overrides.pop(url, None)

    def _forget_directory_for_url(self, url: str) -> None:
        self.url_directory_overrides.pop(url, None)

    def _format_free_space_label(self, path_obj: Path) -> Optional[str]:
        try:
            usage = shutil.disk_usage(path_obj)
        except (OSError, ValueError):
            return None
        free_in_units = usage.free / (1024**3)
        units = ["GB", "TB", "PB", "EB", "ZB"]
        idx = 0
        while free_in_units >= 1024 and idx < len(units) - 1:
            free_in_units /= 1024
            idx += 1
        amount = f"{free_in_units:.1f}".replace(".", ",")
        return f"{amount} {units[idx]} free space"

    def _expanduser_mkdir(
        self,
        directory_value: str,
        *,
        fail_prefix: str,
        color: str = Fore.YELLOW,
    ) -> Optional[Path]:
        """Expand and create a directory, returning Path or None on failure."""
        try:
            directory_path = Path(directory_value).expanduser()
            directory_path.mkdir(parents=True, exist_ok=True)
            return directory_path
        except Exception as exc:
            print(f"{color}{fail_prefix} ({directory_value}): {exc}")
            return None

    def _complete_file_paths(
        self,
        text: str,
        *,
        suffixes: Optional[Set[str]] = None,
    ) -> List[str]:
        """Return filesystem path completions for the given fragment.

        Intentionally restricted to the current working directory.
        """
        fragment_raw = text or ""

        # Track whether the user started a quoted path.
        quote_char: Optional[str] = None
        fragment = fragment_raw
        if fragment.startswith(('"', "'")):
            quote_char = fragment[0]
            fragment = fragment[1:]

        fragment = os.path.expanduser(fragment)
        fragment = fragment.replace("/", os.sep)

        # Only use the basename as prefix; do not traverse directories.
        base_prefix = Path(fragment).name if fragment else ""
        needle = base_prefix.lower()

        try:
            entries = list(Path.cwd().iterdir())
        except Exception:
            return []

        want_suffixes = {s.lower() for s in suffixes} if suffixes else None

        matches: List[str] = []
        for entry in entries:
            name = entry.name
            if needle and not name.lower().startswith(needle):
                continue

            if entry.is_dir():
                suggestion = name + os.sep
            else:
                if want_suffixes is not None and entry.suffix.lower() not in want_suffixes:
                    continue
                suggestion = name

            if " " in suggestion:
                suggestion = f'"{suggestion}"'
            elif quote_char:
                suggestion = f"{quote_char}{suggestion}"
            matches.append(suggestion)

        return sorted(matches)

    def _resolve_session_filename(
        self,
        value: str,
        *,
        must_exist: bool,
        allowed_suffixes: Set[str],
    ) -> Optional[str]:
        """Resolve a session filename.

        Supports prefix matching so `load jac` can resolve to `jac.conf` / `jac.json`.
        """
        raw = (value or "").strip()
        if not raw:
            raw = self.config.session_file

        # Strip quotes.
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
            raw = raw[1:-1]

        # Restrict to current directory; ignore any provided path components.
        prefix = Path(raw).name
        candidate = Path.cwd() / prefix
        if candidate.exists():
            return str(candidate)

        try:
            entries = list(Path.cwd().iterdir())
        except Exception:
            entries = []

        matches: List[Path] = []
        for entry in entries:
            if not entry.is_file():
                continue
            # No extension filtering here by design.
            if prefix and not entry.name.lower().startswith(prefix.lower()):
                continue
            matches.append(entry)

        if len(matches) == 1:
            return str(matches[0])

        if len(matches) > 1:
            print(f"{Fore.YELLOW}Multiple matches:{Style.RESET_ALL}")
            for m in sorted(matches):
                print(f"  {m.name}")
            return None

        if must_exist:
            return None
        return str(candidate)

    def complete_save(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        try:
            parts = shlex.split(line, posix=False)
        except Exception:
            parts = line.split()

        # Only complete the first argument (filename).
        if len(parts) > 2:
            return []
        return self._complete_file_paths(text)

    def complete_load(self, text: str, line: str, begidx: int, endidx: int) -> List[str]:
        try:
            parts = shlex.split(line, posix=False)
        except Exception:
            parts = line.split()

        if len(parts) > 2:
            return []
        return self._complete_file_paths(text)

    # Aliases share the same completion.
    complete_sv = complete_save
    complete_write = complete_save
    complete_export = complete_save
    complete_ld = complete_load
    complete_restore = complete_load
    complete_open = complete_load

    def _get_worker_dir(self, worker_id: int) -> Optional[str]:
        if self.dlq and worker_id < self.dlq.next_worker_id:
            target_dir = self.dlq.get_worker_download_dir(worker_id)
            if target_dir:
                return str(target_dir)
        if worker_id in self.worker_dir_overrides:
            override = self.worker_dir_overrides[worker_id]
            return str(override) if override else None
        if self.config.download_dir:
            return str(self.config.download_dir)
        return None

    # Utility methods ------------------------------------------------------
    def _validate_url(self, url: str) -> bool:
        """URL validation with normalization."""
        return self._normalize_url(url) is not None

    def _config_file(self) -> Path:
        return Path("gallery_dl_queue_config.json")

    def _config_backup_file(self) -> Path:
        config_file = self._config_file()
        return config_file.with_name(f"{config_file.stem}.backup.json")

    def _save_config(self):
        """Save current configuration and update backup."""
        data = self.config.to_dict()
        config_file = self._config_file()
        backup_file = self._config_backup_file()

        try:
            with open(config_file, "w") as handle:
                json.dump(data, handle, indent=2)
        except Exception as exc:
            print(f"{Fore.RED}Failed to save config: {exc}")

        try:
            with open(backup_file, "w") as handle:
                json.dump(data, handle, indent=2)
        except Exception as exc:
            print(
                f"{Fore.YELLOW}Note: Could not write config backup ({backup_file}): {exc}"
            )

    def _load_config(self):
        """Load configuration from primary file, falling back to backup if needed."""
        config_file = self._config_file()
        backup_file = self._config_backup_file()

        def _load_from(path: Path) -> bool:
            with open(path) as handle:
                data = json.load(handle)
            self.config.from_dict(data)
            return True

        loaded = False
        try:
            if config_file.exists():
                loaded = _load_from(config_file)
        except Exception as exc:
            print(f"{Fore.YELLOW}Note: Could not load config ({config_file}): {exc}")

        if not loaded and backup_file.exists():
            try:
                _load_from(backup_file)
                loaded = True
                print(
                    f"{Fore.CYAN}Loaded configuration from backup ({backup_file}).{Style.RESET_ALL}"
                )
            except Exception as exc:
                print(
                    f"{Fore.YELLOW}Note: Could not load config backup ({backup_file}): {exc}"
                )

    def _ensure_downloader(self) -> bool:
        """Create downloader if it doesn't exist."""
        if self.dlq is None:
            try:
                failed_seed = (
                    list(self.failed_offline.values()) if self.failed_offline else None
                )
                self.dlq = GDLQueue(
                    self.config,
                    self.stats,
                    failed_seed=failed_seed,
                )

                # If we previously loaded a session without running workers,
                # carry over completed URLs so duplicates stay blocked and retryall
                # can work after starting.
                if self.completed_offline:
                    self.dlq.completed_urls.update(self.completed_offline)

                # Carry over broken URL suppression loaded while offline.
                if self.broken_offline_details:
                    for broken_url, detail in list(self.broken_offline_details.items()):
                        try:
                            self.dlq._flag_broken(broken_url, detail)
                        except Exception:
                            pass
                self.dlq.set_idle_callback(
                    lambda: self._notify_async(
                        f"{Fore.CYAN}All workers completed their work and are idle again.{Style.RESET_ALL}"
                    )
                )
                if self.failed_offline:
                    self.failed_offline.clear()
                print(f"STARTED: {self.config.workers} workers")

                if self.config.auto_status:
                    self._start_status_updates()
            except Exception as exc:
                print(f"{Fore.RED}Failed to start workers: {exc}")
                self.dlq = None
                return False
        return True

    def _start_status_updates(self):
        """Start background thread for status updates."""

        if self.status_thread and self.status_thread.is_alive():
            return

        def _status_loop():
            try:
                while self.dlq and not self.dlq.stop_event.is_set():
                    if not self.config.auto_status:
                        break

                    active = self.dlq.get_active_downloads()
                    if active:
                        queue_size = self.dlq.get_queue_size()
                        print(
                            f"\r{Fore.BLUE}STATUS: Active: {len(active)}, Queue: {queue_size}    ",
                            end="",
                        )
                    time.sleep(2)
            finally:
                self.status_thread = None

        self.status_thread = threading.Thread(target=_status_loop, daemon=True)
        self.status_thread.start()

    def _schedule_autostart(self):
        if self.pending:
            self.auto_start_ready.set()

        def _auto_runner():
            while not self.auto_start_done.is_set():
                self.auto_start_ready.wait()
                if self.auto_start_done.is_set():
                    break
                try:
                    time.sleep(5)
                except Exception:
                    pass
                if self.auto_start_done.is_set():
                    break
                if self.dlq or not self.pending:
                    self.auto_start_ready.clear()
                    continue
                try:
                    self.auto_start_done.set()
                    self.do_start("")
                except Exception as exc:
                    self.auto_start_done.clear()
                    self._notify_async(
                        f"{Fore.YELLOW}AUTO-START: Unable to start workers automatically: {exc}{Style.RESET_ALL}"
                    )
                    # Allow reattempt on next pending signal
                    self.auto_start_ready.clear()

        threading.Thread(target=_auto_runner, name="auto-start", daemon=True).start()

    def _signal_auto_start_ready(self) -> None:
        if not self.dlq and self.pending:
            self.auto_start_ready.set()

    def _apply_session_data(self, session_data: dict, *, merge: bool) -> int:
        # ------------------------------------------------------------------
        # Session format v3 (current) / v4 (legacy label): human-readable URLs grouped by directory
        #
        # Shape:
        #   pending_by_path: {"C:/path": {"1": "url", "2": "url"}, "": {...}}
        #   completed_by_path / failed_by_path: same idea
        #
        # Normalize to the legacy shape expected below.
        # ------------------------------------------------------------------
        if isinstance(session_data, dict) and (
            session_data.get("format") in {3, 4}
            or isinstance(session_data.get("pending_by_path"), dict)
        ):
            normalized: Dict[str, object] = dict(session_data)

            def _extract_urls(value: object) -> List[str]:
                if isinstance(value, dict):
                    # Prefer numeric key ordering for stable reconstruction.
                    keys = list(value.keys())
                    try:
                        keys.sort(key=lambda k: int(str(k)))
                    except Exception:
                        keys.sort(key=lambda k: str(k))
                    urls: List[str] = []
                    for k in keys:
                        u = value.get(k)
                        if u:
                            urls.append(str(u))
                    return urls
                if isinstance(value, list):
                    return [str(u) for u in value if u]
                return []

            pending_payload: List[object] = []
            all_urls: Set[str] = set()
            overrides: Dict[str, Optional[str]] = {}

            pending_by_path = session_data.get("pending_by_path") or {}
            if isinstance(pending_by_path, dict):
                for path_key, id_map in pending_by_path.items():
                    path_value = str(path_key) if path_key is not None else ""
                    urls = _extract_urls(id_map)
                    for url in urls:
                        all_urls.add(url)
                        if path_value:
                            pending_payload.append({"url": url, "directory": path_value})
                            overrides[url] = path_value
                        else:
                            pending_payload.append(url)
            normalized["pending"] = pending_payload

            completed_urls: List[str] = []
            completed_by_path = session_data.get("completed_by_path") or {}
            if isinstance(completed_by_path, dict):
                for path_key, id_map in completed_by_path.items():
                    path_value = str(path_key) if path_key is not None else ""
                    urls = _extract_urls(id_map)
                    for url in urls:
                        completed_urls.append(url)
                        all_urls.add(url)
                        if path_value:
                            overrides[url] = path_value
            else:
                completed_payload = session_data.get("completed")
                if isinstance(completed_payload, list):
                    # allow legacy completed list to coexist
                    completed_urls = [str(u) for u in completed_payload if u]
                    all_urls.update(completed_urls)
            normalized["completed"] = completed_urls

            failed_payload: List[object] = []
            failed_by_path = session_data.get("failed_by_path") or {}
            if isinstance(failed_by_path, dict):
                for path_key, id_map in failed_by_path.items():
                    path_value = str(path_key) if path_key is not None else ""
                    urls = _extract_urls(id_map)
                    for url in urls:
                        all_urls.add(url)
                        if path_value:
                            failed_payload.append({"url": url, "directory": path_value})
                            overrides[url] = path_value
                        else:
                            failed_payload.append({"url": url, "directory": None})
            if failed_payload:
                normalized["failed"] = failed_payload

            # Keep explicit overrides if provided, otherwise infer from grouping.
            overrides_payload = session_data.get("url_directory_overrides")
            if isinstance(overrides_payload, dict):
                normalized["url_directory_overrides"] = dict(overrides_payload)
            elif overrides:
                normalized["url_directory_overrides"] = overrides

            normalized["all_urls"] = sorted(all_urls)
            session_data = normalized

        # ------------------------------------------------------------------
        # Indexed session format (removed)
        #
        # Previously, sessions could be stored as an indexed URL table grouped
        # by directory. That on-disk structure is no longer supported.
        # Detect it by its keys so we don't clash with the current numeric label.
        # ------------------------------------------------------------------
        if isinstance(session_data, dict) and (
            "pending_by_directory" in session_data
            or "url_directory_overrides_idx" in session_data
            or "worker_directories_idx" in session_data
            or ("urls" in session_data and "directories" in session_data)
        ):
            raise UnsupportedSessionFormatError(
                "Indexed session format is no longer supported. "
                "Please migrate this session to the current by-path format."
            )

        pending_payload = session_data.get("pending", []) or []
        restored_entries: List[PendingEntry] = []

        if not merge:
            self.pending = []
            self.all_urls = set()
            if not self.dlq:
                self.worker_dir_overrides.clear()
            self.url_directory_overrides.clear()

        for item in pending_payload:
            directory_path: Optional[Path] = None
            if isinstance(item, dict):
                url = item.get("url")
                if not url:
                    continue
                directory_value = item.get("directory")
                if directory_value:
                    directory_path = self._expanduser_mkdir(
                        directory_value,
                        fail_prefix="Note: Could not prepare pending directory",
                    )
            else:
                url = str(item)

            restored_entry = PendingEntry(url, directory_path)
            restored_entries.append(restored_entry)
            self._remember_directory_for_url(url, directory_path)

        if merge:
            self.pending.extend(restored_entries)
            self.all_urls.update(entry.url for entry in restored_entries)
        else:
            self.pending = list(restored_entries)
            self.all_urls = {entry.url for entry in restored_entries}

        # Restore broader URL tracking and per-URL directory overrides (for retryall etc.).
        all_urls_payload = session_data.get("all_urls")
        if isinstance(all_urls_payload, list):
            urls_from_payload = {str(u) for u in all_urls_payload if u}
            if merge:
                self.all_urls.update(urls_from_payload)
            else:
                self.all_urls = set(urls_from_payload) or self.all_urls

        overrides_payload = session_data.get("url_directory_overrides")
        if isinstance(overrides_payload, dict):
            for url, directory in overrides_payload.items():
                if not url:
                    continue
                if directory:
                    self.url_directory_overrides[str(url)] = str(directory)
                else:
                    self.url_directory_overrides.pop(str(url), None)

        completed_payload = session_data.get("completed")
        loaded_completed_urls: Optional[Set[str]] = None
        if isinstance(completed_payload, list):
            loaded_completed_urls = {str(u) for u in completed_payload if u}
            self.all_urls.update(loaded_completed_urls)
            if self.dlq:
                if not merge:
                    self.dlq.completed_urls.clear()
                self.dlq.completed_urls.update(loaded_completed_urls)
            else:
                if not merge:
                    self.completed_offline.clear()
                self.completed_offline.update(loaded_completed_urls)

        if "config" in session_data:
            self.config.from_dict(session_data["config"])

        next_dir_value = session_data.get("session_url_directory")
        if next_dir_value:
            next_dir_path = self._expanduser_mkdir(
                next_dir_value,
                fail_prefix="Note: Could not restore session default directory",
            )
            self.session_url_directory = next_dir_path
        else:
            self.session_url_directory = None

        worker_dirs_data = session_data.get("worker_directories")
        if worker_dirs_data is not None:
            # Only restore worker directory slots that fit the current worker count.
            # This prevents errors when older sessions contain more workers than
            # the current configuration (e.g. after downsizing 8 -> 4 workers).
            try:
                worker_limit = int(getattr(self.config, "workers", 0) or 0)
            except Exception:
                worker_limit = 0
            target_dirs: Dict[int, Optional[Path]] = {}
            for worker_id_str, directory_value in worker_dirs_data.items():
                try:
                    worker_id = int(worker_id_str)
                except (TypeError, ValueError):
                    continue

                if worker_limit and worker_id >= worker_limit:
                    continue

                directory_path = None
                if directory_value:
                    directory_path = self._expanduser_mkdir(
                        directory_value,
                        fail_prefix="Note: Could not restore worker directory",
                    )

                target_dirs[worker_id] = directory_path

            if self.dlq:
                for worker_id, path_obj in target_dirs.items():
                    try:
                        self.dlq.set_worker_download_dir(
                            worker_id, str(path_obj) if path_obj else None
                        )
                    except Exception as exc:
                        print(
                            f"{Fore.YELLOW}Note: Could not assign worker {worker_id} directory while loading: {exc}"
                        )
            else:
                if not merge:
                    self.worker_dir_overrides.clear()
                for worker_id, path_obj in target_dirs.items():
                    self.worker_dir_overrides[worker_id] = path_obj
        elif not merge and not self.dlq:
            self.worker_dir_overrides.clear()

        failed_payload = session_data.get("failed", []) or []
        restored_failures: List[FailedEntry] = []
        for item in failed_payload:
            if isinstance(item, dict):
                url = item.get("url")
                if not url:
                    continue
                directory_value = item.get("directory")
                directory_path = None
                if directory_value:
                    directory_path = self._expanduser_mkdir(
                        directory_value,
                        fail_prefix="Note: Could not prepare failed directory",
                    )
                restored_failures.append(FailedEntry(url, directory_path))
            else:
                restored_failures.append(FailedEntry(str(item), None))

        if restored_failures and not self.config.keep_failed:
            print(
                f"{Fore.YELLOW}Note: keep_failed is disabled, skipping restoration of {len(restored_failures)} failed downloads"
            )
            restored_failures = []

        if restored_failures:
            if self.dlq:
                self.dlq.import_failed_entries(restored_failures, replace=not merge)
            else:
                if not merge:
                    self.failed_offline.clear()
                for entry in restored_failures:
                    self.failed_offline[entry.url] = entry
        elif not merge and not self.dlq:
            self.failed_offline.clear()

        # Restore broken URL suppression
        broken_payload = session_data.get("broken")
        if isinstance(broken_payload, dict):
            restored_broken = {str(k): str(v) for k, v in broken_payload.items() if k}
            if self.dlq:
                if not merge:
                    try:
                        with self.dlq.broken_lock:
                            self.dlq.broken_details.clear()
                    except Exception:
                        pass
                for k, v in restored_broken.items():
                    try:
                        self.dlq._flag_broken(k, v)
                    except Exception:
                        pass
            else:
                if not merge:
                    self.broken_offline_details.clear()
                self.broken_offline_details.update(restored_broken)
        elif not merge and not self.dlq:
            self.broken_offline_details.clear()

        # Backward compatibility: older session files may not have an explicit
        # completed URL list. If we have all_urls, infer completed URLs as
        # all_urls - pending - failed.
        if loaded_completed_urls is None and self.all_urls:
            pending_urls = {entry.url for entry in self.pending}
            failed_urls = {entry.url for entry in restored_failures}
            inferred_completed = set(self.all_urls) - pending_urls - failed_urls
            if inferred_completed:
                if self.dlq:
                    if not merge:
                        self.dlq.completed_urls.clear()
                    self.dlq.completed_urls.update(inferred_completed)
                else:
                    if not merge:
                        self.completed_offline.clear()
                    self.completed_offline.update(inferred_completed)
        elif not self.dlq and not merge and loaded_completed_urls is None:
            # No way to infer (e.g. missing all_urls) -> keep empty.
            self.completed_offline.clear()

        self._save_config()

        self._signal_auto_start_ready()

        return len(restored_entries)

    # Commands -------------------------------------------------------------
    def precmd(self, line: str) -> str:
        stripped = line.strip()
        if stripped:
            parts = stripped.split(None, 1)
            command = parts[0].lower()
            remainder = parts[1] if len(parts) > 1 else ""
            canonical = self.command_aliases.get(command)
            if canonical:
                line = f"{canonical} {remainder}".strip()
        return super().precmd(line)

    def postcmd(self, stop: bool, line: str) -> bool:
        stop = super().postcmd(stop, line)
        return stop

    def default(self, line: str) -> None:
        """Handle URLs and unknown commands."""
        line = line.strip()
        if not line:
            return

        # Check if it looks like a URL
        normalized = self._normalize_url(line)
        if normalized:
            self._add_url(normalized)
            return

        print(f"{Fore.RED}Unknown command or invalid URL: {line}")
        print(f"Type {Fore.YELLOW}?{Style.RESET_ALL} for help")
        return

    def _add_url(self, url: str, *, announce: bool = True) -> bool:
        """Add a URL to queue or pending list."""
        normalized = self._normalize_url(url)
        if not normalized:
            if announce:
                print(f"{Fore.RED}Invalid URL: {url}")
            return False

        url = normalized

        # If clipboard is armed, learn whitelist domain from the next manual URL.
        if self.clipboard_enabled and self.clipboard_armed and announce:
            host = self._normalize_host(url)
            if host:
                self.clipboard_whitelist_host = host
                self.clipboard_armed = False
                self._notify_async(
                    f"{Fore.GREEN}CLIPBOARD: Whitelisted domain '{host}'. Now auto-ingesting clipboard links from this domain.{Style.RESET_ALL}"
                )

        # Always track the URL
        self.all_urls.add(url)

        directory_override = self.session_url_directory
        added_now = False

        if self.dlq is None:
            if not any(entry.url == url for entry in self.pending):
                self.pending.append(PendingEntry(url, directory_override))
                self._signal_auto_start_ready()
                dir_msg = f" (dir: {directory_override})" if directory_override else ""
                if announce:
                    print(
                        f"{Fore.WHITE}PENDING: Added to pending: {url}{dir_msg}{Style.RESET_ALL}"
                    )
                added_now = True
                self._remember_directory_for_url(url, directory_override)
            else:
                if announce:
                    print(f"{Fore.YELLOW}WARNING: Already in pending: {url}")
        else:
            if self.dlq.add(url, directory_override):
                dir_msg = f" (dir: {directory_override})" if directory_override else ""
                if announce:
                    print(
                        f"{Fore.WHITE}QUEUED: Queued for download: {url}{dir_msg}{Style.RESET_ALL}"
                    )
                added_now = True
                self._remember_directory_for_url(url, directory_override)
            else:
                if announce:
                    print(
                        f"{Fore.YELLOW}WARNING: Duplicate URL (already queued/completed): {url}"
                    )

        if added_now and self.dlq is None:
            if announce:
                print(
                    f"{Fore.WHITE}AUTO-START: Workers will start automatically after 5s delay{Style.RESET_ALL}"
                )
                print(
                    f"{Fore.WHITE}PENDING: URL added. Use 'start' to begin downloading.{Style.RESET_ALL}"
                )

        return added_now

    def do_clipboard(self, arg: str) -> bool:
        """Toggle clipboard ingest automation (arm -> whitelist next manual URL -> auto-add)."""
        if not sys.platform.startswith("win"):
            print(f"{Fore.RED}CLIPBOARD: Not supported on this platform{Style.RESET_ALL}")
            return False

        if not self.clipboard_enabled:
            self.clipboard_enabled = True
            # Preserve whitelist across enable/disable; only arm if we don't have one yet.
            self.clipboard_armed = self.clipboard_whitelist_host is None
            self._clipboard_last_text = None
            self._start_clipboard_monitor()
            if self.clipboard_armed:
                print(
                    f"{Fore.GREEN}CLIPBOARD: Armed. Add one URL normally to whitelist its domain, then clipboard links from that domain will be queued automatically.{Style.RESET_ALL}"
                )
            else:
                print(
                    f"{Fore.GREEN}CLIPBOARD: Enabled. Whitelist is '{self.clipboard_whitelist_host}'. Clipboard links from this domain will be queued automatically.{Style.RESET_ALL}"
                )
            return False

        # Disable
        self.clipboard_enabled = False
        self.clipboard_armed = False
        self._stop_clipboard_monitor()
        kept = f" (kept whitelist: {self.clipboard_whitelist_host})" if self.clipboard_whitelist_host else ""
        print(f"{Fore.YELLOW}CLIPBOARD: Disabled{kept}{Style.RESET_ALL}")
        return False

    def do_start(self, arg: str) -> bool:
        """Start workers and begin downloading."""
        if not self._ensure_downloader():
            return False

        # `_ensure_downloader` guarantees dlq is ready.
        assert self.dlq is not None

        self.auto_start_done.set()
        self.auto_start_ready.clear()

        if self.dlq.is_paused():
            self.dlq.resume()
            print(f"{Fore.GREEN}RESUMED: Resumed workers")

        # Add pending URLs and track them
        if self.worker_dir_overrides:
            max_worker_id = getattr(self.dlq, "next_worker_id", 0) or 0
            for worker_id, path in list(self.worker_dir_overrides.items()):
                if max_worker_id and worker_id >= max_worker_id:
                    continue
                try:
                    self.dlq.set_worker_download_dir(
                        worker_id, str(path) if path else None
                    )
                except Exception as e:
                    print(f"{Fore.RED}Failed to set worker {worker_id} directory: {e}")
            self.worker_dir_overrides.clear()

        added = 0
        for entry in list(self.pending):
            if self.dlq.add(entry.url, entry.directory):
                added += 1
                self.pending.remove(entry)

        if added:
            print(f"{Fore.GREEN}STARTED: Added {added} URLs from pending list")

        if (
            added == 0
            and not self.dlq.get_queue_size()
            and not self.dlq.get_active_downloads()
        ):
            print(f"{Fore.YELLOW}No URLs to download. Add some URLs first!")

        return False

    def do_pause(self, arg: str) -> bool:
        """Pause all workers."""
        if self.dlq:
            self.dlq.pause()
            print(f"{Fore.YELLOW}PAUSED: Paused all workers")
        else:
            print(f"{Fore.RED}No workers running")
        return False

    def do_resume(self, arg: str) -> bool:
        """Resume paused workers or reload the last saved session when idle."""
        if self.dlq:
            self.dlq.resume()
            print(f"{Fore.GREEN}RESUMED: Resumed all workers")
            return False

        filename = arg.strip() or self.config.session_file
        session_path = Path(filename)

        if not session_path.exists():
            print(f"{Fore.RED}Session file not found: {session_path}")
            return False

        try:
            with open(session_path) as f:
                session_data = json.load(f)
        except Exception as exc:
            print(f"{Fore.RED}Failed to resume session: {exc}")
            return False

        try:
            restored_count = self._apply_session_data(session_data, merge=False)
        except UnsupportedSessionFormatError as exc:
            print(f"{Fore.RED}{exc}")
            return False
        print(
            f"{Fore.GREEN}RESUME: Loaded session from {session_path} with {restored_count} URLs"
        )

        worker_dirs = session_data.get("worker_directories")
        if isinstance(worker_dirs, dict) and worker_dirs:
            print(
                f"Restored {len(worker_dirs)} worker directory settings"
            )

        if "total_urls_added" in session_data:
            total = session_data["total_urls_added"]
            completed = session_data.get("completed_count", 0)
            print(f"Original session: {total} total URLs, {completed} completed")

        self.do_start("")
        return False

    def do_retry(self, arg: str) -> bool:
        """Retry all failed downloads with their recorded directories."""
        if self.dlq:
            added = self.dlq.retry_failed()
            if added:
                print(f"{Fore.GREEN}RETRY: Re-queued {added} failed downloads")
                if self.dlq.is_paused():
                    print(
                        f"{Fore.YELLOW}Workers are paused, use 'resume' to continue processing"
                    )
            else:
                print(f"{Fore.CYAN}No failed downloads awaiting retry")
            return False

        if not self.failed_offline:
            print(f"{Fore.CYAN}No failed downloads recorded")
            return False

        existing_pending = {entry.url for entry in self.pending}
        added = 0
        skipped = 0
        for entry in list(self.failed_offline.values()):
            if entry.url in existing_pending:
                skipped += 1
                continue
            self.pending.append(PendingEntry(entry.url, entry.directory))
            self.all_urls.add(entry.url)
            self._remember_directory_for_url(entry.url, entry.directory)
            added += 1

        self.failed_offline.clear()

        if added:
            print(f"{Fore.GREEN}RETRY: Added {added} failed downloads back to pending")
            if skipped:
                print(f"{Fore.YELLOW}Skipped {skipped} already-pending URLs")
            print(
                f"{Fore.CYAN}Use 'start' to process the retried downloads{Style.RESET_ALL}"
            )
        else:
            print(f"{Fore.CYAN}No failed downloads were ready for retry")

        return False

    def do_retryall(self, arg: str) -> bool:
        """Re-queue all completed (finished) URLs to the back of the list."""

        completed_set = self.dlq.completed_urls if self.dlq else self.completed_offline
        if not completed_set:
            print(f"{Fore.CYAN}No completed downloads to retry")
            return False

        pending_set = {entry.url for entry in self.pending}
        queued_set = self.dlq.queued_urls if self.dlq else set()
        active_urls: Set[str] = set()
        if self.dlq:
            try:
                active_urls = {u for (u, _d) in self.dlq.get_active_downloads().values()}
            except Exception:
                active_urls = set()

        # Stable-ish ordering: use history order first (oldest -> newest), then remaining sorted.
        history = self.stats.history_snapshot()
        ordered: List[str] = []
        seen: Set[str] = set()
        for entry in history:
            if not entry.get("success"):
                continue
            url = entry.get("url")
            if not url or url in seen or url not in completed_set:
                continue
            ordered.append(url)
            seen.add(url)

        remaining = sorted([u for u in completed_set if u not in seen])
        ordered.extend(remaining)

        added = 0
        skipped = 0
        for url in ordered:
            if url in pending_set or url in queued_set or url in active_urls:
                skipped += 1
                continue

            directory_override = self.url_directory_overrides.get(url)
            directory_path: Optional[Path] = None
            if directory_override:
                directory_path = self._expanduser_mkdir(
                    directory_override,
                    fail_prefix="Note: Could not prepare directory for retryall",
                )

            self.all_urls.add(url)

            if self.dlq:
                if self.dlq.add_force(url, directory_path):
                    added += 1
                else:
                    skipped += 1
            else:
                self.pending.append(PendingEntry(url, directory_path))
                self._remember_directory_for_url(url, directory_path)
                self.completed_offline.discard(url)
                added += 1

        if added:
            print(f"{Fore.GREEN}RETRYALL: Re-queued {added} completed downloads")
            if skipped:
                print(f"{Fore.YELLOW}Skipped {skipped} already pending/active/queued URLs")
            if self.dlq and self.dlq.is_paused():
                print(f"{Fore.YELLOW}Workers are paused, use 'resume' to continue processing")
            if not self.dlq:
                print(f"{Fore.CYAN}Use 'start' to process the retried downloads{Style.RESET_ALL}")
                self._signal_auto_start_ready()
        else:
            print(f"{Fore.CYAN}No completed downloads were ready for retry")

        return False

    def do_status(self, arg: str) -> bool:
        """Show detailed status information."""
        print(f"\n{Fore.CYAN}=== Status Report ==={Style.RESET_ALL}")
        print(f"Workers: {self.config.workers}")
        print(f"Pending URLs: {len(self.pending)}")
        print(f"Total URLs tracked: {len(self.all_urls)}")
        failed_count = self.dlq.failed_count() if self.dlq else len(self.failed_offline)
        print(f"Failed downloads awaiting retry: {failed_count}")

        # Clipboard automation status
        clipboard_state = "ENABLED" if self.clipboard_enabled else "DISABLED"
        armed_state = "ARMED" if self.clipboard_armed else "READY"
        whitelist = self.clipboard_whitelist_host or "(none)"
        seen = len(self._clipboard_seen_urls)
        print(
            f"Clipboard: {clipboard_state} ({armed_state}), whitelist: {whitelist}, seen: {seen}"
        )

        if self.dlq:
            active = self.dlq.get_active_downloads()
            queue_size = self.dlq.get_queue_size()
            completed_count = len(self.dlq.completed_urls)

            print(f"Queue size: {queue_size}")
            print(f"Active downloads: {len(active)}")
            print(f"Completed downloads: {completed_count}")
            print(f"Status: {'PAUSED' if self.dlq.is_paused() else 'RUNNING'}")

            # Calculate URLs that would be saved
            pending_url_set = {entry.url for entry in self.pending}
            urls_to_save_count = len(pending_url_set)
            for url in self.all_urls:
                if url not in self.dlq.completed_urls and url not in pending_url_set:
                    urls_to_save_count += 1
            print(f"URLs that would be saved: {urls_to_save_count}")

            if active:
                print(f"\n{Fore.YELLOW}Active Downloads:{Style.RESET_ALL}")
                for worker_id, (url, active_dir) in active.items():
                    dir_label = (
                        str(active_dir)
                        if active_dir is not None
                        else self._format_worker_dir(worker_id)
                    )
                    print(f"  Worker {worker_id}: {url}")
                    print(f"    → Directory: {dir_label}")
                    transfer = self.dlq.get_worker_transfer_snapshot(worker_id)
                    if transfer:
                        downloaded, rate, elapsed = transfer
                        print(
                            f"    → Approx. Net: {_format_bytes(rate)}/s, {_format_bytes(downloaded)} in {elapsed:.0f}s"
                        )

            if self._has_worker_dir_overrides():
                self._print_worker_dirs()
        else:
            print("Workers: Not started")
            print(f"Completed downloads: {len(self.completed_offline)}")
            print(f"URLs that would be saved: {len(self.pending)}")
            if self._has_worker_dir_overrides():
                self._print_worker_dirs(label="Planned Worker Directories")

        session_label = self._format_directory_label(self.session_url_directory)
        print(f"Session default directory: {session_label}")

        print(f"\n{self.stats.get_summary()}")
        return False

    def do_worker(self, arg: str) -> bool:
        """Follow live stdout/stderr for a worker. Usage: worker <id> [tail_lines]"""
        if not self.dlq:
            print(f"{Fore.RED}No workers running")
            return False

        args = arg.split()
        if not args:
            print("Usage: worker <id> [tail_lines]")
            return False

        first_arg = args[0].lower()
        if first_arg in {"all", "*"}:
            try:
                tail_lines = int(args[1]) if len(args) > 1 else 25
            except ValueError:
                tail_lines = 25
            return self._follow_all_workers(tail_lines)

        try:
            worker_id = int(args[0])
        except ValueError:
            print(f"{Fore.RED}Worker id must be an integer")
            return False

        if worker_id < 0 or worker_id >= self.dlq.next_worker_id:
            print(f"{Fore.RED}No worker with id {worker_id}")
            return False

        try:
            tail_lines = int(args[1]) if len(args) > 1 else 25
        except ValueError:
            tail_lines = 25

        tail_lines = max(0, tail_lines)

        lines, index = self.dlq.get_worker_log_tail(worker_id, tail_lines)
        if lines:
            print(
                f"\n{Fore.CYAN}=== Worker {worker_id} log (last {len(lines)} lines) ==={Style.RESET_ALL}"
            )
            for line in lines:
                color = Fore.WHITE
                prefix = ""
                if line.startswith("[STDERR]"):
                    color = Fore.RED
                    prefix = line[len("[STDERR]") :].lstrip()
                elif line.startswith("[STDOUT]"):
                    color = Fore.WHITE
                    prefix = line[len("[STDOUT]") :].lstrip()
                else:
                    prefix = line
                print(f"{color}Worker {worker_id}{Style.RESET_ALL} {prefix}")
        else:
            print(f"{Fore.YELLOW}No log output from worker {worker_id} yet")

        event = self.dlq.get_worker_event(worker_id)
        event.clear()
        print(
            f"\n{Fore.CYAN}Live feed – press Ctrl+C to return to the main prompt (worker keeps running).{Style.RESET_ALL}"
        )

        index_tracker = index
        last_idle_notice = 0.0
        idle_frames = ["   ", ".  ", ".. ", "..."]
        idle_index = 0
        status_line_active = False
        last_status_text = ""
        last_progress_value: Optional[str] = None
        last_status_len = 0

        def _clear_status_line() -> None:
            nonlocal status_line_active, last_status_text, last_status_len
            if status_line_active:
                sys.stdout.write("\r" + " " * last_status_len + "\r")
                sys.stdout.flush()
                status_line_active = False
                last_status_text = ""
                last_status_len = 0

        def _show_status_line(text: str) -> None:
            nonlocal status_line_active, last_status_text, last_status_len
            if text == last_status_text and status_line_active:
                return
            sys.stdout.write("\r" + text)
            visible_len = len(text)
            if last_status_len > visible_len:
                sys.stdout.write(" " * (last_status_len - visible_len))
            sys.stdout.flush()
            status_line_active = True
            last_status_text = text
            last_status_len = visible_len

        try:
            while True:
                new_lines, next_index, truncated = self.dlq.get_worker_log_since(
                    worker_id, index_tracker
                )
                if truncated:
                    _clear_status_line()
                    print(
                        f"{Fore.YELLOW}... worker {worker_id} log truncated, showing latest entries"
                    )
                if new_lines:
                    _clear_status_line()
                    for line in new_lines:
                        color = Fore.WHITE
                        message = line
                        if line.startswith("[STDERR]"):
                            color = Fore.RED
                            message = line[len("[STDERR]") :].lstrip()
                        elif line.startswith("[STDOUT]"):
                            message = line[len("[STDOUT]") :].lstrip()
                        print(f"{color}Worker {worker_id}{Style.RESET_ALL} {message}")
                    index_tracker = next_index
                    last_idle_notice = time.time()
                else:
                    index_tracker = next_index

                progress_text = self.dlq.get_worker_progress(worker_id)
                worker_active = self.dlq.is_worker_active(worker_id)

                if progress_text and worker_active:
                    if progress_text != last_progress_value:
                        pretty = f"{Fore.YELLOW}Worker {worker_id} progress:{Style.RESET_ALL} {progress_text}"
                        _show_status_line(pretty)
                        last_progress_value = progress_text
                    last_idle_notice = time.time()
                else:
                    if last_progress_value is not None:
                        _clear_status_line()
                        last_progress_value = None

                    if not worker_active:
                        now = time.time()
                        if now - last_idle_notice > 1:
                            frame = idle_frames[idle_index % len(idle_frames)]
                            status = f"{Fore.YELLOW}Worker {worker_id} idle {frame}{Style.RESET_ALL}"
                            _show_status_line(status)
                            idle_index += 1
                            last_idle_notice = now
                    else:
                        _clear_status_line()
                        last_idle_notice = time.time()

                event.wait(timeout=1)
                event.clear()
        except KeyboardInterrupt:
            _clear_status_line()
            print(f"{Fore.GREEN}Stopped following worker {worker_id}{Style.RESET_ALL}")

        return False

    def _follow_all_workers(self, tail_lines: int) -> bool:
        if not self.dlq:
            print(f"{Fore.RED}No workers running")
            return False

        worker_ids = [wid for wid in range(self.dlq.next_worker_id)]
        if not worker_ids:
            print(f"{Fore.YELLOW}No workers available yet")
            return False

        tail_lines = max(0, tail_lines)
        print(
            f"\n{Fore.CYAN}=== Combined worker tail (last {tail_lines} lines per worker) ==={Style.RESET_ALL}"
        )
        indices: Dict[int, int] = {}
        for wid in worker_ids:
            lines, index = self.dlq.get_worker_log_tail(wid, tail_lines)
            indices[wid] = index
            if lines:
                print(f"{Fore.WHITE}--- Worker {wid} tail ---{Style.RESET_ALL}")
                for line in lines:
                    color = Fore.WHITE
                    message = line
                    if line.startswith("[STDERR]"):
                        color = Fore.RED
                        message = line[len("[STDERR]") :].lstrip()
                    elif line.startswith("[STDOUT]"):
                        message = line[len("[STDOUT]") :].lstrip()
                    print(f"{color}Worker {wid}{Style.RESET_ALL} {message}")
        print(
            f"\n{Fore.CYAN}Live combined feed – press Ctrl+C to return to the main prompt (workers keep running).{Style.RESET_ALL}"
        )

        last_progress: Dict[int, Optional[str]] = {wid: None for wid in worker_ids}

        try:
            while True:
                any_activity = False
                for wid in worker_ids:
                    new_lines, next_index, truncated = self.dlq.get_worker_log_since(
                        wid, indices[wid]
                    )
                    if truncated:
                        print(
                            f"{Fore.YELLOW}... worker {wid} log truncated, showing latest entries"
                        )
                    if new_lines:
                        any_activity = True
                        for line in new_lines:
                            color = Fore.WHITE
                            message = line
                            if line.startswith("[STDERR]"):
                                color = Fore.RED
                                message = line[len("[STDERR]") :].lstrip()
                            elif line.startswith("[STDOUT]"):
                                message = line[len("[STDOUT]") :].lstrip()
                            print(f"{color}Worker {wid}{Style.RESET_ALL} {message}")
                    indices[wid] = next_index

                    progress_text = self.dlq.get_worker_progress(wid)
                    worker_active = self.dlq.is_worker_active(wid)
                    if progress_text and worker_active:
                        if progress_text != last_progress[wid]:
                            any_activity = True
                            print(
                                f"{Fore.YELLOW}Worker {wid} progress:{Style.RESET_ALL} {progress_text}"
                            )
                            last_progress[wid] = progress_text
                    else:
                        last_progress[wid] = None

                if not any_activity:
                    time.sleep(0.5)

        except KeyboardInterrupt:
            print(f"{Fore.GREEN}Stopped following all workers{Style.RESET_ALL}")

        return False

    def do_workerdir(self, arg: str) -> bool:
        """Manage worker download directories or set the session default directory.

        Usage examples:
          workerdir                       # list worker directories and session default
          workerdir <id>                  # show directory for worker id
          workerdir <id> <path>           # set worker directory (use 'default' to clear)
          workerdir <path>                # set session default directory for new URLs
          workerdir reset|default         # clear session default directory
        """

        tokens = shlex.split(arg, posix=False)

        def _normalize_path_input(value: str) -> str:
            text = value.strip()
            if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
                text = text[1:-1]
            return text

        if not tokens:
            # label = "Worker Directories" if self.dlq else "Planned Worker Directories"
            # self._print_worker_dirs(label=label)
            session_label = self._format_directory_label(self.session_url_directory)
            print(f"Session default directory: {session_label}")
            return False

        first_token = tokens[0]

        # Try worker-id mode first
        worker_id: Optional[int]
        try:
            worker_id = int(first_token)
        except ValueError:
            worker_id = None

        if worker_id is None:
            # Treat as session-level directory override
            directive = first_token.lower()
            if directive in {"default", "none", "clear", "reset"}:
                self.session_url_directory = None
                print(
                    f"{Fore.GREEN}Session default directory cleared – using global/default location"
                )
                return False

            path_input = _normalize_path_input(" ".join(tokens))
            try:
                path_obj = Path(path_input).expanduser()
                path_obj.mkdir(parents=True, exist_ok=True)
                self.session_url_directory = path_obj
                free_info = self._format_free_space_label(path_obj)
                info_text = f" ({free_info})" if free_info else ""
                print(
                    f"{Fore.GREEN}Session default directory set to {path_obj}{info_text}"
                )
            except Exception as exc:
                print(f"{Fore.RED}Failed to set session default directory: {exc}")
            return False

        if worker_id < 0:
            print(f"{Fore.RED}Worker id must be non-negative")
            return False

        if len(tokens) == 1:
            print(f"Worker {worker_id}: {self._format_worker_dir(worker_id)}")
            return False

        path_token_raw = _normalize_path_input(" ".join(tokens[1:]))
        directory_input: Optional[str]
        if path_token_raw.lower() in {"default", "none", "clear"}:
            directory_input = None
        else:
            directory_input = path_token_raw or None

        try:
            if self.dlq:
                if worker_id >= self.dlq.next_worker_id:
                    print(
                        f"{Fore.RED}Worker {worker_id} does not exist (currently {self.dlq.next_worker_id} workers)"
                    )
                    return False
                new_path = self.dlq.set_worker_download_dir(worker_id, directory_input)
            else:
                new_path = (
                    self._expanduser_mkdir(
                        directory_input,
                        fail_prefix="Failed to set directory",
                        color=Fore.RED,
                    )
                    if directory_input
                    else None
                )
                self.worker_dir_overrides[worker_id] = new_path

            if directory_input is None:
                msg = "(use global/default)"
            else:
                msg = str(new_path) if new_path else directory_input
                if new_path:
                    free_info = self._format_free_space_label(new_path)
                    if free_info:
                        msg = f"{msg} ({free_info})"

            print(f"{Fore.GREEN}Worker {worker_id} directory set to: {msg}")
        except Exception as e:
            print(f"{Fore.RED}Failed to set directory: {e}")

        return False

    def do_history(self, arg: str) -> bool:
        """Show download history."""
        history = self.stats.history_snapshot()
        if not history:
            print("No download history available")
            return False

        # Parse argument for limit
        try:
            limit = int(arg) if arg.strip() else 10
        except ValueError:
            limit = 10

        print(f"\n{Fore.CYAN}=== Last {limit} Downloads ==={Style.RESET_ALL}")
        for entry in history[-limit:]:
            status = f"{Fore.GREEN}SUCCESS" if entry["success"] else f"{Fore.RED}FAILED"
            timestamp = datetime.fromisoformat(entry["timestamp"]).strftime("%H:%M:%S")
            print(f"{status} [{timestamp}] {entry['url']} ({entry['duration']:.1f}s)")

        return False

    def do_config(self, arg: str) -> bool:
        """View or modify configuration. Usage: config [key [value]]"""
        args = arg.split()

        if not args:
            # Show all config
            print(f"\n{Fore.CYAN}=== Configuration ==={Style.RESET_ALL}")
            for key, value in self.config.to_dict().items():
                print(f"{key}: {value}")
        elif len(args) == 1:
            # Show specific config
            key = args[0]
            if hasattr(self.config, key):
                print(f"{key}: {getattr(self.config, key)}")
            else:
                print(f"{Fore.RED}Unknown config key: {key}")
        else:
            # Set config
            key, value = args[0], " ".join(args[1:])
            if hasattr(self.config, key):
                # Type conversion
                current = getattr(self.config, key)
                try:
                    if isinstance(current, int):
                        value = int(value)
                    elif isinstance(current, bool):
                        value = value.lower() in ("true", "1", "yes")
                    elif isinstance(current, list):
                        value = value.split(",") if value else []

                    if key == "directory_template" and isinstance(value, str):
                        normalized = value.strip()
                        if normalized.lower() in {
                            "",
                            "none",
                            "null",
                            "default",
                        }:
                            value = None
                        else:
                            value = normalized

                    setattr(self.config, key, value)
                    print(f"{Fore.GREEN}Set {key} = {value}")
                    self._save_config()

                    # Special handling for workers
                    if key == "workers" and self.dlq:
                        try:
                            new_workers = value
                            if not isinstance(new_workers, int):
                                raise ValueError("workers must be an integer")
                            old_count, new_count = self.dlq.resize_workers(new_workers)
                            if new_count < old_count:
                                print(
                                    f"{Fore.YELLOW}WORKERS: Downsizing {old_count} -> {new_count}. Extra workers will finish their current download and then stop picking up new jobs."
                                )
                            elif new_count > old_count:
                                print(
                                    f"{Fore.YELLOW}WORKERS: Upsizing {old_count} -> {new_count}. New workers started immediately."
                                )
                        except Exception as exc:
                            print(f"{Fore.RED}Failed to resize workers: {exc}")
                    elif key == "auto_status" and self.config.auto_status and self.dlq:
                        self._start_status_updates()
                    elif (
                        key == "flat_directories"
                        and self.config.flat_directories
                        and self.config.directory_template
                    ):
                        print(
                            f"{Fore.YELLOW}Note: flat_directories overrides directory_template for downloads"
                        )
                    elif key == "keep_failed" and not self.config.keep_failed:
                        if self.dlq:
                            self.dlq.import_failed_entries([], replace=True)
                        self.failed_offline.clear()

                except ValueError as e:
                    print(f"{Fore.RED}Invalid value for {key}: {e}")
            else:
                print(f"{Fore.RED}Unknown config key: {key}")

        return False

    def do_save(self, arg: str) -> bool:
        """Save current session to file."""
        resolved = self._resolve_session_filename(
            arg,
            must_exist=False,
            allowed_suffixes={".json", ".conf"},
        )
        filename = resolved or (arg.strip() or self.config.session_file)

        # Collect all URLs that need to be saved (not yet completed)
        urls_to_save: List[str] = []
        pending_entries: List[Tuple[str, Optional[str]]] = []

        # Add pending URLs (retain their per-URL directory overrides)
        for entry in self.pending:
            urls_to_save.append(entry.url)
            pending_entries.append(
                (entry.url, str(entry.directory) if entry.directory else None)
            )

        # Add URLs that are queued but not completed
        if self.dlq:
            completed_urls = self.dlq.completed_urls
            pending_urls_set = {entry.url for entry in self.pending}
            for url in sorted(self.all_urls):
                if url not in completed_urls and url not in pending_urls_set:
                    urls_to_save.append(url)
                    directory_override = self.url_directory_overrides.get(url)
                    pending_entries.append((url, directory_override))

        completed_urls_sorted = (
            sorted(self.dlq.completed_urls) if self.dlq else sorted(self.completed_offline)
        )

        if self.dlq:
            failed_entries = self.dlq.get_failed_downloads()
        else:
            failed_entries = list(self.failed_offline.values())

        # Build human-readable session format (v4): group URLs by directory.
        def _group_id_map(items: List[Tuple[str, Optional[str]]]) -> Dict[str, Dict[str, str]]:
            grouped: Dict[str, List[str]] = {}
            for url, directory_value in items:
                key = directory_value or ""
                grouped.setdefault(key, []).append(url)
            result: Dict[str, Dict[str, str]] = {}
            for path_key, urls in grouped.items():
                urls_sorted = sorted({u for u in urls if u})
                id_map: Dict[str, str] = {}
                for i, url in enumerate(urls_sorted, start=1):
                    id_map[str(i)] = url
                result[path_key] = id_map
            return result

        pending_by_path = _group_id_map(pending_entries)

        completed_entries: List[Tuple[str, Optional[str]]] = []
        for url in completed_urls_sorted:
            completed_entries.append((url, self.url_directory_overrides.get(url)))
        completed_by_path = _group_id_map(completed_entries)

        failed_pairs: List[Tuple[str, Optional[str]]] = [
            (
                entry.url,
                (str(entry.directory) if entry.directory else None),
            )
            for entry in failed_entries
        ]
        failed_by_path = _group_id_map(failed_pairs) if failed_pairs else {}

        broken_payload: Dict[str, str] = {}
        if self.dlq:
            try:
                with self.dlq.broken_lock:
                    broken_payload = dict(self.dlq.broken_details)
            except Exception:
                broken_payload = {}
        else:
            broken_payload = dict(self.broken_offline_details)

        worker_dirs_payload: Dict[str, Optional[str]] = {}
        if self.dlq:
            try:
                upper = int(getattr(self.dlq, "next_worker_id", 0) or 0)
            except Exception:
                upper = 0
            for worker_id in range(max(0, upper)):
                directory = self.dlq.get_worker_download_dir(worker_id)
                if directory:
                    worker_dirs_payload[str(worker_id)] = str(directory)
        else:
            for worker_id, path in sorted(self.worker_dir_overrides.items()):
                if path:
                    worker_dirs_payload[str(worker_id)] = str(path)

        session_data: Dict[str, object] = {
            "format": 3,
            "pending_by_path": pending_by_path,
            "completed_by_path": completed_by_path,
            "config": self.config.to_dict(),
            "timestamp": datetime.now().isoformat(),
            "total_urls_added": len(self.all_urls),
            "completed_count": len(self.dlq.completed_urls)
            if self.dlq
            else len(self.completed_offline),
            "session_url_directory": str(self.session_url_directory)
            if self.session_url_directory
            else None,
        }

        if failed_by_path:
            session_data["failed_by_path"] = failed_by_path

        # Keep a readable explicit override map for retryall and future-proofing.
        if self.url_directory_overrides:
            session_data["url_directory_overrides"] = dict(self.url_directory_overrides)

        if broken_payload:
            session_data["broken"] = dict(broken_payload)

        if worker_dirs_payload:
            session_data["worker_directories"] = worker_dirs_payload

        try:
            wrote = _write_json_atomic_if_changed(filename, session_data)
            if wrote:
                print(f"{Fore.GREEN}SAVED: Session saved to {filename}")
            else:
                print(f"{Fore.GREEN}SAVED: Session unchanged (skipped write): {filename}")
            print(f"Saved {len(urls_to_save)} uncompleted URLs")
            if failed_entries:
                print(f"Saved {len(failed_entries)} failed downloads for future retry")
        except Exception as e:
            print(f"{Fore.RED}Failed to save session: {e}")

        return False

    def do_load(self, arg: str) -> bool:
        """Load session from file."""
        resolved = self._resolve_session_filename(
            arg,
            must_exist=True,
            allowed_suffixes={".json", ".conf"},
        )
        filename = resolved or (arg.strip() or self.config.session_file)

        try:
            with open(filename) as f:
                session_data = json.load(f)

        except FileNotFoundError:
            print(f"{Fore.RED}Session file not found: {filename}")
            return False
        except Exception as e:
            print(f"{Fore.RED}Failed to load session: {e}")
            return False

        try:
            restored_count = self._apply_session_data(session_data, merge=True)
        except UnsupportedSessionFormatError as exc:
            print(f"{Fore.RED}{exc}")
            return False

        print(f"{Fore.GREEN}LOADED: Session loaded from {filename}")
        print(f"Added {restored_count} URLs to pending")

        if "total_urls_added" in session_data:
            total = session_data["total_urls_added"]
            completed = session_data.get("completed_count", 0)
            print(f"Original session: {total} total URLs, {completed} completed")

        return False

    def do_pending(self, arg: str) -> bool:
        """Show pending URLs (before workers start)."""
        if not self.pending:
            print("No pending URLs")
        else:
            print(
                f"\n{Fore.CYAN}=== Pending URLs ({len(self.pending)}) ==={Style.RESET_ALL}"
            )
            for i, entry in enumerate(self.pending):
                dir_msg = f" [dir: {entry.directory}]" if entry.directory else ""
                print(f"{i:2d}: {entry.url}{dir_msg}")
        return False

    def do_urls(self, arg: str) -> bool:
        """Show all tracked URLs and their status."""
        if not self.all_urls:
            print("No URLs tracked")
            return False

        print(
            f"\n{Fore.CYAN}=== All Tracked URLs ({len(self.all_urls)}) ==={Style.RESET_ALL}"
        )

        pending_set = {entry.url for entry in self.pending}
        completed_set = self.dlq.completed_urls if self.dlq else self.completed_offline
        queued_set = self.dlq.queued_urls if self.dlq else set()

        for i, url in enumerate(sorted(self.all_urls)):
            if url in completed_set:
                status = f"{Fore.GREEN}COMPLETED"
            elif url in pending_set:
                status = f"{Fore.YELLOW}PENDING"
            elif url in queued_set:
                status = f"{Fore.BLUE}QUEUED"
            else:
                status = f"{Fore.RED}UNKNOWN"

            print(f"{i:2d}: {status}{Style.RESET_ALL} {url}")

        # Show what would be saved
        urls_to_save = [entry.url for entry in self.pending]
        if self.dlq:
            for url in self.all_urls:
                if url not in completed_set and url not in pending_set:
                    urls_to_save.append(url)

        print(
            f"\n{Fore.CYAN}URLs that would be saved: {len(urls_to_save)}{Style.RESET_ALL}"
        )
        return False

    def do_remove(self, arg: str) -> bool:
        """Remove URL from pending list. Usage: remove <index|url>"""
        if self.dlq:
            print(f"{Fore.RED}Cannot remove - workers already running")
            return False

        arg = arg.strip()
        if not arg:
            print("Usage: remove <index|url>")
            return False

        removed: Optional[PendingEntry] = None
        if arg.isdigit():
            idx = int(arg)
            if 0 <= idx < len(self.pending):
                removed = self.pending.pop(idx)
        else:
            for entry in self.pending:
                if entry.url == arg:
                    self.pending.remove(entry)
                    removed = entry
                    break

        if removed:
            # Also remove from all_urls tracking if it was only pending
            self.all_urls.discard(removed.url)
            self._forget_directory_for_url(removed.url)
            dir_msg = f" (dir: {removed.directory})" if removed.directory else ""
            print(f"{Fore.GREEN}REMOVED: Removed: {removed.url}{dir_msg}")
        else:
            print(f"{Fore.RED}Nothing removed")

        return False

    def do_clearqueue(self, arg: str) -> bool:
        """Clear pending list."""
        if self.dlq:
            print(f"{Fore.RED}Cannot clear - workers already running")
            return False

        count = len(self.pending)

        # Remove pending URLs from all_urls tracking
        for entry in self.pending:
            self.all_urls.discard(entry.url)
            self._forget_directory_for_url(entry.url)

        self.pending.clear()
        print(f"{Fore.GREEN}CLEARED: Cleared {count} pending URLs")
        return False

    def do_cls(self, arg: str) -> bool:
        """Clear the terminal output and reprint the header."""
        command = "cls" if os.name == "nt" else "clear"
        try:
            os.system(command)
        except Exception:
            print("\n" * 50)
        print(self.intro)
        return False

    def do_quit(self, arg: str) -> bool:
        """Quit the application."""
        print("SHUTTING DOWN...")

        self._stop_clipboard_monitor()

        if self.config.auto_save and (self.pending or self.dlq):
            self.do_save("")

        if self.dlq:
            print("WAITING: Waiting for active downloads to complete...")
            self.dlq.shutdown()

        self._stop_async_printer()
        print(f"{Fore.GREEN}COMPLETE: All downloads complete – goodbye!")
        return True

    def do_EOF(self, arg: str) -> bool:
        """Handle Ctrl+D."""
        print()
        return self.do_quit(arg)

    # Help system ----------------------------------------------------------
    def do_help(self, arg: str) -> bool:
        if not arg:
            print(f"""
{Fore.CYAN}gallery-dl Queue - Command Sheet:{Style.RESET_ALL}

{Fore.YELLOW}Basic Operations:{Style.RESET_ALL}
    <url>                  Add URL to queue
    clipboard              Arm/disable clipboard auto-ingest (aliases: clip, cb)
    start                  Start/resume workers (aliases: run, go, begin)
    pause                  Pause all workers (aliases: hold, stop)
    resume                 Resume workers or reload last session (aliases: continue, unpause)
    cls                    Clear the terminal display (aliases: clear)
    quit                   Shutdown and exit (aliases: exit, q, x)

{Fore.YELLOW}Queue Management:{Style.RESET_ALL}
    pending                Show pending URLs (aliases: queue, pend, list)
    urls                   Show all tracked URLs and their status (aliases: links, all)
    remove <index|url>     Remove from pending (aliases: rm, del, delete)
    clearqueue             Clear pending list (aliases: clq, clear-queue, flush, reset)
    retry                  Re-queue all failed downloads (aliases: redo, rerun)
    retryall               Re-queue all completed downloads back to the end
  
{Fore.YELLOW}Monitoring:{Style.RESET_ALL}
    status                 Show detailed status (aliases: stat, info)
    history [n]            Show last n downloads (default: 10) (aliases: hist, recent)
    worker <id> [lines]    Follow live stdout/stderr for a worker (default tail: 25) (aliases: log, tail, feed)
    workerdir [...]        Set session default directory (recommended) or optional per-worker dirs (aliases: wd, dir)
  
{Fore.YELLOW}Configuration:{Style.RESET_ALL}
    config                 Show all settings (aliases: cfg, conf, settings)
    config <key>           Show specific setting
    config <key> <value>   Set configuration value (e.g. config auto_status true)
                           Useful keys: keep_failed, flat_directories, directory_template, dump_json
  
{Fore.YELLOW}Session Management:{Style.RESET_ALL}
    save [file]            Save current session (aliases: sv, write, export)
    load [file]            Load session from file (aliases: ld, restore, open)

{Fore.YELLOW}Notes:{Style.RESET_ALL}
    - Recommended directory workflow: use 'workerdir <path>' to set a session default.
    - Per-worker directories are optional (useful e.g. to split across drives).
    - Session files are backward compatible (v1/v2 load-only); current saves use format v3.

Type {Fore.YELLOW}help <command>{Style.RESET_ALL} for detailed help on specific commands.
""")
        else:
            mapped = self.command_aliases.get(arg.strip().lower())
            if mapped:
                arg = mapped
            super().do_help(arg)
        return False


if __name__ == "__main__":
    if any(arg in {"--help", "-h"} for arg in sys.argv[1:]):
        print(
                        """gallery-dl Queue (interactive shell)

Start:
    python gallery-dl.queue.py

Description:
    Interactive command-line shell around gallery-dl with a queue, retries,
    live worker logs, and session persistence.

Quickstart:
    1) Optional: set a session default directory for new URLs:
             workerdir <path>
    2) Paste/type URL(s) (just enter the URL)
    3) Start downloads:
             start

Key commands:
    status
    worker <id> | worker all   (Ctrl+C returns to the prompt)
    pause | resume
    retry | retryall
    save [file] | load [file]
    quit

Inside the shell:
    - '?' shows the full help including aliases.
    - 'help <command>' shows details for one command.
"""
        )
        sys.exit(0)

    app = InteractiveQueue()
    try:
        app.cmdloop()
    except (KeyboardInterrupt, BrokenPipeError):
        print(
            f"\n{Fore.YELLOW}Interrupted - stopping workers and exiting...{Style.RESET_ALL}"
        )
        try:
            app._stop_clipboard_monitor()
        except Exception:
            pass

        try:
            if app.dlq:
                print(
                    f"{Fore.YELLOW}INTERRUPT: Terminating gallery-dl processes...{Style.RESET_ALL}"
                )
                app.dlq.interrupt_now()
        except Exception as exc:
            print(
                f"{Fore.YELLOW}Note: Could not terminate all processes: {exc}{Style.RESET_ALL}"
            )

        try:
            app._stop_async_printer()
        except Exception:
            pass
        sys.exit(0)
