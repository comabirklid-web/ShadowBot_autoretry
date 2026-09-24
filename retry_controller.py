from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Iterable
import urllib.request


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "retry_config.json"
STATE_PATH = BASE_DIR / "retry_state.json"
SECRET_PATH = BASE_DIR / "wecom_webhook.dpapi"
LOG_DIR = BASE_DIR / "logs"
UUID_SQL_COLUMNS = "uuid, status, jobId, jobName, appId, appName, createTime, error, description, sourceKind"
SUCCESS_STATUS = 2
FAILURE_STATUSES = {3}
CANCELLED_STATUS = 4
DEFAULT_ACTIVE_STATUSES = (1,)
EDITING_ERROR_MARKERS = ("正在编辑", "应用正在编辑", "编辑应用")


class RecentDaysFileHandler(logging.Handler):
    """Append local logs and retain a strict sliding time window."""
    _timestamp_format = "%Y-%m-%d %H:%M:%S,%f"

    def __init__(self, path: Path, retention_days: float = 3, encoding: str = "utf-8"):
        super().__init__()
        self.path = path
        self.retention = timedelta(days=max(1 / 24, float(retention_days)))
        self.encoding = encoding
        self.terminator = "\n"

    def _in_window(self, line: str, cutoff: datetime) -> bool:
        try:
            return datetime.strptime(line[:23], self._timestamp_format) >= cutoff
        except ValueError:
            # A malformed line must not prevent valid, newer log records from
            # being retained.  It is safe to discard because every record this
            # handler writes begins with the configured logging timestamp.
            return False

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record) + self.terminator
            with self.lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding=self.encoding, newline="") as stream:
                    stream.write(message)
                lines = self.path.read_text(encoding=self.encoding).splitlines(keepends=True)
                cutoff = datetime.now() - self.retention
                retained = [line for line in lines if self._in_window(line, cutoff)]
                if len(retained) != len(lines):
                    temporary = self.path.with_suffix(self.path.suffix + ".tmp")
                    temporary.write_text("".join(retained), encoding=self.encoding, newline="")
                    os.replace(temporary, self.path)
        except Exception:
            self.handleError(record)


@dataclass(frozen=True)
class TaskRecord:
    uuid: str
    status: int
    job_id: str | None
    job_name: str | None
    app_id: str
    app_name: str
    create_time: str
    error: str | None
    description: str | None
    source_kind: int

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TaskRecord":
        return cls(
            uuid=row["uuid"], status=int(row["status"]), job_id=row["jobId"],
            job_name=row["jobName"], app_id=row["appId"], app_name=row["appName"],
            create_time=row["createTime"], error=row["error"], description=row["description"],
            source_kind=int(row["sourceKind"]),
        )


@dataclass(frozen=True)
class TaskLogObservation:
    """Filesystem evidence that ShadowBot created a task execution.

    Community Edition can keep the matching ``tasks.db3`` transaction invisible
    to other processes until a long-running task finishes.  The task log file is
    created at execution start, so it is the earliest durable evidence available
    without an Enterprise API.
    """

    task_id: str
    created_at: datetime
    modified_at: datetime
    size: int


@dataclass(frozen=True)
class CommunityClientState:
    """Read-only state reported by the Community Edition desktop client."""

    available: bool
    editing: bool
    pages: tuple[str, ...]
    error: str | None = None
    running: bool = False
    running_task_id: str | None = None
    running_app_name: str | None = None


def is_editing_failure(task: TaskRecord) -> bool:
    text = " ".join(part for part in (task.error, task.description) if part).lower()
    return any(marker in text for marker in EDITING_ERROR_MARKERS)


@dataclass(frozen=True)
class LaunchReceipt:
    """Evidence describing how a launch request was submitted.

    Desktop UI automation cannot authoritatively prove that a workflow started.
    An empty ``task_id`` deliberately keeps the attempt unverified until a new
    task log or database row appears.
    """

    task_id: str
    method: str


@dataclass(frozen=True)
class CommunityTaskStatus:
    """One task state read from the bundled Community Edition local CLI."""

    task_id: str
    status: int
    app_id: str
    app_name: str
    error: str | None = None


class TaskStore:
    def __init__(self, user_folder: Path):
        self.user_folder = user_folder
        self.database_path = user_folder / "tasks.db3"
        self.log_folder = user_folder / "task_logs"

    def _connect(self) -> Any:
        connection = sqlite3.connect(f"file:{self.database_path}?mode=ro", uri=True, timeout=10)
        connection.row_factory = sqlite3.Row
        class SqliteRetryWrapper:
            def __init__(self, conn):
                self._conn = conn
            def execute(self, sql, parameters=()):
                for attempt in range(5):
                    try:
                        return self._conn.execute(sql, parameters)
                    except sqlite3.OperationalError as e:
                        if "locked" in str(e).lower() and attempt < 4:
                            import time
                            time.sleep(0.5 * (2 ** attempt))
                            continue
                        raise
            def close(self):
                self._conn.close()
        return SqliteRetryWrapper(connection)

    def trigger_failures(
        self, max_age_seconds: int, excluded_ids: Iterable[str], source_kinds: Iterable[int]
    ) -> list[TaskRecord]:
        excluded = set(excluded_ids)
        kinds = tuple(sorted({int(kind) for kind in source_kinds}))
        if not kinds:
            return []
        placeholders = ", ".join("?" for _ in kinds)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {UUID_SQL_COLUMNS} FROM tasks "
                f"WHERE status = 3 AND sourceKind IN ({placeholders}) "
                "ORDER BY createTime DESC, rowid DESC",
                kinds,
            ).fetchall()
        cutoff = datetime.now() - timedelta(seconds=max_age_seconds)
        results: list[TaskRecord] = []
        for row in rows:
            task = TaskRecord.from_row(row)
            try:
                created = datetime.fromisoformat(task.create_time)
            except (TypeError, ValueError):
                continue
            if created < cutoff:
                break
            if task.uuid not in excluded:
                results.append(task)
        return results

    def latest_scheduled_failure(self, max_age_seconds: int, excluded_ids: Iterable[str]) -> TaskRecord | None:
        tasks = self.trigger_failures(max_age_seconds, excluded_ids, (0,))
        return tasks[0] if tasks else None

    def task_ids_for_app(self, app_id: str) -> set[str]:
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT uuid FROM tasks WHERE appId = ?", (app_id,)).fetchall()
        return {str(row["uuid"]) for row in rows}

    @staticmethod
    def _valid_task_id(value: str) -> bool:
        return bool(value) and all(char in "0123456789abcdef-" for char in value.lower())

    def task_log_observation(self, task_id: str) -> TaskLogObservation | None:
        if not self._valid_task_id(task_id):
            return None
        path = self.log_folder / f"{task_id}.tasklog"
        try:
            stat = path.stat()
        except (FileNotFoundError, PermissionError, OSError):
            return None
        return TaskLogObservation(
            task_id=task_id,
            created_at=datetime.fromtimestamp(stat.st_ctime),
            modified_at=datetime.fromtimestamp(stat.st_mtime),
            size=int(stat.st_size),
        )

    def find_new_task_logs(
        self, known_ids: set[str], not_before: datetime | None = None,
    ) -> list[TaskLogObservation]:
        """Return newly-created task logs, oldest first.

        On Windows ``st_ctime`` is the file creation time.  ``known_ids`` also
        protects the small launch matching grace window from treating a log that
        existed before the protocol request as new evidence.
        """
        if not self.log_folder.exists():
            return []
        observations: list[TaskLogObservation] = []
        try:
            for path in self.log_folder.iterdir():
                task_id = path.stem
                if (
                    path.suffix.lower() != ".tasklog"
                    or task_id in known_ids
                    or not self._valid_task_id(task_id)
                ):
                    continue
                try:
                    stat = path.stat()
                except (FileNotFoundError, PermissionError):
                    continue
                created_at = datetime.fromtimestamp(stat.st_ctime)
                if not_before is not None and created_at < not_before:
                    continue
                observations.append(TaskLogObservation(
                    task_id=task_id,
                    created_at=created_at,
                    modified_at=datetime.fromtimestamp(stat.st_mtime),
                    size=int(stat.st_size),
                ))
        except OSError:
            logging.exception("无法扫描影刀任务日志目录：%s", self.log_folder)
            return []
        observations.sort(key=lambda value: (value.created_at, value.task_id))
        return observations

    def uncommitted_task_logs(
        self, not_before: datetime, known_ids: set[str] | None = None,
    ) -> list[TaskLogObservation]:
        """Find recent task logs whose database rows are not visible yet."""
        observations = self.find_new_task_logs(known_ids or set(), not_before)
        if not observations:
            return []
        pending: list[TaskLogObservation] = []
        for observation in observations:
            if self.get_task(observation.task_id) is None:
                pending.append(observation)
        return pending

    def find_new_retry_task(
        self,
        app_id: str,
        known_ids: set[str],
        source_kinds: Iterable[int] = (5,),
        not_before: datetime | None = None,
    ) -> TaskRecord | None:
        """Find a retry run created by the controller after it launched the app.

        ``sourceKind=2`` is a desktop-initiated run.  It is eligible only when
        the active launch attempt was submitted through our desktop UI method;
        otherwise it remains a user's manual run.  Legacy sources are passed by
        the caller for old persisted attempts.  Keeping this choice outside the
        SQL query prevents an emergency manual run from being reported as an
        automatic retry success.
        """
        kinds = tuple(sorted({int(kind) for kind in source_kinds}))
        if not kinds:
            return None
        placeholders = ", ".join("?" for _ in kinds)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {UUID_SQL_COLUMNS} FROM tasks WHERE appId = ? AND sourceKind IN ({placeholders}) "
                "ORDER BY createTime DESC, rowid DESC",
                (app_id, *kinds),
            ).fetchall()
        for row in rows:
            task = TaskRecord.from_row(row)
            if task.uuid in known_ids:
                continue
            if not_before is not None:
                try:
                    created_at = datetime.fromisoformat(task.create_time)
                except (TypeError, ValueError):
                    logging.warning("重试任务记录时间无法解析，忽略：%s", task.uuid)
                    continue
                if created_at < not_before:
                    continue
            return task
        return None

    def get_task(self, task_id: str) -> TaskRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                f"SELECT {UUID_SQL_COLUMNS} FROM tasks WHERE uuid = ?", (task_id,)
            ).fetchone()
        return TaskRecord.from_row(row) if row else None

    def successful_tasks(self, source_kinds: Iterable[int], limit: int = 100) -> list[TaskRecord]:
        """Return the newest completed runs for monitor/audit logging."""
        kinds = tuple(sorted({int(kind) for kind in source_kinds}))
        if not kinds or limit <= 0:
            return []
        placeholders = ", ".join("?" for _ in kinds)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {UUID_SQL_COLUMNS} FROM tasks "
                f"WHERE status = ? AND sourceKind IN ({placeholders}) "
                "ORDER BY createTime DESC, rowid DESC LIMIT ?",
                (SUCCESS_STATUS, *kinds, int(limit)),
            ).fetchall()
        return [TaskRecord.from_row(row) for row in rows]

    def active_tasks(self, active_statuses: Iterable[int] = DEFAULT_ACTIVE_STATUSES) -> list[TaskRecord]:
        """Return tasks that ShadowBot reports as actually running.

        A previous implementation considered every non-terminal status active.
        That includes waiting/scheduled rows, so a future timer could hold the
        retry queue forever even while ShadowBot was idle.
        """
        statuses = tuple(sorted({int(status) for status in active_statuses}))
        if not statuses:
            return []
        placeholders = ", ".join("?" for _ in statuses)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {UUID_SQL_COLUMNS} FROM tasks "
                f"WHERE status IN ({placeholders}) ORDER BY createTime ASC, rowid ASC",
                statuses,
            ).fetchall()
        return [TaskRecord.from_row(row) for row in rows]

    def latest_task_activity_evidence(
        self, source_kinds: Iterable[int], limit: int = 200,
    ) -> tuple[datetime, TaskRecord] | None:
        """Return the newest observable creation/log-write time for normal work.

        A task row's ``createTime`` can be several minutes older than its actual
        completion.  The task log modification time provides the missing end-of-
        activity evidence needed for the post-task quiet window.
        """
        kinds = tuple(sorted({int(kind) for kind in source_kinds}))
        if not kinds or limit <= 0:
            return None
        placeholders = ", ".join("?" for _ in kinds)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT {UUID_SQL_COLUMNS} FROM tasks WHERE sourceKind IN ({placeholders}) "
                "ORDER BY createTime DESC, rowid DESC LIMIT ?",
                (*kinds, int(limit)),
            ).fetchall()
        newest: tuple[datetime, TaskRecord] | None = None
        for row in rows:
            task = TaskRecord.from_row(row)
            try:
                activity_at = datetime.fromisoformat(task.create_time)
            except (TypeError, ValueError):
                continue
            observation = self.task_log_observation(task.uuid)
            if observation and observation.modified_at > activity_at:
                activity_at = observation.modified_at
            if newest is None or activity_at > newest[0]:
                newest = (activity_at, task)
        return newest

    def read_log_lines(self, task_id: str, limit: int = 100) -> list[str]:
        if not self._valid_task_id(task_id):
            return []
        path = self.log_folder / f"{task_id}.tasklog"
        if not path.exists():
            return []
        try:
            file_size = path.stat().st_size
            max_read = 5 * 1024 * 1024  # Max 5MB against OOM
            with path.open("rb") as f:
                if file_size > max_read:
                    f.seek(file_size - max_read)
                decoded = f.read().decode("utf-8", errors="replace")
        except OSError:
            return []
        chunks: list[str] = []
        current: list[str] = []
        for char in decoded:
            if ord(char) < 32:
                text = "".join(current).strip()
                if len(text) > 1 and "\ufffd" not in text and any(ch.isalnum() for ch in text):
                    chunks.append(text)
                current = []
            else:
                current.append(char)
        text = "".join(current).strip()
        if len(text) > 1 and "\ufffd" not in text and any(ch.isalnum() for ch in text):
            chunks.append(text)
        return chunks[-limit:]


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.lock_path = path.with_suffix(path.suffix + ".lock")

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"tasks": {}}
        last_error: OSError | None = None
        # os.replace is atomic, but on Windows a reader can briefly receive
        # PermissionError/FileNotFoundError while another thread swaps the
        # state file.  Retry that transient window; never replace it with an
        # empty state if every attempt fails.
        for _ in range(20):
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
            except (PermissionError, FileNotFoundError) as error:
                last_error = error
                time.sleep(0.01)
                continue
            except (OSError, json.JSONDecodeError) as error:
                raise RuntimeError(f"重试状态文件无法读取：{self.path}") from error
            if not isinstance(value, dict):
                raise RuntimeError(f"重试状态文件格式无效：{self.path}")
            return value
        raise RuntimeError(f"重试状态文件暂时不可读取：{self.path}") from last_error

    def _save_unlocked(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f"{self.path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        last_error: PermissionError | None = None
        for attempt in range(20):
            try:
                os.replace(temporary, self.path)
                return
            except PermissionError as error:
                last_error = error
                time.sleep(min(0.5, 0.025 * (attempt + 1)))
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"无法原子保存重试状态文件：{self.path}") from last_error

    def save(self, state: dict[str, Any]) -> None:
        with ProcessLock(self.lock_path, stale_seconds=30, wait_seconds=10):
            self._save_unlocked(state)

    def mutate(self, action: Callable[[dict[str, Any]], Any]) -> Any:
        """Apply a small state transition while holding the inter-process lock."""
        with ProcessLock(self.lock_path, stale_seconds=30, wait_seconds=10):
            state = self.load()
            result = action(state)
            self._save_unlocked(state)
            return result

    def mark_successes_observed(self, task_ids: Iterable[str], max_history: int = 500) -> set[str]:
        """Persist monitor observations and return only IDs not logged before."""
        candidates = [str(task_id) for task_id in task_ids]
        if not candidates:
            return set()
        with ProcessLock(self.lock_path, stale_seconds=30, wait_seconds=10):
            state = self.load()
            monitor = state.setdefault("monitor", {})
            seen = [str(task_id) for task_id in monitor.get("observedSuccessTaskIds", [])]
            seen_set = set(seen)
            newly_observed = {task_id for task_id in candidates if task_id not in seen_set}
            if not newly_observed:
                return set()
            merged = (seen + [task_id for task_id in candidates if task_id not in seen_set])[-max(1, max_history):]
            monitor["observedSuccessTaskIds"] = merged
            monitor["lastSuccessScanAt"] = datetime.now().isoformat()
            self._save_unlocked(state)
            return newly_observed


class ProcessLock:
    def __init__(self, path: Path, stale_seconds: int = 8 * 3600, wait_seconds: float = 0):
        self.path = path
        self.stale_seconds = stale_seconds
        self.wait_seconds = wait_seconds
        self.acquired = False

    def __enter__(self) -> "ProcessLock":
        deadline = time.monotonic() + self.wait_seconds
        while True:
            try:
                if self.path.exists():
                    try:
                        content = json.loads(self.path.read_text(encoding="utf-8"))
                        old_pid = int(content.get("pid", 0))
                    except Exception:
                        old_pid = 0
                    is_stale = (time.time() - self.path.stat().st_mtime > self.stale_seconds)
                    is_dead = (old_pid > 0 and not _process_is_running(old_pid))
                    if is_stale or is_dead:
                        try:
                            self.path.unlink(missing_ok=True)
                        except PermissionError:
                            # On Windows another thread/process can still have a
                            # just-released lock file open for a moment.
                            pass
                descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                break
            except FileNotFoundError:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            except (FileExistsError, PermissionError) as error:
                if time.monotonic() >= deadline:
                    raise RuntimeError("状态文件正被另一个重试控制器更新。") from error
                time.sleep(0.05)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump({"pid": os.getpid(), "startedAt": datetime.now().isoformat()}, stream)
        self.acquired = True
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)


def _process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    process_query_limited_information = 0x1000
    handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid)
    if handle:
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    # Retry-worker PIDs are created by this controller under the same account.
    # Access denied must not pin a stale state entry forever.
    return False


class ShadowBotLauncher:
    """Control the logged-in Community Edition desktop UI without an API.

    The Community Edition CLI has a daily trial quota, while the historic
    ``shadowbot:Run`` protocol only activates the already-open client in recent
    releases.  This launcher therefore uses Windows UI Automation to select one
    exact application name and invoke the visible Run button.  Execution is
    *not* considered accepted here; the queue later requires task-log/database
    evidence before consuming a retry attempt.
    """

    MAIN_WINDOW_TITLE = "影刀"
    EDITOR_TITLE_MARKERS = ("影刀应用设计器", "应用设计器")
    RUNNER_CLASS_NAME = "RobotRunnerView"
    TERMINAL_MARKERS = ("运行成功", "运行失败", "已取消", "运行取消")

    def __init__(
        self, executable: Path, mode: str = "community_desktop_ui", timeout_seconds: float = 10,
    ):
        self.executable = executable
        self.mode = mode
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    @staticmethod
    def _desktop() -> Any:
        try:
            from pywinauto import Desktop
        except ImportError as error:
            raise RuntimeError("缺少桌面控制依赖 pywinauto；请重新运行项目启动器进行检查") from error
        return Desktop(backend="uia")

    @staticmethod
    def _text(control: Any) -> str:
        try:
            return (control.window_text() or "").strip()
        except Exception:
            return ""

    @classmethod
    def _top_windows(cls, desktop: Any) -> list[Any]:
        return [window for window in desktop.windows() if window.is_visible()]

    @classmethod
    def _main_windows(cls, windows: Iterable[Any]) -> list[Any]:
        result = []
        for window in windows:
            if cls._text(window) != cls.MAIN_WINDOW_TITLE:
                continue
            try:
                process_id = int(window.element_info.process_id)
                executable = cls._process_executable(process_id)
            except Exception:
                executable = ""
            if Path(executable).name.lower() == "shadowbot.shell.exe":
                result.append(window)
        return result

    @staticmethod
    def _process_executable(process_id: int) -> str:
        """Resolve a window owner's executable without trusting its title.

        Explorer folders can legitimately be named ``影刀``.  Using only the
        caption therefore turns an unrelated folder window into a false second
        ShadowBot client and freezes the retry queue.
        """
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(0x1000, False, process_id)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            buffer = ctypes.create_unicode_buffer(32768)
            size = wintypes.DWORD(len(buffer))
            if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
                raise ctypes.WinError(ctypes.get_last_error())
            return buffer.value
        finally:
            kernel32.CloseHandle(handle)

    @classmethod
    def _runner_is_terminal(cls, runner: Any) -> bool:
        texts = [cls._text(runner)]
        try:
            texts.extend(cls._text(item) for item in runner.descendants())
        except Exception:
            pass
        combined = "\n".join(text for text in texts if text)
        return any(marker in combined for marker in cls.TERMINAL_MARKERS)

    @classmethod
    def _visible_runners(cls, windows: Iterable[Any]) -> list[Any]:
        result = []
        for window in windows:
            try:
                class_name = window.element_info.class_name or ""
                process_id = int(window.element_info.process_id)
                executable = cls._process_executable(process_id)
            except Exception:
                continue
            # 6.3.9 exposed RobotRunnerView as the UIA class name, while
            # 6.3.12 exposes a generic ``Window`` class and puts the same
            # identity in the top-level caption. Support both shapes, but only
            # trust windows owned by the verified ShadowBot shell process.
            is_runner = class_name == cls.RUNNER_CLASS_NAME or cls._text(window) == cls.RUNNER_CLASS_NAME
            if is_runner and Path(executable).name.lower() == "shadowbot.shell.exe":
                result.append(window)
        return result

    def inspect_state(self) -> CommunityClientState:
        try:
            desktop = self._desktop()
            windows = self._top_windows(desktop)
            titles = tuple(self._text(window) for window in windows if self._text(window))
            editing = any(
                any(marker in title for marker in self.EDITOR_TITLE_MARKERS)
                for title in titles
            )
            runners = self._visible_runners(windows)
            running_runners = [runner for runner in runners if not self._runner_is_terminal(runner)]
            terminal_runners = [runner for runner in runners if self._runner_is_terminal(runner)]
            main_windows = self._main_windows(windows)
            if len(main_windows) != 1:
                # ShadowBot may hide its main shell while RobotRunnerView owns
                # the active workflow. The runner is stronger execution
                # evidence than absence of the shell, so keep the lane blocked
                # instead of reporting the client as unavailable/cancelled.
                if running_runners:
                    return CommunityClientState(
                        available=True,
                        editing=editing,
                        pages=(f"影刀主窗口={len(main_windows)}", f"运行窗口={len(running_runners)}"),
                        running=True,
                        running_app_name=self._text(running_runners[0]),
                    )
                # After a run finishes, 6.3.12 can leave the terminal
                # RobotRunnerView in front while its main shell stays hidden.
                # This is a recoverable idle state: launch() will close the
                # terminal runner and wait for the main window to reappear.
                if not main_windows and terminal_runners:
                    return CommunityClientState(
                        available=True,
                        editing=editing,
                        pages=("影刀主窗口=0", f"已结束运行窗口={len(terminal_runners)}"),
                        running=False,
                    )
                detail = "未找到已登录影刀主窗口" if not main_windows else f"检测到 {len(main_windows)} 个影刀主窗口"
                return CommunityClientState(False, False, (), detail)
            return CommunityClientState(
                available=True,
                editing=editing,
                pages=(f"影刀主窗口=1", f"运行窗口={len(running_runners)}"),
                running=bool(running_runners),
                running_app_name=self._text(running_runners[0]) if running_runners else None,
            )
        except Exception as error:
            return CommunityClientState(
                available=False, editing=False, pages=(), error=f"{type(error).__name__}: {error}",
            )

    def recover_window(self, wait_seconds: float | None = None) -> CommunityClientState:
        """Start or reactivate the Community client when its UI is hidden.

        ShadowBot 6.3.12 can leave ``ShadowBot.Shell.exe`` alive but remove all
        top-level windows after a scheduled runner closes. Reopening the normal
        Community launcher activates that existing single-instance client; it
        does not call the quota-limited CLI/API and does not start an RPA app.
        """
        state = self.inspect_state()
        if state.available:
            return state
        if not self.executable.exists():
            return CommunityClientState(
                False, False, (), f"影刀启动程序不存在：{self.executable}",
            )
        try:
            subprocess.Popen(
                [str(self.executable)],
                cwd=str(self.executable.parent),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as error:
            return CommunityClientState(
                False, False, (), f"影刀窗口恢复启动失败：{type(error).__name__}: {error}",
            )
        deadline = time.monotonic() + max(
            0.0, self.timeout_seconds if wait_seconds is None else float(wait_seconds),
        )
        while True:
            state = self.inspect_state()
            if state.available:
                return state
            if time.monotonic() >= deadline:
                return state
            time.sleep(0.2)

    @classmethod
    def _application_name(cls, item: Any) -> str:
        try:
            names = [cls._text(text) for text in item.descendants(control_type="Text")]
        except Exception:
            names = []
        return next((name for name in names if name), cls._text(item))

    @staticmethod
    def _application_list_boxes(main_window: Any) -> list[Any]:
        return [
            item for item in main_window.descendants()
            if (item.element_info.automation_id or "") == "ListBox"
        ]

    def _ensure_apps_view(self, main_window: Any) -> None:
        """Navigate from schedules/logs back to My Apps when necessary."""
        list_boxes = self._application_list_boxes(main_window)
        if len(list_boxes) == 1:
            return
        if len(list_boxes) > 1:
            raise RuntimeError(f"应用列表定位失败：检测到 {len(list_boxes)} 个 ListBox")
        tab_lists = [
            item for item in main_window.descendants()
            if (item.element_info.automation_id or "") == "MainTabList"
        ]
        if len(tab_lists) != 1:
            raise RuntimeError(f"影刀主导航定位失败：期望 1 个 MainTabList，实际 {len(tab_lists)} 个")
        tabs = tab_lists[0].children(control_type="ListItem")
        if not tabs:
            raise RuntimeError("影刀主导航没有可用模块")
        # In Community Edition 6.3.x the first MainTabList item is My Apps.
        # Use the live UIA item rectangle rather than a hard-coded coordinate.
        main_window.set_focus()
        tabs[0].click_input()
        deadline = time.monotonic() + min(5.0, self.timeout_seconds)
        while time.monotonic() < deadline:
            list_boxes = self._application_list_boxes(main_window)
            if len(list_boxes) == 1:
                return
            if len(list_boxes) > 1:
                raise RuntimeError(f"切换应用页后检测到 {len(list_boxes)} 个 ListBox")
            time.sleep(0.1)
        raise RuntimeError("已切换影刀‘我的应用’，但应用列表未在规定时间内加载")

    @classmethod
    def _dismiss_terminal_runners(cls, windows: Iterable[Any]) -> None:
        for runner in cls._visible_runners(windows):
            if not cls._runner_is_terminal(runner):
                continue
            close_buttons = [
                button for button in runner.descendants(control_type="Button")
                if (button.element_info.automation_id or "") == "CloseButton"
            ]
            if len(close_buttons) == 1 and close_buttons[0].is_enabled():
                close_buttons[0].invoke()

    @classmethod
    def _confirm_parameter_dialog_if_present(
        cls, main_window: Any, wait_seconds: float = 0,
    ) -> bool:
        """Confirm ShadowBot's optional launch-parameter dialog.

        Clicking Run for an app that declares ``parameter`` does not create a
        task immediately.  ShadowBot first opens a child window named
        ``输入应用参数``.  Leaving it open makes every later click appear to
        succeed while no task is ever started, so it must be part of the same
        verified desktop launch transaction.
        """
        deadline = time.monotonic() + max(0.0, wait_seconds)
        while True:
            dialogs = [
                item for item in main_window.descendants(control_type="Window")
                if cls._text(item) == "输入应用参数" and item.is_visible()
            ]
            if len(dialogs) > 1:
                raise RuntimeError(f"检测到 {len(dialogs)} 个输入应用参数窗口，已停止自动确认")
            if dialogs:
                confirm_buttons = [
                    button for button in dialogs[0].descendants(control_type="Button")
                    if cls._text(button) == "确定" and button.is_visible()
                ]
                if len(confirm_buttons) != 1 or not confirm_buttons[0].is_enabled():
                    raise RuntimeError("输入应用参数窗口中的确定按钮不可用或不唯一")
                main_window.set_focus()
                confirm_buttons[0].click_input()
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.1)

    @classmethod
    def _select_and_invoke(cls, main_window: Any, app_name: str) -> None:
        list_boxes = cls._application_list_boxes(main_window)
        if len(list_boxes) != 1:
            raise RuntimeError(f"应用列表定位失败：期望 1 个 ListBox，实际 {len(list_boxes)} 个")
        items = list_boxes[0].descendants(control_type="ListItem")
        matches = [item for item in items if cls._application_name(item) == app_name]
        if not matches:
            # WPF virtualizes the My Apps list, so descendants() only returns
            # the dozen currently materialized rows. Use the unique visible
            # search box to materialize an off-screen app, then still require
            # one exact full-name match before selecting anything.
            search_boxes = [
                item for item in main_window.descendants(control_type="Edit")
                if item.is_visible() and item.is_enabled()
            ]
            if len(search_boxes) != 1:
                raise RuntimeError(
                    f"应用未出现在当前列表，且搜索框不唯一：{app_name}（搜索框 {len(search_boxes)} 个）"
                )
            search_boxes[0].set_focus()
            search_boxes[0].set_edit_text(app_name)
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                list_boxes = cls._application_list_boxes(main_window)
                if len(list_boxes) != 1:
                    raise RuntimeError(f"搜索后应用列表数量异常：{len(list_boxes)}")
                items = list_boxes[0].descendants(control_type="ListItem")
                matches = [item for item in items if cls._application_name(item) == app_name]
                if matches:
                    break
                time.sleep(0.1)
        if len(matches) != 1:
            raise RuntimeError(f"应用名必须唯一匹配：{app_name}（实际匹配 {len(matches)} 条）")
        target = matches[0]
        try:
            target.scroll_into_view()
        except Exception:
            pass
        target.select()
        if hasattr(target, "is_selected") and not target.is_selected():
            raise RuntimeError(f"影刀未确认选中应用：{app_name}")
        run_buttons = [
            button for button in main_window.descendants(control_type="Button")
            if cls._text(button) == "运行" and button.is_visible()
        ]
        if len(run_buttons) != 1:
            raise RuntimeError(f"运行按钮定位失败：期望 1 个，实际 {len(run_buttons)} 个")
        run_button = run_buttons[0]
        if not run_button.is_enabled():
            raise RuntimeError("影刀运行按钮当前不可用")
        # ShadowBot's WPF button exposes UIA InvokePattern but 6.3.9 can return
        # from Invoke without dispatching the command.  A real input click at
        # the control's live rectangle is what a Community Edition user does
        # and does not touch the quota-limited CLI/API.  Bring only the verified
        # ShadowBot window to the foreground immediately before clicking.
        main_window.set_focus()
        run_button.click_input()

    def launch(self, app_id: str, app_name: str) -> LaunchReceipt:
        if self.mode != "community_desktop_ui":
            raise RuntimeError(f"社区版仅支持 community_desktop_ui 启动方式，当前方式无效：{self.mode}")
        if not app_name.strip():
            raise RuntimeError("失败记录缺少应用名，无法安全地在桌面客户端中精确选择")
        state = self.inspect_state()
        if not state.available:
            raise RuntimeError(state.error or "影刀主窗口不可用")
        if state.editing:
            raise RuntimeError("影刀应用设计器仍处于打开状态")
        if state.running:
            raise RuntimeError("影刀仍有流程正在运行")
        desktop = self._desktop()
        windows = self._top_windows(desktop)
        # A completed 6.3.12 runner can be the only visible ShadowBot window.
        # Close it first, then wait for the hidden main shell to return before
        # locating the next app. Doing this after requiring the main window
        # creates a permanent post-success queue deadlock.
        self._dismiss_terminal_runners(windows)
        deadline = time.monotonic() + min(5.0, self.timeout_seconds)
        while True:
            windows = self._top_windows(desktop)
            main_windows = self._main_windows(windows)
            if len(main_windows) == 1:
                break
            if len(main_windows) > 1:
                raise RuntimeError(f"启动前检测到多个影刀主窗口：{len(main_windows)}")
            if time.monotonic() >= deadline:
                raise RuntimeError("已关闭结束运行窗口，但影刀主窗口未重新出现")
            time.sleep(0.1)
        main_window = main_windows[0]
        # Recover a dialog left by a previously unverified Run click before
        # sending another click to the obscured main window.
        if not self._confirm_parameter_dialog_if_present(main_window):
            self._ensure_apps_view(main_window)
            self._select_and_invoke(main_window, app_name)
            self._confirm_parameter_dialog_if_present(
                main_window, wait_seconds=min(3.0, self.timeout_seconds),
            )
        return LaunchReceipt(task_id="", method="community-desktop-ui-unacknowledged")


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _crypt_protect(data: bytes) -> bytes:
    buffer = ctypes.create_string_buffer(data)
    input_blob = _DataBlob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
    output_blob = _DataBlob()
    if not ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(input_blob), "ShadowBot retry webhook", None, None, None, 1, ctypes.byref(output_blob)
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(output_blob.pbData)


def _crypt_unprotect(data: bytes) -> bytes:
    buffer = ctypes.create_string_buffer(data)
    input_blob = _DataBlob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
    output_blob = _DataBlob()
    if not ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(input_blob), None, None, None, None, 1, ctypes.byref(output_blob)
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(output_blob.pbData)


def save_webhook_secret(url: str, path: Path = SECRET_PATH) -> None:
    expected = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="
    if not url.startswith(expected) or len(url) <= len(expected):
        raise ValueError("请输入完整的企业微信群机器人 webhook 地址。")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_crypt_protect(url.encode("utf-8")))


def load_webhook_secret(path: Path = SECRET_PATH) -> str | None:
    if not path.exists():
        return None
    try:
        return _crypt_unprotect(path.read_bytes()).decode("utf-8")
    except Exception as e:
        logging.error("解密 Webhook 失败 (可能 SYSTEM 服务无权解密用户 DPAPI): %s", e)
        return None


class WeComNotifier:
    def __init__(self, secret_path: Path = SECRET_PATH):
        self.secret_path = secret_path

    def send(self, title: str, message: str) -> bool:
        logging.info("%s | %s", title, message.replace("\n", " | "))
        url = load_webhook_secret(self.secret_path)
        if not url:
            logging.warning("未配置企业微信 webhook；通知仅写入本地日志。")
            return False
        content = f"### {title}\n{message}"[:3900]
        payload = json.dumps({"msgtype": "markdown", "markdown": {"content": content}}, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        for attempt, delay in enumerate((0, 2, 5), start=1):
            if delay:
                time.sleep(delay)
            try:
                with urllib.request.urlopen(request, timeout=15) as response:
                    result = json.loads(response.read().decode("utf-8"))
                if result.get("errcode") == 0:
                    return True
                logging.error(
                    "企业微信通知失败（第 %s 次）：errcode=%s errmsg=%s",
                    attempt, result.get("errcode"), result.get("errmsg"),
                )
            except Exception as error:
                logging.error("企业微信通知请求失败（第 %s 次）：%s", attempt, type(error).__name__)
        return False


class CodexAnalyzer:
    def __init__(self, executable: Path, workdir: Path, schema_path: Path, timeout_seconds: int = 60):
        self.executable = executable
        self.workdir = workdir
        self.schema_path = schema_path
        self.timeout_seconds = max(1, int(timeout_seconds))

    def analyze(self, original: TaskRecord, attempts: list[dict[str, Any]], log_lines: list[str]) -> dict[str, Any]:
        context = {
            "original_failure": asdict(original),
            "retry_attempts": attempts,
            "last_log_lines": log_lines,
        }
        prompt = (
            "你是影刀 RPA 故障分析助手。只根据下方 JSON 分析失败原因，不调用任何工具，不执行命令。"
            "请用简洁中文输出符合指定 JSON Schema 的结果，不要臆造日志中没有的信息。\n\n"
            + json.dumps(context, ensure_ascii=False, indent=2)
        )
        command = [
            str(self.executable), "exec", "--ephemeral", "--sandbox", "read-only",
            "--ignore-user-config", "--skip-git-repo-check", "--output-schema", str(self.schema_path),
            "-C", str(self.workdir), "-",
        ]
        try:
            result = subprocess.run(
                command, input=prompt, text=True, encoding="utf-8", capture_output=True,
                timeout=self.timeout_seconds,
            )
            if result.returncode != 0:
                raise RuntimeError(f"Codex exit code {result.returncode}")
            return json.loads(result.stdout)
        except Exception as error:
            logging.error("Codex 分析失败：%s", error)
            return {
                "summary": "自动重试三次后仍未成功。",
                "likely_cause": original.error or "任务日志未提供明确原因。",
                "recommendations": ["在影刀 Studio 中检查失败指令的元素选择器、页面加载等待和网络重试设置。"],
            }


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("配置文件根节点必须是 JSON 对象。")
    required_paths = ("shadowbot_exe", "user_folder", "codex_exe", "codex_workdir")
    missing = [key for key in required_paths if not config.get(key)]
    if missing:
        raise ValueError("配置缺少必填字段：" + ", ".join(missing))
    mode = str(config.get("shadowbot_launch_mode", "community_desktop_ui"))
    if mode != "community_desktop_ui":
        raise ValueError("本控制器仅支持社区版 community_desktop_ui 启动方式")
    for key in (
        "poll_interval_seconds", "scan_interval_seconds", "task_start_timeout_seconds",
        "task_run_timeout_seconds", "trigger_max_age_seconds",
        "uncommitted_task_log_max_age_seconds",
    ):
        if key in config and float(config[key]) < 0:
            raise ValueError(f"配置 {key} 不能小于 0。")
    for key in ("trigger_source_kinds", "retry_result_source_kinds", "active_task_statuses"):
        if key in config and not isinstance(config[key], list):
            raise ValueError(f"配置 {key} 必须是数组。")
    return config


def configure_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        retention_days = float(load_config().get("log_retention_days", 3))
    except Exception:
        retention_days = 3
    handler = RecentDaysFileHandler(LOG_DIR / "retry_controller.log", retention_days=retention_days)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[handler, logging.StreamHandler()])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ShadowBot retry controller diagnostics")
    parser.add_argument("command", choices=["status", "health", "test-notification"])
    arguments = parser.parse_args(argv)
    configure_logging()
    if arguments.command == "status":
        print(json.dumps(StateStore(STATE_PATH).load(), ensure_ascii=False, indent=2))
        return 0
    config = load_config()
    if arguments.command == "health":
        codex = subprocess.run(
            [config["codex_exe"], "login", "status"], capture_output=True, text=True, timeout=30
        )
        client = ShadowBotLauncher(
            Path(config["shadowbot_exe"]),
            str(config.get("shadowbot_launch_mode", "community_desktop_ui")),
            float(config.get("community_ui_timeout_seconds", 10)),
        ).inspect_state()
        result = {
            "shadowbotExecutable": Path(config["shadowbot_exe"]).exists(),
            "shadowbotDesktopAvailable": client.available,
            "shadowbotEditing": client.editing,
            "shadowbotRunning": client.running,
            "shadowbotDesktopError": client.error,
            "taskDatabase": (Path(config["user_folder"]) / "tasks.db3").exists(),
            "codexExecutable": Path(config["codex_exe"]).exists(),
            "codexLoggedIn": codex.returncode == 0,
            "wecomWebhookConfigured": SECRET_PATH.exists(),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        required = (
            result["shadowbotExecutable"], result["shadowbotDesktopAvailable"],
            result["taskDatabase"], result["codexExecutable"], result["codexLoggedIn"],
        )
        return 0 if all(required) else 1
    if arguments.command == "test-notification":
        sent = WeComNotifier().send("影刀重试控制器测试", "> 本机控制器与企业微信群机器人连接正常。")
        print(json.dumps({"sent": sent}, ensure_ascii=False))
        return 0 if sent else 1
    raise AssertionError("未处理的命令")


if __name__ == "__main__":
    raise SystemExit(main())

"""影刀失败重试常驻服务。

这里不使用“一个任务占住线程直到三次重试结束”的模型。服务每次扫描只推进
一个持久化状态转换；因此影刀忙碌、等待静默窗口、等待任务记录和后台分析都
不会让后续失败项从队列中消失。
"""
