from __future__ import annotations

import atexit
import ctypes
from ctypes import wintypes
from dataclasses import asdict
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable

from retry_controller import (
    CANCELLED_STATUS,
    CodexAnalyzer,
    FAILURE_STATUSES,
    CommunityClientState,
    SUCCESS_STATUS,
    ShadowBotLauncher,
    StateStore,
    TaskRecord,
    TaskStore,
    WeComNotifier,
    configure_logging,
    is_editing_failure,
    load_config,
)


BASE_DIR = Path(__file__).resolve().parent
LOCK_PATH = BASE_DIR / "retry_controller_service.lock"
SCAN_INTERVAL_SECONDS = 2
SERVICE_LOCK_STOP_GRACE_SECONDS = 5
# One controller instance per Windows user session.  Keep this identifier
# installation-neutral so the public package contains no local account ID.
SERVICE_MUTEX_NAME = r"Local\ShadowBotRetryController"
ERROR_ALREADY_EXISTS = 183
PENDING_STATUSES = {"queued", "deferred", "paused_editing", "launch_requested", "retry_running"}
TERMINAL_STATUSES = {"succeeded", "exhausted", "cancelled", "unaccepted", "superseded", "controller_error"}
_service_mutex_handle: int | None = None


def now() -> datetime:
    return datetime.now()


def iso(value: datetime | None = None) -> str:
    return (value or now()).isoformat()


def parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if handle:
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    # The lock belongs to this controller, which always runs in the same user
    # session.  Treating ERROR_ACCESS_DENIED as "alive" makes a stale PID lock
    # permanent when Windows refuses an otherwise harmless query.  A real
    # controller process is queryable with PROCESS_QUERY_LIMITED_INFORMATION.
    return False


def acquire_service_lock() -> bool:
    """Acquire an OS-owned mutex; the lock file is diagnostic only.

    PID files are unsafe on Windows: a stopped process can leave one behind and
    its PID can later be reused by an unrelated process.  A named mutex is
    released by Windows when the owner exits, including crashes and forced
    termination.
    """
    global _service_mutex_handle
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_mutex = kernel32.CreateMutexW
        create_mutex.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR)
        create_mutex.restype = wintypes.HANDLE
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = (wintypes.HANDLE,)
        close_handle.restype = wintypes.BOOL
        handle = create_mutex(None, True, SERVICE_MUTEX_NAME)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        if ctypes.get_last_error() == ERROR_ALREADY_EXISTS:
            close_handle(handle)
            return False
        _service_mutex_handle = handle
        LOCK_PATH.write_text(json.dumps({"pid": os.getpid(), "startedAt": iso()}), encoding="utf-8")
        return True
    except OSError:
        return False


def release_service_lock() -> None:
    global _service_mutex_handle
    try:
        if LOCK_PATH.exists() and json.loads(LOCK_PATH.read_text(encoding="utf-8")).get("pid") == os.getpid():
            LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    if _service_mutex_handle:
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.ReleaseMutex(wintypes.HANDLE(_service_mutex_handle))
            kernel32.CloseHandle(wintypes.HANDLE(_service_mutex_handle))
        finally:
            _service_mutex_handle = None


class RetryQueueService:
    """Durable, non-blocking scheduler for one ShadowBot execution lane."""

    def __init__(
        self,
        config_loader: Callable[[], dict[str, Any]] = load_config,
        task_store_factory: Callable[[Path], TaskStore] = TaskStore,
        state_store: StateStore | None = None,
        launcher_factory: Callable[[dict[str, Any]], ShadowBotLauncher] | None = None,
        notifier_factory: Callable[[], WeComNotifier] = WeComNotifier,
        analyzer_factory: Callable[[dict[str, Any]], CodexAnalyzer] | None = None,
        community_state_factory: Callable[[dict[str, Any]], CommunityClientState] | None = None,
    ) -> None:
        self.load_config = config_loader
        self.task_store_factory = task_store_factory
        self.state_store = state_store or StateStore(BASE_DIR / "retry_state.json")
        self.launcher_factory = launcher_factory or (
            lambda config: ShadowBotLauncher(
                Path(config["shadowbot_exe"]),
                str(config.get("shadowbot_launch_mode", "community_desktop_ui")),
                float(config.get("community_ui_timeout_seconds", 10)),
            )
        )
        self.notifier_factory = notifier_factory
        self.analyzer_factory = analyzer_factory or (
            lambda config: CodexAnalyzer(
                Path(config["codex_exe"]), Path(config["codex_workdir"]),
                BASE_DIR / "analysis_schema.json", int(config.get("codex_analysis_timeout_seconds", 60)),
            )
        )
        self.community_state_factory = community_state_factory or (
            lambda config: self.launcher_factory(config).inspect_state()
        )
        self._monitor_stop = threading.Event()
        self._scan_health_lock = threading.Lock()
        self._last_completed_scan_at = time.monotonic()
        self._last_active_task_count: int | None = None
        self._last_block_log_at = 0.0
        self._analysis_worker: threading.Thread | None = None
        self._notification_worker: threading.Thread | None = None
        self._success_observer_initialized = False
        self._recovered = False
        self._last_client_recovery_at = 0.0

    def _store(self, config: dict[str, Any]) -> TaskStore:
        return self.task_store_factory(Path(config["user_folder"]))

    @staticmethod
    def _entry_for(task: TaskRecord, initial_quiet_seconds: float = 0) -> dict[str, Any]:
        detected_at = now()
        created_at = parse_time(task.create_time)
        detection_delay = max(0.0, (detected_at - created_at).total_seconds()) if created_at else None
        initial_quiet_until = (
            max(detected_at, created_at + timedelta(seconds=max(0, initial_quiet_seconds)))
            if created_at else detected_at
        )
        return {
            "status": "queued",
            "workerStartTime": iso(),
            "appId": task.app_id,
            "appName": task.app_name,
            "originalTaskId": task.uuid,
            "sourceKind": task.source_kind,
            "original": asdict(task),
            "queuedAt": iso(detected_at),
            "nextEligibleAt": iso(initial_quiet_until),
            "initialQuietUntil": iso(initial_quiet_until),
            "detectionDelaySeconds": round(detection_delay, 3) if detection_delay is not None else None,
            "attempts": [],
            "launchRequests": 0,
            "actualExecutionCount": 0,
            "executionAttempts": 0,
        }

    @staticmethod
    def _is_due(entry: dict[str, Any], moment: datetime) -> bool:
        due = parse_time(entry.get("nextEligibleAt"))
        return due is None or due <= moment

    @staticmethod
    def _notification_due(entry: dict[str, Any], moment: datetime) -> bool:
        due = parse_time(entry.get("nextNotificationAt"))
        return due is None or due <= moment

    def _mutate(self, action: Callable[[dict[str, Any]], Any]) -> Any:
        return self.state_store.mutate(action)

    def recover_interrupted_work(self) -> None:
        """Make every persisted non-terminal state recoverable after a restart."""
        if self._recovered:
            return

        def repair(state: dict[str, Any]) -> int:
            changed = 0
            for entry in state.setdefault("tasks", {}).values():
                if entry.get("analysisStatus") == "running":
                    entry["analysisStatus"] = "pending"
                    entry["analysisRecoveredAt"] = iso()
                    changed += 1
                if entry.get("notificationStatus") == "running":
                    entry["notificationStatus"] = "pending"
                    entry["notificationRecoveredAt"] = iso()
                    changed += 1
            return changed

        repaired = self._mutate(repair)
        if repaired:
            logging.warning("服务恢复：已重新接管 %s 个未完成状态。", repaired)
        self._recovered = True

    def enqueue_new_failures(self, config: dict[str, Any], store: TaskStore) -> int:
        source_kinds = tuple(int(value) for value in config.get("trigger_source_kinds", [0, 8]))
        snapshot = self.state_store.load()
        known = set(snapshot.setdefault("tasks", {})) | set(snapshot.get("seenFailureIds", []))
        try:
            max_age = int(float(config["trigger_max_age_seconds"]))
        except (ValueError, TypeError):
            max_age = 259200
        candidates = store.trigger_failures(max_age, known, source_kinds)
        if not candidates:
            return 0

        def claim_all(state: dict[str, Any]) -> list[TaskRecord]:
            tasks = state.setdefault("tasks", {})
            seen = [str(value) for value in state.setdefault("seenFailureIds", [])]
            seen_set = set(seen)
            claimed: list[TaskRecord] = []
            for task in reversed(candidates):  # database returns new-to-old; queue is old-to-new.
                if task.uuid in tasks or task.uuid in seen_set:
                    continue
                tasks[task.uuid] = self._entry_for(
                    task, float(config.get("community_initial_quiet_seconds", 0)),
                )
                seen.append(task.uuid)
                seen_set.add(task.uuid)
                claimed.append(task)
            state["seenFailureIds"] = seen[-max(100, int(config.get("seen_failure_history_size", 5000))):]
            return claimed

        claimed = self._mutate(claim_all)
        for task in claimed:
            entry = self.state_store.load().setdefault("tasks", {}).get(task.uuid, {})
            delay = entry.get("detectionDelaySeconds")
            warning_after = max(0.0, float(config.get("failure_detection_lag_warning_seconds", 30)))
            if isinstance(delay, (int, float)) and delay > warning_after:
                logging.warning("失败任务延迟发现：%s | %s | 延迟=%.1f 秒", task.uuid, task.app_name, delay)
            else:
                logging.info("失败任务已入队：%s | %s", task.uuid, task.app_name)
        return len(claimed)

    def _finish(self, task_id: str, succeeded: bool, reason: str, config: dict[str, Any]) -> None:
        def mutate(state: dict[str, Any]) -> None:
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") in TERMINAL_STATUSES:
                return
            entry.update({
                "status": "succeeded" if succeeded else "exhausted",
                "completedAt": iso(),
                "finalReason": reason,
            })
            if succeeded:
                entry.update({"notificationStatus": "pending", "nextNotificationAt": iso()})
            elif config.get("enable_codex_analysis", False):
                entry.update({"analysisStatus": "pending", "notificationStatus": "waiting_analysis"})
            else:
                entry.update({"analysisStatus": "skipped", "notificationStatus": "pending", "nextNotificationAt": iso()})

        self._mutate(mutate)
        logging.info("重试任务已结束：%s | %s", task_id, reason)

    def _finish_cancelled(self, task_id: str, retry: TaskRecord) -> None:
        """A user-cancelled ShadowBot run is final, not a retryable failure."""
        def mutate(state: dict[str, Any]) -> None:
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") in TERMINAL_STATUSES:
                return
            if entry.get("attempts"):
                entry["attempts"][-1].update({
                    "status": "cancelled", "statusCode": retry.status, "finishedAt": iso(),
                })
            entry.update({
                "status": "cancelled", "completedAt": iso(),
                "finalReason": f"影刀重试已由用户取消，任务={retry.uuid}",
                "analysisStatus": "skipped", "notificationStatus": "suppressed",
            })
        self._mutate(mutate)
        logging.info("影刀重试已取消，停止后续重试且不发送通知：%s | 重试任务=%s", task_id, retry.uuid)

    def _defer_uncommitted_termination(
        self, task_id: str, task_log_ids: list[str], client: CommunityClientState,
        config: dict[str, Any],
    ) -> None:
        """Requeue a desktop launch that ended before tasks.db3 was committed.

        Community Edition creates a small ``.tasklog`` before it inserts the
        task row. If that log stops changing and the runner is no longer
        present, the launch was cancelled, the client exited, or the desktop
        runtime aborted before commit. There is no reliable local evidence that
        distinguishes those cases, so it must never be labelled as a user
        cancellation. Requeue it without consuming a retry and release the FIFO
        lane. Only an explicit ShadowBot status 4 may permanently cancel work.
        """
        client_reason = "影刀客户端已空闲" if client.available else "影刀客户端已退出或状态不可读"
        reason = f"重试在数据库提交前中断（{client_reason}），临时日志={','.join(task_log_ids)}"
        delay = max(5.0, float(config.get("community_state_retry_seconds", 30)))

        def mutate(state: dict[str, Any]) -> None:
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") != "launch_requested":
                return
            if entry.get("attempts"):
                entry["attempts"][-1].update({
                    "status": "launch_unverified_stopped",
                    "taskLogIds": task_log_ids,
                    "finishedAt": iso(),
                })
            entry.update({
                "status": "deferred",
                "deferredReason": reason,
                "nextEligibleAt": iso(now() + timedelta(seconds=delay)),
            })
            for key in ("completedAt", "finalReason", "analysisStatus", "notificationStatus"):
                entry.pop(key, None)

        self._mutate(mutate)
        logging.warning(
            "影刀重试在数据库提交前中断；任务已重新排队、未消耗重试次数，并释放通道给后续任务：%s | 临时日志=%s | %s",
            task_id, ",".join(task_log_ids), client_reason,
        )

    def _finish_manual_takeover(self, task_id: str, manual_task: TaskRecord) -> None:
        """Stop automation when the user has manually run the same application."""
        def mutate(state: dict[str, Any]) -> None:
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") in TERMINAL_STATUSES:
                return
            if entry.get("attempts"):
                entry["attempts"][-1].update({
                    "status": "manual_takeover", "externalTaskId": manual_task.uuid,
                    "externalStatusCode": manual_task.status, "finishedAt": iso(),
                })
            entry.update({
                "status": "superseded", "completedAt": iso(),
                "supersededReason": f"检测到同一应用的人工运行，任务={manual_task.uuid}",
                "analysisStatus": "skipped", "notificationStatus": "suppressed",
            })
        self._mutate(mutate)
        logging.info(
            "检测到人工接管，停止自动重试且不冒充人工结果：%s | 人工任务=%s",
            task_id, manual_task.uuid,
        )

    def _defer_after_external_traffic(self, task_id: str, external_ids: list[str]) -> None:
        """A normal/manual task displaced the protocol request; retry later."""
        def mutate(state: dict[str, Any]) -> None:
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") in TERMINAL_STATUSES:
                return
            if entry.get("attempts"):
                entry["attempts"][-1].update({
                    "status": "displaced_by_external_task",
                    "externalTaskIds": external_ids,
                    "finishedAt": iso(),
                })
            entry.update({
                "status": "deferred",
                "deferredReason": "社区版启动请求被正常任务占用，等待静默窗口后重新发送",
                "nextEligibleAt": iso(),
            })
        self._mutate(mutate)
        logging.warning(
            "社区版启动请求遇到其他影刀任务，原失败项已保留并重新排队：%s | 其他任务=%s",
            task_id, ",".join(external_ids),
        )

    def _defer_after_attempt(self, task_id: str, reason: str, config: dict[str, Any]) -> None:
        """Count one verified RPA execution failure and schedule the next one."""
        delays = [float(value) for value in config.get("retry_delays_seconds", [30, 60, 180])]
        finish_reason: str | None = None

        def mutate(state: dict[str, Any]) -> None:
            nonlocal finish_reason
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") in TERMINAL_STATUSES:
                return
            entry["executionAttempts"] = int(entry.get("executionAttempts", 0)) + 1
            completed = int(entry["executionAttempts"])
            if completed >= max(1, len(delays)):
                finish_reason = reason
                return
            delay = delays[completed - 1] if completed <= len(delays) else delays[-1]
            entry.update({
                "status": "deferred", "deferredReason": reason,
                "nextEligibleAt": iso(now() + timedelta(seconds=max(0, delay))),
            })

        self._mutate(mutate)
        if finish_reason:
            self._finish(task_id, False, finish_reason, config)
        else:
            logging.warning("重试已延后，不计为成功：%s | %s", task_id, reason)

    def _defer_controller_issue(self, task_id: str, reason: str, config: dict[str, Any]) -> None:
        """Keep an unverified launch in the queue without consuming an RPA retry."""
        delay = max(5.0, float(config.get("community_state_retry_seconds", 30)))
        log_interval = max(60.0, float(config.get("controller_error_log_interval_seconds", 300)))
        should_log = [False]

        def mutate(state: dict[str, Any]) -> None:
            entry = state.setdefault("tasks", {}).get(task_id)
            if not entry or entry.get("status") in TERMINAL_STATUSES:
                return
            if entry.get("attempts"):
                entry["attempts"][-1].update({
                    "status": "launch_unverified", "error": reason, "finishedAt": iso(),
                })
            entry.update({
                "status": "deferred", "deferredReason": reason,
                "nextEligibleAt": iso(now() + timedelta(seconds=delay)),
                "controllerLaunchErrors": int(entry.get("controllerLaunchErrors", 0)) + 1,
            })
            last_log = parse_time(entry.get("lastControllerErrorLoggedAt"))
            previous_reason = entry.get("lastControllerErrorReason")
            if previous_reason != reason or last_log is None or (now() - last_log).total_seconds() >= log_interval:
                entry.update({"lastControllerErrorLoggedAt": iso(), "lastControllerErrorReason": reason})
                should_log[0] = True

        self._mutate(mutate)
        if should_log[0]:
            logging.error("重试启动未获确认，任务继续保留在队列且不消耗重试次数：%s | %s", task_id, reason)

    def reconcile_attempts(self, config: dict[str, Any], store: TaskStore) -> None:
        moment = now()
        snapshot = self.state_store.load().setdefault("tasks", {})
        for task_id, entry in snapshot.items():
            status = entry.get("status")
            if status == "launch_requested":
                attempts = entry.get("attempts", [])
                if not attempts:
                    self._defer_controller_issue(task_id, "启动状态丢失，等待本机控制恢复后重试", config)
                    continue
                attempt = attempts[-1]
                launch_method = attempt.get("launchMethod")
                retry_kinds = (
                    (2,) if launch_method == "community-desktop-ui-unacknowledged"
                    else tuple(int(value) for value in config.get("retry_result_source_kinds", [5, 12]))
                )
                launched_after = parse_time(attempt.get("launchedAfter"))
                retry = store.find_new_retry_task(
                    entry["appId"], set(attempt.get("knownTaskIds", [])), retry_kinds, launched_after,
                )
                if retry:
                    def found(state: dict[str, Any]) -> None:
                        current = state.setdefault("tasks", {}).get(task_id)
                        if not current or current.get("status") != "launch_requested":
                            return
                        active = current["attempts"][-1]
                        active.update({"status": "retry_running", "taskId": retry.uuid, "statusCode": retry.status})
                        current.update({
                            "status": "retry_running", "retryTaskId": retry.uuid,
                            "actualExecutionCount": int(current.get("actualExecutionCount", 0)) + 1,
                            "runDeadlineAt": iso(now() + timedelta(seconds=float(config["task_run_timeout_seconds"]))),
                        })
                    self._mutate(found)
                    logging.info("已关联本次重试记录：%s | 原始失败=%s", retry.uuid, task_id)
                    continue

                # Community Edition commonly creates and writes the .tasklog at
                # execution start while keeping the tasks.db3 row invisible to
                # external readers until the task finishes.  Treat an unmatched
                # new log as provisional running evidence, never as success.
                known_log_ids = set(attempt.get("knownLogIds", []))
                log_candidates = store.find_new_task_logs(known_log_ids, launched_after)
                candidate_rows = [(item, store.get_task(item.task_id)) for item in log_candidates]
                uncommitted_logs = [item for item, record in candidate_rows if record is None]
                if uncommitted_logs:
                    candidate_ids = [item.task_id for item in uncommitted_logs]
                    previously_seen = set(attempt.get("provisionalTaskLogIds", []))

                    def remember_logs(state: dict[str, Any]) -> None:
                        current = state.setdefault("tasks", {}).get(task_id)
                        if not current or current.get("status") != "launch_requested" or not current.get("attempts"):
                            return
                        active = current["attempts"][-1]
                        merged = list(dict.fromkeys([
                            *active.get("provisionalTaskLogIds", []), *candidate_ids,
                        ]))
                        active.update({
                            "provisionalTaskLogIds": merged,
                            "lastTaskLogEvidenceAt": iso(),
                        })

                    self._mutate(remember_logs)
                    newly_seen = [value for value in candidate_ids if value not in previously_seen]
                    if newly_seen:
                        logging.info(
                            "已发现影刀任务日志，确认客户端正在执行；等待数据库提交后核对：%s | 原始失败=%s",
                            ",".join(newly_seen), task_id,
                        )

                    # A provisional log is only running evidence while either
                    # it is still being written or the desktop runner remains
                    # visible. A stale log plus an idle/missing runner means
                    # execution ended before SQLite was committed. The old
                    # unconditional ``continue`` blocked the FIFO queue forever
                    # after an early user cancellation.
                    stale_after = max(
                        0.0, float(config.get("uncommitted_task_log_max_age_seconds", 60)),
                    )
                    newest_write = max(item.modified_at for item in uncommitted_logs)
                    if (moment - newest_write).total_seconds() >= stale_after:
                        try:
                            client = self.community_state_factory(config)
                        except Exception as error:
                            client = CommunityClientState(
                                False, False, (), f"{type(error).__name__}: {error}",
                            )
                        if not client.running:
                            self._defer_uncommitted_termination(task_id, candidate_ids, client, config)
                            continue
                    continue

                # A matching retry row would already have taken the branch
                # above.  Every remaining committed candidate is external
                # ShadowBot traffic that happened while this request waited.
                committed_external = [
                    (item, record) for item, record in candidate_rows if record is not None
                ]
                manual_same_app = next((
                    record for _item, record in committed_external
                    if record.source_kind == 2 and record.app_id == entry["appId"]
                ), None) if launch_method != "community-desktop-ui-unacknowledged" else None
                if manual_same_app:
                    self._finish_manual_takeover(task_id, manual_same_app)
                    continue
                if committed_external:
                    external_ids = [record.uuid for _item, record in committed_external]
                    observed = set(attempt.get("externalTrafficTaskIds", []))
                    new_external = [value for value in external_ids if value not in observed]
                    if new_external:
                        quiet_seconds = max(0.0, float(config.get("scheduled_task_quiet_seconds", 60)))
                        activity_end = max(item.modified_at for item, _record in committed_external)
                        wait_until = max(moment, activity_end + timedelta(seconds=quiet_seconds))

                        def remember_external(state: dict[str, Any]) -> None:
                            current = state.setdefault("tasks", {}).get(task_id)
                            if not current or current.get("status") != "launch_requested" or not current.get("attempts"):
                                return
                            active = current["attempts"][-1]
                            active["externalTrafficTaskIds"] = list(dict.fromkeys([
                                *active.get("externalTrafficTaskIds", []), *external_ids,
                            ]))
                            previous_deadline = parse_time(active.get("recordDeadlineAt"))
                            if previous_deadline is None or wait_until > previous_deadline:
                                active["recordDeadlineAt"] = iso(wait_until)

                        self._mutate(remember_external)
                        logging.info(
                            "重试请求期间检测到其他影刀任务；保留原失败项并等待其结束：%s | 其他任务=%s",
                            task_id, ",".join(new_external),
                        )
                        continue
                    deadline = parse_time(attempt.get("recordDeadlineAt"))
                    if deadline and moment < deadline:
                        continue
                    self._defer_after_external_traffic(task_id, external_ids)
                    continue
                deadline = parse_time(attempt.get("recordDeadlineAt"))
                if deadline and moment >= deadline:
                    self._defer_controller_issue(task_id, "启动超时：未发现可验证的影刀重试记录", config)

            elif status == "retry_running":
                retry_id = entry.get("retryTaskId")
                retry = store.get_task(str(retry_id)) if retry_id else None
                observed_status = retry.status if retry else None
                observed_id = retry.uuid if retry else str(retry_id or "")
                observed_error = retry.error if retry else None
                if observed_status == SUCCESS_STATUS:
                    actual_count = max(1, int(entry.get("actualExecutionCount", 0)))
                    self._finish(task_id, True, f"第 {actual_count} 次实际重试成功，任务={observed_id}", config)
                    continue
                if observed_status == CANCELLED_STATUS:
                    self._finish_cancelled(task_id, retry)
                    continue
                if observed_status in FAILURE_STATUSES:
                    def failed(state: dict[str, Any]) -> None:
                        current = state.setdefault("tasks", {}).get(task_id)
                        if current and current.get("attempts"):
                            current["attempts"][-1].update({
                                "status": "failed", "statusCode": observed_status, "error": observed_error,
                                "finishedAt": iso(),
                            })
                    self._mutate(failed)
                    self._defer_after_attempt(task_id, f"影刀重试失败：{observed_error or observed_id}", config)
                    continue
                deadline = parse_time(entry.get("runDeadlineAt"))
                if deadline and moment >= deadline:
                    # A timeout is not a verified execution failure.  Continue
                    # observing the same task instead of consuming one of the
                    # three retries or launching a duplicate process.
                    extension = max(60.0, float(config.get("task_run_timeout_seconds", 1800)))

                    def extend_monitoring(state: dict[str, Any]) -> None:
                        current = state.setdefault("tasks", {}).get(task_id)
                        if current and current.get("attempts"):
                            current["attempts"][-1].update({
                                "status": "retry_running",
                                "lastRunTimeoutAt": iso(),
                            })
                            current["runDeadlineAt"] = iso(now() + timedelta(seconds=extension))
                    self._mutate(extend_monitoring)
                    logging.warning(
                        "重试任务超过预期时长但尚无终态，继续监控且不计失败：%s | 重试任务=%s",
                        task_id, retry_id or "数据库记录暂不可见",
                    )

    def _has_active_retry(self, state: dict[str, Any]) -> bool:
        return any(entry.get("status") in {"launch_requested", "retry_running"} for entry in state.setdefault("tasks", {}).values())

    def _quiet_remaining(self, store: TaskStore, config: dict[str, Any], moment: datetime) -> tuple[float, TaskRecord | None]:
        quiet = max(0.0, float(config.get("scheduled_task_quiet_seconds", 0)))
        if not quiet:
            return 0.0, None
        kinds = config.get("scheduled_activity_source_kinds", config.get("trigger_source_kinds", [0, 8]))
        evidence = store.latest_task_activity_evidence(
            kinds, int(config.get("scheduled_activity_history_limit", 200)),
        )
        if not evidence:
            return 0.0, None
        activity_at, activity = evidence
        return max(0.0, quiet - (moment - activity_at).total_seconds()), activity

    def _defer_for_community_state(
        self, task_id: str, status: str, reason: str, delay_seconds: float,
    ) -> None:
        def defer(state_to_change: dict[str, Any]) -> None:
            current = state_to_change.setdefault("tasks", {}).get(task_id)
            if current and current.get("status") not in TERMINAL_STATUSES:
                current.update({
                    "status": status,
                    "deferredReason": reason,
                    "nextEligibleAt": iso(now() + timedelta(seconds=max(1, delay_seconds))),
                })
                if status == "paused_editing":
                    current["editingLastSeenAt"] = iso()
                    # Every positive/unknown editor observation breaks the
                    # previous clear interval.  A fresh uninterrupted stable
                    # interval is required before retrying.
                    current.pop("editingClearSince", None)
        self._mutate(defer)

    def _editing_pause_ready(self, task_id: str, entry: dict[str, Any], config: dict[str, Any]) -> bool:
        """Require a stable non-editor state before a formerly edited app resumes."""
        try:
            client = self.community_state_factory(config)
        except Exception as error:
            client = CommunityClientState(False, False, (), f"{type(error).__name__}: {error}")
        retry_after = max(5.0, float(config.get("community_state_retry_seconds", 30)))
        if not client.available:
            self._defer_for_community_state(task_id, "paused_editing", "无法确认影刀社区版界面状态，已暂停自动重试", retry_after)
            self._log_blocked("无法读取影刀社区版界面状态", task_id, entry.get("appName", "未知流程"))
            return False
        if client.editing:
            self._defer_for_community_state(task_id, "paused_editing", "检测到影刀正在编辑应用，已暂停自动重试", retry_after)
            self._log_blocked("影刀正在编辑应用", task_id, entry.get("appName", "未知流程"))
            return False
        clear_since = parse_time(entry.get("editingClearSince"))
        stable_seconds = max(0.0, float(config.get("editing_clear_stable_seconds", 30)))
        if clear_since is None:
            def mark_clear(state_to_change: dict[str, Any]) -> None:
                current = state_to_change.setdefault("tasks", {}).get(task_id)
                if current and current.get("status") == "paused_editing":
                    current.update({"editingClearSince": iso(), "nextEligibleAt": iso(now() + timedelta(seconds=stable_seconds))})
            self._mutate(mark_clear)
            self._log_blocked("等待编辑界面关闭确认", task_id, entry.get("appName", "未知流程"))
            return False
        if (now() - clear_since).total_seconds() < stable_seconds:
            return False
        def resume(state_to_change: dict[str, Any]) -> None:
            current = state_to_change.setdefault("tasks", {}).get(task_id)
            if current and current.get("status") == "paused_editing":
                current.update({
                    "status": "deferred", "deferredReason": "编辑界面已关闭，恢复等待正常任务静默窗口",
                    "nextEligibleAt": iso(), "editingResumedAt": iso(),
                })
        self._mutate(resume)
        logging.info("编辑界面已关闭，恢复自动重试队列：%s | %s", task_id, entry.get("appName", "未知流程"))
        return False

    def schedule_next_launch(self, config: dict[str, Any], store: TaskStore) -> None:
        moment = now()
        state = self.state_store.load()
        if self._has_active_retry(state):
            return
        candidates = [
            (task_id, entry) for task_id, entry in state.setdefault("tasks", {}).items()
            if entry.get("status") in {"queued", "deferred", "paused_editing"} and self._is_due(entry, moment)
        ]
        if not candidates:
            return
        # Explicit user-requested replays may overtake the historical backlog;
        # otherwise retain original FIFO order among currently eligible work.
        task_id, entry = min(candidates, key=lambda item: (
            -int(item[1].get("manualPriority", 0)),
            item[1].get("queuedAt", ""),
            item[1].get("nextEligibleAt", ""),
        ))
        original = store.get_task(task_id)
        if not original or original.status != 3:
            def supersede(state_to_change: dict[str, Any]) -> None:
                current = state_to_change.setdefault("tasks", {}).get(task_id)
                if current and current.get("status") in {"queued", "deferred"}:
                    current.update({
                        "status": "superseded", "completedAt": iso(),
                        "supersededReason": "原始失败记录已不存在或不再是失败状态",
                    })
            self._mutate(supersede)
            logging.warning("丢弃失效队列项：%s", task_id)
            return
        if entry.get("status") == "paused_editing":
            self._editing_pause_ready(task_id, entry, config)
            return
        # An editing-related original failure must be paused once.  Once the
        # client has been confirmed clear and _editing_pause_ready recorded a
        # resume marker, do not re-enter the same pause from historical text.
        if is_editing_failure(original) and not entry.get("editingResumedAt"):
            self._defer_for_community_state(task_id, "paused_editing", "原始失败提示应用正在编辑，未发起自动重试", 1)
            self._log_blocked("原始任务失败时应用正在编辑", task_id, entry.get("appName", "未知流程"))
            return
        try:
            client = self.community_state_factory(config)
        except Exception as error:
            client = CommunityClientState(False, False, (), f"{type(error).__name__}: {error}")
        state_retry_seconds = max(5.0, float(config.get("community_state_retry_seconds", 30)))
        if not client.available and config.get("community_auto_recover_window", False):
            recovery_interval = max(
                10.0, float(config.get("community_window_recovery_interval_seconds", 60)),
            )
            if time.monotonic() - self._last_client_recovery_at >= recovery_interval:
                self._last_client_recovery_at = time.monotonic()
                try:
                    client = self.launcher_factory(config).recover_window(
                        float(config.get("community_window_recovery_wait_seconds", 15)),
                    )
                except Exception as error:
                    client = CommunityClientState(
                        False, False, (), f"{type(error).__name__}: {error}",
                    )
                if client.available:
                    logging.info(
                        "影刀主窗口已自动恢复；继续处理重试队列：%s | %s",
                        task_id, entry.get("appName", "未知流程"),
                    )
        if not client.available:
            self._defer_for_community_state(task_id, "deferred", "无法确认影刀社区版界面状态，等待后重试", state_retry_seconds)
            self._log_blocked("无法读取影刀社区版界面状态", task_id, entry.get("appName", "未知流程"))
            return
        if client.editing:
            self._defer_for_community_state(task_id, "paused_editing", "检测到影刀正在编辑应用，已暂停自动重试", state_retry_seconds)
            self._log_blocked("影刀正在编辑应用", task_id, entry.get("appName", "未知流程"))
            return
        if client.running:
            self._log_blocked(
                "影刀社区版客户端报告任务运行中",
                client.running_task_id or "任务ID未知",
                client.running_app_name or "流程名未知",
            )
            return
        active = store.active_tasks(config.get("active_task_statuses", [1]))
        if active:
            self._log_blocked("影刀运行中", active[0].uuid, active[0].app_name)
            return
        # The 6.3.x local client state is authoritative for the single runtime
        # lane.  A .tasklog can outlive a finished task while its database row
        # is still uncommitted; letting that stale file override an explicit
        # client-idle state can freeze the retry queue for hours.
        quiet_remaining, activity = self._quiet_remaining(store, config, moment)
        if quiet_remaining > 0:
            def defer_for_schedule(state_to_change: dict[str, Any]) -> None:
                current = state_to_change.setdefault("tasks", {}).get(task_id)
                if current and current.get("status") in {"queued", "deferred"}:
                    current.update({
                        "status": "deferred", "deferredReason": "等待普通定时任务静默窗口",
                        "nextEligibleAt": iso(moment + timedelta(seconds=quiet_remaining)),
                    })
            self._mutate(defer_for_schedule)
            self._log_blocked("定时任务静默窗口", activity.uuid if activity else "未知", activity.app_name if activity else "未知")
            return

        launched_after = moment - timedelta(seconds=float(config.get("retry_launch_match_grace_seconds", 2)))
        known_ids = sorted(store.task_ids_for_app(entry["appId"]))[-int(config.get("retry_known_task_id_limit", 1000)):]
        # Only logs inside the small pre-launch grace window can be mistaken for
        # this request.  Persisting every historical log ID on every attempt
        # would make retry_state.json grow without bound.
        known_log_ids = [item.task_id for item in store.find_new_task_logs(set(), launched_after)]
        request_number = int(entry.get("launchRequests", 0)) + 1
        attempt = {
            "number": request_number,
            "status": "launch_requested",
            "requestedAt": iso(moment),
            "launchedAfter": iso(launched_after),
            "recordDeadlineAt": iso(moment + timedelta(seconds=float(config["task_start_timeout_seconds"]))),
            "knownTaskIds": known_ids,
            "knownLogIds": known_log_ids,
        }

        def reserve_launch(state_to_change: dict[str, Any]) -> bool:
            if self._has_active_retry(state_to_change):
                return False
            current = state_to_change.setdefault("tasks", {}).get(task_id)
            if not current or current.get("status") not in {"queued", "deferred"} or not self._is_due(current, moment):
                return False
            current["attempts"].append(attempt)
            current.update({"status": "launch_requested", "launchRequests": request_number, "lastLaunchRequestedAt": iso(moment)})
            return True

        if not self._mutate(reserve_launch):
            return
        try:
            receipt = self.launcher_factory(config).launch(entry["appId"], entry["appName"])
            receipt_method = getattr(receipt, "method", None)
            receipt_task_id = str(getattr(receipt, "task_id", "") or "")

            def attach_receipt(state_to_change: dict[str, Any]) -> None:
                current = state_to_change.setdefault("tasks", {}).get(task_id)
                if current and current.get("status") == "launch_requested" and current.get("attempts"):
                    active_attempt = current["attempts"][-1]
                    active_attempt["launchMethod"] = receipt_method or "unknown"
                    if receipt_task_id:
                        active_attempt.update({
                            "status": "retry_running",
                            "taskId": receipt_task_id,
                            "acceptedAt": iso(),
                        })
                        current.update({
                            "status": "retry_running",
                            "retryTaskId": receipt_task_id,
                            "actualExecutionCount": int(current.get("actualExecutionCount", 0)) + 1,
                            "runDeadlineAt": iso(now() + timedelta(seconds=float(config["task_run_timeout_seconds"]))),
                        })
            self._mutate(attach_receipt)
            if receipt_task_id:
                logging.info(
                    "影刀社区版已受理重试：%s | 第 %s 次实际执行 | 重试任务=%s",
                    task_id, int(entry.get("executionAttempts", 0)) + 1, receipt_task_id,
                )
            else:
                logging.info("已在社区版桌面客户端点击运行，等待任务日志/数据库确认：%s | 请求=%s | 流程=%s", task_id, request_number, entry["appName"])
        except Exception as error:
            error_text = f"{type(error).__name__}: {error}"
            log_interval = max(60.0, float(config.get("controller_error_log_interval_seconds", 300)))
            should_log = [False]

            def launch_error(state_to_change: dict[str, Any]) -> None:
                current = state_to_change.setdefault("tasks", {}).get(task_id)
                if current and current.get("attempts"):
                    failed_attempt = current["attempts"].pop()
                    recent = current.setdefault("recentControllerErrors", [])
                    recent.append({
                        "number": failed_attempt.get("number"),
                        "requestedAt": failed_attempt.get("requestedAt"),
                        "finishedAt": iso(),
                        "error": error_text,
                    })
                    del recent[:-20]
                    current.update({
                        "status": "deferred",
                        "deferredReason": f"影刀桌面控制暂不可用：{type(error).__name__}",
                        "nextEligibleAt": iso(now() + timedelta(seconds=max(
                            5.0, float(config.get("community_state_retry_seconds", 30)),
                        ))),
                        "controllerLaunchErrors": int(current.get("controllerLaunchErrors", 0)) + 1,
                    })
                    last_log = parse_time(current.get("lastControllerErrorLoggedAt"))
                    previous_reason = current.get("lastControllerErrorReason")
                    if previous_reason != error_text or last_log is None or (now() - last_log).total_seconds() >= log_interval:
                        current.update({"lastControllerErrorLoggedAt": iso(), "lastControllerErrorReason": error_text})
                        should_log[0] = True
            self._mutate(launch_error)
            if should_log[0]:
                logging.error(
                    "影刀社区版桌面启动未完成，失败项仍保留在队列且不计重试次数：%s | %s",
                    task_id, error,
                )

    def _log_blocked(self, reason: str, task_id: str, app_name: str) -> None:
        if time.monotonic() - self._last_block_log_at >= 30:
            logging.info("重试调度暂缓：%s | %s | %s", reason, task_id, app_name)
            self._last_block_log_at = time.monotonic()

    def _original_from_entry(self, task_id: str, entry: dict[str, Any]) -> TaskRecord:
        source = entry.get("original") or {}
        return TaskRecord(
            uuid=task_id, status=3, job_id=source.get("job_id"), job_name=source.get("job_name"),
            app_id=entry["appId"], app_name=entry.get("appName", "未知流程"),
            create_time=source.get("create_time", entry.get("queuedAt", iso())),
            error=source.get("error"), description=source.get("description"), source_kind=int(entry.get("sourceKind", 0)),
        )

    def _complete_analysis(self, task_id: str, analysis: dict[str, Any]) -> None:
        def complete(state: dict[str, Any]) -> None:
            current = state.setdefault("tasks", {}).get(task_id)
            if current and current.get("status") == "exhausted":
                current.update({
                    "analysisStatus": "completed", "analysisCompletedAt": iso(), "analysis": analysis,
                    "notificationStatus": "pending", "nextNotificationAt": iso(),
                })
        self._mutate(complete)

    def dispatch_analysis(self, config: dict[str, Any], store: TaskStore) -> None:
        if not config.get("enable_codex_analysis", False) or (self._analysis_worker and self._analysis_worker.is_alive()):
            return
        snapshot = self.state_store.load().setdefault("tasks", {})
        selected = next(((task_id, entry) for task_id, entry in snapshot.items() if entry.get("status") == "exhausted" and entry.get("analysisStatus") == "pending"), None)
        if not selected:
            return
        task_id, entry = selected

        def reserve(state: dict[str, Any]) -> bool:
            current = state.setdefault("tasks", {}).get(task_id)
            if not current or current.get("analysisStatus") != "pending":
                return False
            current.update({"analysisStatus": "running", "analysisStartedAt": iso(), "analysisWorkerPid": os.getpid()})
            return True
        if not self._mutate(reserve):
            return
        original = self._original_from_entry(task_id, entry)
        attempts = json.loads(json.dumps(entry.get("attempts", []), ensure_ascii=False))
        last_task_id = next((item.get("taskId") for item in reversed(attempts) if item.get("taskId")), task_id)
        log_lines = store.read_log_lines(str(last_task_id))

        def work() -> None:
            try:
                analysis = self.analyzer_factory(config).analyze(original, attempts, log_lines)
            except Exception as error:
                logging.exception("后台失败分析异常：%s", task_id)
                analysis = {
                    "summary": "自动分析未完成。",
                    "likely_cause": original.error or f"分析器异常：{type(error).__name__}",
                    "recommendations": ["检查影刀流程日志和本机重试控制器日志。"],
                }
            self._complete_analysis(task_id, analysis)
            logging.info("后台失败分析完成：%s", task_id)

        self._analysis_worker = threading.Thread(target=work, name="shadowbot-retry-analysis", daemon=True)
        self._analysis_worker.start()

    def dispatch_notification(self, config: dict[str, Any]) -> None:
        if self._notification_worker and self._notification_worker.is_alive():
            return
        moment = now()
        snapshot = self.state_store.load().setdefault("tasks", {})
        selected = next(
            ((task_id, entry) for task_id, entry in snapshot.items()
             if entry.get("status") in {"succeeded", "exhausted"}
             and entry.get("notificationStatus") == "pending" and self._notification_due(entry, moment)),
            None,
        )
        if not selected:
            return
        task_id, entry = selected

        def reserve(state: dict[str, Any]) -> bool:
            current = state.setdefault("tasks", {}).get(task_id)
            if not current or current.get("notificationStatus") != "pending":
                return False
            current.update({"notificationStatus": "running", "notificationStartedAt": iso(), "notificationWorkerPid": os.getpid()})
            return True
        if not self._mutate(reserve):
            return

        def work() -> None:
            succeeded = entry.get("status") == "succeeded"
            if succeeded:
                title = "影刀任务重试成功"
                message = f"> 流程：{entry.get('appName')}\n> {entry.get('finalReason', '已成功')}"
            else:
                analysis = entry.get("analysis", {})
                recommendations = "\n".join(f"- {value}" for value in analysis.get("recommendations", []))
                title = "影刀任务重试未完成"
                message = f"> 流程：{entry.get('appName')}\n> 原因：{analysis.get('likely_cause', entry.get('finalReason', '未知'))}\n{recommendations}"
            try:
                sent = self.notifier_factory().send(title, message)
            except Exception:
                logging.exception("后台通知异常：%s", task_id)
                sent = False
            def complete(state: dict[str, Any]) -> None:
                current = state.setdefault("tasks", {}).get(task_id)
                if not current:
                    return
                if sent:
                    current.update({"notificationStatus": "sent", "notificationSentAt": iso()})
                    return
                failures = int(current.get("notificationFailures", 0)) + 1
                current.update({
                    "notificationStatus": "pending", "notificationFailures": failures,
                    "nextEligibleAt": current.get("nextEligibleAt", iso()),
                    "nextNotificationAt": iso(now() + timedelta(seconds=min(900, 30 * (2 ** min(failures, 5))))),
                })
            self._mutate(complete)

        self._notification_worker = threading.Thread(target=work, name="shadowbot-retry-notification", daemon=True)
        self._notification_worker.start()

    def observe_successful_tasks(self, config: dict[str, Any], store: TaskStore) -> None:
        source_kinds = tuple(int(value) for value in config.get("success_log_source_kinds", [0, 5, 8, 12]))
        successes = store.successful_tasks(source_kinds, int(config.get("success_log_max_rows", 100)))
        newly_observed = self.state_store.mark_successes_observed(
            (task.uuid for task in successes), int(config.get("success_log_history_size", 500))
        )
        if not self._success_observer_initialized:
            self._success_observer_initialized = True
            if not config.get("log_historical_successes_on_start", False):
                logging.info("成功监控基线已建立：已忽略 %s 条既有成功记录，后续新成功会逐条记录。", len(newly_observed))
                return
        for task in reversed(successes):
            if task.uuid in newly_observed:
                logging.info("扫描到影刀任务成功：%s | %s | sourceKind=%s | 创建时间=%s", task.uuid, task.app_name, task.source_kind, task.create_time)

    def prune_state(self, config: dict[str, Any]) -> None:
        keep = max(50, int(config.get("state_max_terminal_tasks", 500)))
        snapshot = self.state_store.load().setdefault("tasks", {})
        controller_history = max(1, int(config.get("controller_attempt_history_size", 5)))
        noisy_statuses = {"launcher_error", "launch_unverified"}
        needs_compaction = any(
            sum(attempt.get("status") in noisy_statuses for attempt in entry.get("attempts", [])) > controller_history
            for entry in snapshot.values()
        )
        if not needs_compaction and sum(entry.get("status") in TERMINAL_STATUSES for entry in snapshot.values()) <= keep:
            return
        def prune(state: dict[str, Any]) -> tuple[int, int]:
            tasks = state.setdefault("tasks", {})
            compacted = 0
            for entry in tasks.values():
                attempts = entry.get("attempts", [])
                noisy = [attempt for attempt in attempts if attempt.get("status") in noisy_statuses]
                if len(noisy) <= controller_history:
                    continue
                keep_numbers = {attempt.get("number") for attempt in noisy[-controller_history:]}
                retained = []
                for attempt in attempts:
                    if attempt.get("status") not in noisy_statuses or attempt.get("number") in keep_numbers:
                        if attempt.get("status") in noisy_statuses:
                            attempt = {key: attempt.get(key) for key in (
                                "number", "status", "requestedAt", "finishedAt", "error", "launchMethod",
                            ) if attempt.get(key) is not None}
                        retained.append(attempt)
                    else:
                        compacted += 1
                entry["attempts"] = retained
            terminal = [(task_id, entry) for task_id, entry in tasks.items() if entry.get("status") in TERMINAL_STATUSES]
            terminal.sort(key=lambda item: item[1].get("completedAt", ""), reverse=True)
            removed = 0
            for task_id, _entry in terminal[keep:]:
                del tasks[task_id]
                removed += 1
            return removed, compacted
        removed, compacted = self._mutate(prune)
        if compacted:
            logging.info("已压缩 %s 条无实际执行的桌面控制历史记录。", compacted)
        if removed:
            logging.info("已清理 %s 条过久的终态重试记录。", removed)

    def record_completed_scan(self, active_task_count: int) -> None:
        with self._scan_health_lock:
            self._last_completed_scan_at = time.monotonic()
            self._last_active_task_count = active_task_count

    def emit_heartbeat(self) -> None:
        config = self.load_config()
        with self._scan_health_lock:
            scan_lag = int(time.monotonic() - self._last_completed_scan_at)
            active = self._last_active_task_count
        try:
            client = self.community_state_factory(config)
        except Exception as error:
            client = CommunityClientState(False, False, (), f"{type(error).__name__}: {error}")
        if client.available:
            if client.running:
                active = max(1, active or 0)
                client_status = f"运行中({client.running_app_name or client.running_task_id or '未知任务'})"
            elif client.editing:
                client_status = "编辑中"
            else:
                client_status = "空闲"
        else:
            client_status = "状态不可读"
        state = self.state_store.load().setdefault("tasks", {})
        counts: dict[str, int] = {}
        for entry in state.values():
            status = str(entry.get("status", "unknown"))
            counts[status] = counts.get(status, 0) + 1
        pending = [(task_id, entry) for task_id, entry in state.items() if entry.get("status") in PENDING_STATUSES]
        oldest = min(pending, key=lambda item: item[1].get("queuedAt", ""), default=(None, {}))
        age = "无"
        queued_at = parse_time(oldest[1].get("queuedAt")) if oldest[0] else None
        if queued_at:
            age = f"{int((now() - queued_at).total_seconds())} 秒"
        active_retry = next(((task_id, entry.get("status")) for task_id, entry in state.items() if entry.get("status") in {"launch_requested", "retry_running"}), None)
        message = (
            "监控心跳：扫描滞后=%s 秒；社区版客户端=%s；影刀运行中=%s；待处理=%s；延后=%s；等待重试结果=%s；"
            "最早等待=%s；当前重试=%s；终态历史=%s。"
        )
        values = (
            scan_lag, client_status, "未知" if active is None else active,
            counts.get("queued", 0), counts.get("deferred", 0) + counts.get("paused_editing", 0),
            active_retry or "无", age, active_retry or "无",
            sum(counts.get(status, 0) for status in TERMINAL_STATUSES),
        )
        if scan_lag > int(config.get("monitor_stall_warning_seconds", 90)):
            logging.warning("监控心跳异常：" + message, *values)
        else:
            logging.info(message, *values)

    def run_heartbeat_loop(self) -> None:
        while not self._monitor_stop.is_set():
            try:
                self.emit_heartbeat()
            except Exception:
                logging.exception("监控心跳异常；将在下一周期继续")
            interval = max(1.0, float(self.load_config().get("monitor_heartbeat_seconds", 60)))
            self._monitor_stop.wait(interval)

    def scan_once(self, config: dict[str, Any]) -> None:
        store = self._store(config)
        self.recover_interrupted_work()
        self.enqueue_new_failures(config, store)
        self.reconcile_attempts(config, store)
        self.schedule_next_launch(config, store)
        self.dispatch_analysis(config, store)
        self.dispatch_notification(config)
        self.observe_successful_tasks(config, store)
        self.prune_state(config)
        active = len(store.active_tasks(config.get("active_task_statuses", [1])))
        self.record_completed_scan(active)

    def run(self) -> None:
        config = self.load_config()
        logging.info("影刀失败重试状态机服务已启动；扫描间隔=%s 秒", config.get("scan_interval_seconds", SCAN_INTERVAL_SECONDS))
        try:
            launcher = self.launcher_factory(config)
            client = launcher.inspect_state()
            if not client.available:
                raise RuntimeError(client.error or "客户端状态不可读")
            logging.info("影刀社区版桌面控制已就绪；不调用影刀 CLI/API，不消耗社区版体验额度")
        except Exception as error:
            logging.error("影刀社区版桌面控制暂不可用；失败任务会保留并持续等待恢复：%s", error)
        heartbeat = threading.Thread(target=self.run_heartbeat_loop, name="shadowbot-retry-heartbeat", daemon=True)
        heartbeat.start()
        try:
            while True:
                try:
                    try:
                        new_config = self.load_config()
                        config = new_config
                    except Exception as e:
                        logging.warning("配置热加载失败（保持原有配置）：%s", e)
                    self.scan_once(config)
                except Exception:
                    logging.exception("失败任务状态机扫描异常；将在下一轮继续")
                time.sleep(max(0.1, float(config.get("scan_interval_seconds", SCAN_INTERVAL_SECONDS))))
        finally:
            self._monitor_stop.set()
            heartbeat.join(timeout=2)


def main() -> int:
    if not acquire_service_lock():
        return 0
    configure_logging()
    atexit.register(release_service_lock)
    try:
        RetryQueueService().run()
    finally:
        release_service_lock()


if __name__ == "__main__":
    raise SystemExit(main())
