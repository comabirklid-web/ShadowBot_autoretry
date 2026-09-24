import json
import logging
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

from retry_controller import (
    CommunityClientState, CommunityTaskStatus, LaunchReceipt, RecentDaysFileHandler,
    ShadowBotLauncher, StateStore, TaskStore,
)
from retry_controller_service import RetryQueueService


SCHEMA = '''
CREATE TABLE tasks (
  uuid TEXT NOT NULL UNIQUE, status INTEGER, jobId TEXT, jobName TEXT,
  appId TEXT, appName TEXT, createTime TEXT, error TEXT, description TEXT, sourceKind INTEGER
)
'''


def task_id(number: int) -> str:
    return f"00000000-0000-0000-0000-{number:012d}"


class FakeNotifier:
    def __init__(self):
        self.messages = []

    def send(self, title, message):
        self.messages.append((title, message))
        return True


class FakeAnalyzer:
    def analyze(self, original, attempts, log_lines):
        return {"summary": "test", "likely_cause": original.error or "unknown", "recommendations": ["inspect"]}


class RaisingAnalyzer:
    def analyze(self, original, attempts, log_lines):
        raise RuntimeError("analysis unavailable")


class RaisingNotifier:
    def send(self, title, message):
        raise RuntimeError("webhook unavailable")


class ControlledLauncher:
    def __init__(self, database_path: Path, outcomes):
        self.database_path = database_path
        self.outcomes = list(outcomes)
        self.launched = []
        self.count = 0

    def launch(self, app_id, app_name=None):
        self.count += 1
        self.launched.append(app_id)
        outcome = self.outcomes.pop(0) if self.outcomes else "no_record"
        if outcome == "no_record":
            return
        status = {"success": 2, "failed": 3, "cancelled": 4, "running": 1}[outcome]
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, ?, '', '', ?, ?, ?, ?, '', 5)",
                (task_id(800000 + self.count), status, app_id, f"Retry {app_id}", datetime.now().isoformat(sep=" "),
                 None if status == 2 else "retry failed"),
            )
            connection.commit()


class VerifiedFailedLauncher:
    def __init__(self, database_path: Path):
        self.database_path = database_path
        self.launched = []

    def launch(self, app_id, app_name=None):
        accepted_id = task_id(910000 + len(self.launched) + 1)
        self.launched.append(app_id)
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, ?, '', '', ?, ?, ?, ?, '', 5)",
                (accepted_id, 3, app_id, f"Retry {app_id}", datetime.now().isoformat(sep=" "), "retry failed"),
            )
            connection.commit()
        return LaunchReceipt("", "community-protocol-unacknowledged")


class DelayedDatabaseLauncher:
    """Simulate Community Edition hiding tasks.db3 until execution finishes."""

    def __init__(self, root: Path):
        self.root = root
        self.database_path = root / "tasks.db3"
        self.launched = []
        self.pending: list[tuple[str, str]] = []

    def launch(self, app_id, app_name=None):
        retry_id = task_id(920000 + len(self.launched) + 1)
        self.launched.append(app_id)
        self.pending.append((retry_id, app_id))
        (self.root / "task_logs" / f"{retry_id}.tasklog").write_text("running", encoding="utf-8")
        return LaunchReceipt("", "community-protocol-unacknowledged")

    def commit(self, status: int, error: str | None = None):
        retry_id, app_id = self.pending.pop(0)
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, ?, '', '', ?, ?, ?, ?, '', 5)",
                (retry_id, status, app_id, f"Retry {app_id}", datetime.now().isoformat(sep=" "), error),
            )
            connection.commit()
        return retry_id


class AcceptedLocalLauncher:
    def __init__(self, statuses=(1,)):
        self.launched = []
        self.statuses = list(statuses)
        self.retry_id = task_id(940001)

    def launch(self, app_id, app_name=None):
        self.launched.append(app_id)
        return LaunchReceipt(self.retry_id, "community-local-cli-accepted")

    def task_status(self, task_id_value):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return CommunityTaskStatus(task_id_value, status, self.launched[-1], "Accepted App", "failed" if status == 3 else None)


class DesktopUiResultLauncher:
    def __init__(self, database_path: Path, status=2):
        self.database_path = database_path
        self.status = status
        self.launched = []

    def launch(self, app_id, app_name=None):
        self.launched.append((app_id, app_name))
        retry_id = task_id(950000 + len(self.launched))
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, ?, '', '', ?, ?, ?, ?, '', 2)",
                (retry_id, self.status, app_id, app_name, datetime.now().isoformat(sep=" "),
                 None if self.status == 2 else "desktop retry failed"),
            )
            connection.commit()
        return LaunchReceipt("", "community-desktop-ui-unacknowledged")


class RetryStateMachineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "tasks.db3"
        (self.root / "task_logs").mkdir()
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(SCHEMA)
            connection.commit()
        self.config = {
            "user_folder": str(self.root), "shadowbot_exe": "unused", "codex_exe": "unused", "codex_workdir": str(self.root),
            "retry_delays_seconds": [30, 120, 300], "task_start_timeout_seconds": 0, "task_run_timeout_seconds": 60,
            "poll_interval_seconds": 0, "scan_interval_seconds": 1, "trigger_max_age_seconds": 86400,
            "trigger_source_kinds": [0], "retry_result_source_kinds": [5, 12], "active_task_statuses": [1],
            "scheduled_task_quiet_seconds": 0, "scheduled_activity_source_kinds": [0],
            "retry_launch_match_grace_seconds": 2, "retry_known_task_id_limit": 100,
            "uncommitted_task_log_max_age_seconds": 21600,
            "seen_failure_history_size": 1000, "state_max_terminal_tasks": 500,
            "enable_codex_analysis": False, "success_log_source_kinds": [0, 5, 12],
            "success_log_max_rows": 100, "success_log_history_size": 100,
            "log_historical_successes_on_start": False, "monitor_heartbeat_seconds": 60,
            "monitor_stall_warning_seconds": 90,
        }

    def tearDown(self):
        # Background analysis/notification workers are daemon threads in the
        # service.  Wait for test-created workers before deleting their temp
        # state directory, otherwise a valid late state write looks like an
        # unrelated FileNotFoundError in the test report.
        for thread in threading.enumerate():
            if thread.name.startswith("shadowbot-retry-"):
                thread.join(timeout=2)
        self.temp.cleanup()

    def insert_task(self, number, app_id, status=3, source_kind=0, created_at=None, error="original failure"):
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, ?, '', '', ?, ?, ?, ?, '', ?)",
                (task_id(number), status, app_id, f"App {app_id}", (created_at or datetime.now()).isoformat(sep=" "), error, source_kind),
            )
            connection.commit()
        return task_id(number)

    def service(self, launcher, community_state=None):
        notifier = FakeNotifier()
        return RetryQueueService(
            config_loader=lambda: self.config, task_store_factory=TaskStore,
            state_store=StateStore(self.root / "state.json"), launcher_factory=lambda _config: launcher,
            notifier_factory=lambda: notifier,
            community_state_factory=(lambda _config: community_state)
            if community_state is not None else (lambda _config: CommunityClientState(True, False, ("home",))),
        ), notifier

    def test_three_failures_are_enqueued_and_processed_in_order(self):
        base = datetime.now() - timedelta(seconds=10)
        apps = ["app-a", "app-b", "app-c"]
        for index, app in enumerate(apps, start=1):
            self.insert_task(index, app, created_at=base + timedelta(seconds=index))
        launcher = ControlledLauncher(self.db, ["success", "success", "success"])
        service, _ = self.service(launcher)

        for _ in range(7):
            service.scan_once(self.config)

        self.assertEqual(launcher.launched, apps)
        entries = service.state_store.load()["tasks"]
        self.assertEqual([entries[task_id(index)]["status"] for index in range(1, 4)], ["succeeded"] * 3)

    def test_missing_retry_record_defers_first_item_and_allows_next_item(self):
        first = self.insert_task(1, "app-a")
        second = self.insert_task(2, "app-b", created_at=datetime.now() + timedelta(milliseconds=1))
        launcher = ControlledLauncher(self.db, ["no_record", "success"])
        service, _ = self.service(launcher)

        service.scan_once(self.config)  # launch A
        service.scan_once(self.config)  # A has no record; defer A and launch B

        entries = service.state_store.load()["tasks"]
        self.assertEqual(entries[first]["status"], "deferred")
        self.assertEqual(entries[first]["executionAttempts"], 0)
        self.assertEqual(launcher.launched, ["app-a", "app-b"])
        self.assertEqual(entries[second]["status"], "launch_requested")

    def test_unverified_request_stays_queued_and_releases_lane_for_next_item(self):
        first = self.insert_task(1, "app-a")
        second = self.insert_task(2, "app-b", created_at=datetime.now() + timedelta(milliseconds=1))
        launcher = ControlledLauncher(self.db, ["no_record", "success"])
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({"task_start_timeout_seconds": 0})

        service.scan_once(config)
        service.scan_once(config)

        entries = service.state_store.load()["tasks"]
        self.assertEqual(entries[first]["status"], "deferred")
        self.assertEqual(entries[first]["executionAttempts"], 0)
        self.assertEqual(entries[first]["controllerLaunchErrors"], 1)
        self.assertEqual(entries[second]["status"], "launch_requested")
        self.assertEqual(launcher.launched, ["app-a", "app-b"])

    def test_manual_replay_priority_overtakes_historical_backlog(self):
        older = self.insert_task(1, "old-app", created_at=datetime.now() - timedelta(hours=2))
        requested = self.insert_task(2, "today-app", created_at=datetime.now())
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher)

        service.enqueue_new_failures(self.config, TaskStore(self.root))
        service._mutate(lambda state: state["tasks"][requested].update({"manualPriority": 100}))
        service.schedule_next_launch(self.config, TaskStore(self.root))

        self.assertEqual(launcher.launched, ["today-app"])
        self.assertEqual(service.state_store.load()["tasks"][older]["status"], "queued")

    def test_recent_scheduled_activity_defers_without_consuming_attempt(self):
        original = self.insert_task(1, "app-a")
        config = dict(self.config)
        config["scheduled_task_quiet_seconds"] = 60
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher)

        service.scan_once(config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "deferred")
        self.assertEqual(entry["attempts"], [])
        self.assertEqual(launcher.launched, [])

    def test_real_retry_failure_is_counted_only_after_a_retry_record_exists(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["failed"])
        service, _ = self.service(launcher)

        service.scan_once(self.config)
        service.scan_once(self.config)
        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "deferred")
        self.assertEqual(entry["executionAttempts"], 1)
        self.assertEqual(entry["attempts"][0]["status"], "failed")

    def test_long_retry_log_prevents_false_launch_timeout_until_database_commit(self):
        original = self.insert_task(1, "app-a")
        launcher = DelayedDatabaseLauncher(self.root)
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({"task_start_timeout_seconds": 0})

        service.scan_once(config)  # protocol request creates tasklog, DB row remains invisible
        service.scan_once(config)  # deadline passed, tasklog must keep the attempt alive

        waiting = service.state_store.load()["tasks"][original]
        self.assertEqual(waiting["status"], "launch_requested")
        self.assertEqual(waiting["actualExecutionCount"], 0)
        self.assertTrue(waiting["attempts"][0]["provisionalTaskLogIds"])

        retry_id = launcher.commit(3, "retry failed")
        service.scan_once(config)  # bind the now-visible sourceKind=5 record
        bound = service.state_store.load()["tasks"][original]
        self.assertEqual(bound["status"], "retry_running")
        self.assertEqual(bound["retryTaskId"], retry_id)
        self.assertEqual(bound["actualExecutionCount"], 1)

        service.scan_once(config)  # verified failure schedules the second attempt
        failed = service.state_store.load()["tasks"][original]
        self.assertEqual(failed["status"], "deferred")
        self.assertEqual(failed["executionAttempts"], 1)

    def test_stale_uncommitted_log_with_idle_client_is_requeued_and_releases_lane(self):
        first = self.insert_task(1, "app-a")
        second = self.insert_task(2, "app-b", created_at=datetime.now() + timedelta(milliseconds=1))
        launcher = DelayedDatabaseLauncher(self.root)
        service, notifier = self.service(
            launcher, CommunityClientState(True, False, ("home",), running=False),
        )
        config = dict(self.config)
        config.update({
            "task_start_timeout_seconds": 0,
            "uncommitted_task_log_max_age_seconds": 0,
        })

        service.scan_once(config)  # launch A and create its provisional log
        service.scan_once(config)  # idle client + stale log => requeue A, launch B

        entries = service.state_store.load()["tasks"]
        self.assertEqual(entries[first]["status"], "deferred")
        self.assertEqual(entries[first]["attempts"][0]["status"], "launch_unverified_stopped")
        self.assertEqual(entries[first]["executionAttempts"], 0)
        self.assertNotIn("notificationStatus", entries[first])
        self.assertEqual(entries[second]["status"], "launch_requested")
        self.assertEqual(launcher.launched, ["app-a", "app-b"])
        self.assertEqual(notifier.messages, [])

    def test_stale_uncommitted_log_keeps_waiting_while_runner_is_visible(self):
        original = self.insert_task(1, "app-a")
        launcher = DelayedDatabaseLauncher(self.root)
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({
            "task_start_timeout_seconds": 0,
            "uncommitted_task_log_max_age_seconds": 0,
        })

        service.scan_once(config)
        service.community_state_factory = lambda _config: CommunityClientState(
            True, False, ("runner",), running=True,
        )
        service.scan_once(config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "launch_requested")
        self.assertEqual(entry["executionAttempts"], 0)

    def test_delayed_database_failure_releases_lane_for_next_queued_item(self):
        first = self.insert_task(1, "app-a")
        second = self.insert_task(2, "app-b", created_at=datetime.now() + timedelta(milliseconds=1))
        launcher = DelayedDatabaseLauncher(self.root)
        service, _ = self.service(launcher)
        config = dict(self.config)
        config["task_start_timeout_seconds"] = 0

        service.scan_once(config)  # launch A
        service.scan_once(config)  # A runs with tasklog evidence and hidden DB row
        launcher.commit(3, "retry failed")
        service.scan_once(config)  # bind A
        service.scan_once(config)  # A enters backoff; B immediately receives the lane

        entries = service.state_store.load()["tasks"]
        self.assertEqual(entries[first]["status"], "deferred")
        self.assertEqual(entries[first]["executionAttempts"], 1)
        self.assertEqual(entries[second]["status"], "launch_requested")
        self.assertEqual(launcher.launched, ["app-a", "app-b"])

    def test_client_idle_state_overrides_stale_uncommitted_normal_task_log(self):
        original = self.insert_task(1, "app-a")
        normal_task_id = task_id(930001)
        (self.root / "task_logs" / f"{normal_task_id}.tasklog").write_text("normal task running", encoding="utf-8")
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher)

        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "launch_requested")
        self.assertEqual(launcher.launched, ["app-a"])

    def test_normal_task_collision_requeues_without_consuming_execution_attempt(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["no_record"])
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({
            "task_start_timeout_seconds": 0,
            "scheduled_task_quiet_seconds": 0,
        })

        service.scan_once(config)
        unrelated_id = task_id(930002)
        (self.root / "task_logs" / f"{unrelated_id}.tasklog").write_text("scheduled task", encoding="utf-8")
        service.scan_once(config)
        self.assertEqual(service.state_store.load()["tasks"][original]["status"], "launch_requested")

        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, 2, '', '', 'app-b', 'App B', ?, NULL, '', 0)",
                (unrelated_id, datetime.now().isoformat(sep=" ")),
            )
            connection.commit()
        service.scan_once(config)
        service.scan_once(config)  # finish collision grace and send a fresh request

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "launch_requested")
        self.assertEqual(entry["actualExecutionCount"], 0)
        self.assertEqual(entry["executionAttempts"], 0)
        self.assertEqual(entry["attempts"][0]["status"], "displaced_by_external_task")
        self.assertEqual(launcher.launched, ["app-a", "app-a"])

    def test_repeated_unverified_requests_never_abandon_the_failed_task(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["no_record", "no_record", "no_record"])
        service, notifier = self.service(launcher)
        config = dict(self.config)
        config.update({
            "task_start_timeout_seconds": 0,
            "community_state_retry_seconds": 5,
        })

        for _ in range(6):
            service.scan_once(config)
            service._mutate(lambda state: state["tasks"][original].update({"nextEligibleAt": datetime.now().isoformat()}))

        entry = service.state_store.load()["tasks"][original]
        self.assertIn(entry["status"], {"deferred", "launch_requested"})
        self.assertGreaterEqual(entry["controllerLaunchErrors"], 2)
        self.assertEqual(entry["executionAttempts"], 0)
        self.assertEqual(launcher.launched, ["app-a", "app-a", "app-a"])
        self.assertEqual(notifier.messages, [])

    def test_unverified_request_uses_controller_backoff_before_retrying(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["no_record", "no_record"])
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({
            "task_start_timeout_seconds": 0,
            "community_state_retry_seconds": 60,
        })

        service.scan_once(config)
        service.scan_once(config)
        self.assertEqual(launcher.launched, ["app-a"])
        service._mutate(lambda state: state["tasks"][original].update({
            "nextEligibleAt": (datetime.now() - timedelta(seconds=1)).isoformat(),
        }))
        service.scan_once(config)

        self.assertEqual(launcher.launched, ["app-a", "app-a"])
        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "launch_requested")
        self.assertEqual(entry["controllerLaunchErrors"], 1)

    def test_manual_same_app_run_stops_automation_without_claiming_success(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["no_record"])
        service, notifier = self.service(launcher)
        config = dict(self.config)
        config.update({"task_start_timeout_seconds": 0, "scheduled_task_quiet_seconds": 0})

        service.scan_once(config)
        manual_id = task_id(930003)
        (self.root / "task_logs" / f"{manual_id}.tasklog").write_text("manual run", encoding="utf-8")
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(
                "INSERT INTO tasks VALUES (?, 2, '', '', 'app-a', 'App A', ?, NULL, '', 2)",
                (manual_id, datetime.now().isoformat(sep=" ")),
            )
            connection.commit()

        service.scan_once(config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "superseded")
        self.assertEqual(entry["attempts"][0]["status"], "manual_takeover")
        self.assertEqual(entry["notificationStatus"], "suppressed")
        self.assertEqual(launcher.launched, ["app-a"])
        self.assertEqual(notifier.messages, [])

    def test_running_timeout_never_counts_as_verified_failure(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["running"])
        service, _ = self.service(launcher)
        config = dict(self.config)
        config["task_run_timeout_seconds"] = 0

        service.scan_once(config)
        service.scan_once(config)  # associate status=1 record
        service.scan_once(config)  # exceed deadline, but result is still unknown

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "retry_running")
        self.assertEqual(entry["executionAttempts"], 0)
        self.assertEqual(entry["attempts"][0]["status"], "retry_running")

    def test_cancelled_retry_is_final_without_another_launch_or_notification(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["cancelled"])
        service, notifier = self.service(launcher)

        service.scan_once(self.config)
        service.scan_once(self.config)
        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "cancelled")
        self.assertEqual(entry["attempts"][0]["status"], "cancelled")
        self.assertEqual(entry["notificationStatus"], "suppressed")
        self.assertEqual(launcher.launched, ["app-a"])
        self.assertEqual(notifier.messages, [])

    def test_registered_protocol_can_make_three_verified_execution_attempts(self):
        original = self.insert_task(1, "app-a")
        launcher = VerifiedFailedLauncher(self.db)
        service, _ = self.service(launcher)
        config = dict(self.config)
        config["retry_delays_seconds"] = [0, 0, 0]

        for _ in range(9):
            service.scan_once(config)
        if service._notification_worker:
            service._notification_worker.join(timeout=2)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "exhausted")
        self.assertEqual(entry["executionAttempts"], 3)
        self.assertEqual(entry["actualExecutionCount"], 3)
        self.assertEqual(launcher.launched, ["app-a", "app-a", "app-a"])

    def test_community_launcher_uses_desktop_ui_and_never_calls_shadowbot_cli(self):
        launcher = ShadowBotLauncher(Path("C:/unused/ShadowBot.exe"))
        desktop = object()
        main_window = object()
        with patch.object(launcher, "inspect_state", return_value=CommunityClientState(True, False, ("home",))), patch.object(
            ShadowBotLauncher, "_desktop", return_value=desktop,
        ), patch.object(ShadowBotLauncher, "_top_windows", return_value=[main_window]), patch.object(
            ShadowBotLauncher, "_main_windows", return_value=[main_window],
        ), patch.object(ShadowBotLauncher, "_dismiss_terminal_runners") as dismiss, patch.object(
            ShadowBotLauncher, "_ensure_apps_view",
        ) as ensure_apps, patch.object(
            ShadowBotLauncher, "_select_and_invoke",
        ) as invoke, patch.object(
            ShadowBotLauncher, "_confirm_parameter_dialog_if_present", side_effect=[False, False],
        ) as confirm, patch("retry_controller.subprocess.run") as run:
            receipt = launcher.launch("00000000-0000-0000-0000-000000000123", "Exact App")

        dismiss.assert_called_once_with([main_window])
        ensure_apps.assert_called_once_with(main_window)
        invoke.assert_called_once_with(main_window, "Exact App")
        self.assertEqual(confirm.call_count, 2)
        run.assert_not_called()
        self.assertEqual(receipt.task_id, "")
        self.assertEqual(receipt.method, "community-desktop-ui-unacknowledged")

    def test_offscreen_application_is_found_through_unique_search_box(self):
        main_window = MagicMock()
        initial_box = MagicMock()
        filtered_box = MagicMock()
        search_box = MagicMock()
        target = MagicMock()
        run_button = MagicMock()
        initial_box.descendants.return_value = []
        filtered_box.descendants.return_value = [target]
        target.is_selected.return_value = True
        run_button.is_visible.return_value = True
        run_button.is_enabled.return_value = True

        def descendants(control_type=None):
            if control_type == "Edit":
                return [search_box]
            if control_type == "Button":
                return [run_button]
            return []

        main_window.descendants.side_effect = descendants
        search_box.is_visible.return_value = True
        search_box.is_enabled.return_value = True
        with patch.object(
            ShadowBotLauncher, "_application_list_boxes", side_effect=[[initial_box], [filtered_box]],
        ), patch.object(
            ShadowBotLauncher, "_application_name", return_value="Exact Offscreen App",
        ), patch.object(
            ShadowBotLauncher, "_text", side_effect=lambda item: "运行" if item is run_button else "",
        ):
            ShadowBotLauncher._select_and_invoke(main_window, "Exact Offscreen App")

        search_box.set_edit_text.assert_called_once_with("Exact Offscreen App")
        target.select.assert_called_once()
        run_button.click_input.assert_called_once()

    def test_existing_parameter_dialog_is_confirmed_without_clicking_run_again(self):
        launcher = ShadowBotLauncher(Path("C:/unused/ShadowBot.exe"))
        main_window = object()
        with patch.object(launcher, "inspect_state", return_value=CommunityClientState(True, False, ("home",))), patch.object(
            ShadowBotLauncher, "_desktop", return_value=object(),
        ), patch.object(ShadowBotLauncher, "_top_windows", return_value=[main_window]), patch.object(
            ShadowBotLauncher, "_main_windows", return_value=[main_window],
        ), patch.object(ShadowBotLauncher, "_dismiss_terminal_runners"), patch.object(
            ShadowBotLauncher, "_confirm_parameter_dialog_if_present", return_value=True,
        ) as confirm, patch.object(ShadowBotLauncher, "_select_and_invoke") as run_click:
            receipt = launcher.launch("app-a", "Exact App")

        confirm.assert_called_once_with(main_window)
        run_click.assert_not_called()
        self.assertEqual(receipt.method, "community-desktop-ui-unacknowledged")

    def test_shadowbot_main_window_ignores_explorer_folder_with_same_title(self):
        def fake_window(process_id):
            element = type("Element", (), {"process_id": process_id})()
            return type("Window", (), {"window_text": lambda self: "影刀", "element_info": element})()

        shadowbot = fake_window(100)
        explorer = fake_window(200)
        with patch.object(ShadowBotLauncher, "_process_executable", side_effect={
            100: r"C:\Program Files\ShadowBot\ShadowBot.Shell.exe",
            200: r"C:\Windows\explorer.exe",
        }.__getitem__):
            found = ShadowBotLauncher._main_windows([shadowbot, explorer])

        self.assertEqual(found, [shadowbot])

    def test_shadowbot_6312_runner_is_recognized_by_title_and_verified_process(self):
        def fake_window(process_id, title, class_name="Window"):
            element = type("Element", (), {
                "process_id": process_id,
                "class_name": class_name,
            })()
            return type("Window", (), {
                "window_text": lambda self: title,
                "element_info": element,
            })()

        runner = fake_window(100, "RobotRunnerView")
        impostor = fake_window(200, "RobotRunnerView")
        with patch.object(ShadowBotLauncher, "_process_executable", side_effect={
            100: r"C:\Program Files\ShadowBot\shadowbot-6.3.12\ShadowBot.Shell.exe",
            200: r"C:\Windows\explorer.exe",
        }.__getitem__):
            found = ShadowBotLauncher._visible_runners([runner, impostor])

        self.assertEqual(found, [runner])

    def test_recover_window_activates_existing_community_client(self):
        executable = self.root / "ShadowBot.exe"
        executable.write_bytes(b"")
        launcher = ShadowBotLauncher(executable, timeout_seconds=1)
        unavailable = CommunityClientState(False, False, (), "hidden")
        available = CommunityClientState(True, False, ("home",))
        with patch.object(
            launcher, "inspect_state", side_effect=[unavailable, available],
        ), patch("retry_controller.subprocess.Popen") as popen:
            state = launcher.recover_window(wait_seconds=1)

        self.assertTrue(state.available)
        popen.assert_called_once()
        self.assertEqual(popen.call_args.kwargs["cwd"], str(executable.parent))

    def test_visible_runner_is_running_evidence_when_main_window_is_hidden(self):
        launcher = ShadowBotLauncher(Path("C:/unused/ShadowBot.exe"))
        runner = object()
        with patch.object(ShadowBotLauncher, "_desktop", return_value=object()), patch.object(
            ShadowBotLauncher, "_top_windows", return_value=[runner],
        ), patch.object(ShadowBotLauncher, "_main_windows", return_value=[]), patch.object(
            ShadowBotLauncher, "_visible_runners", return_value=[runner],
        ), patch.object(ShadowBotLauncher, "_runner_is_terminal", return_value=False), patch.object(
            ShadowBotLauncher, "_text", side_effect=lambda item: "Active Flow" if item is runner else "",
        ):
            state = launcher.inspect_state()

        self.assertTrue(state.available)
        self.assertTrue(state.running)
        self.assertEqual(state.running_app_name, "Active Flow")

    def test_terminal_runner_without_main_is_recoverable_idle_state(self):
        launcher = ShadowBotLauncher(Path("C:/unused/ShadowBot.exe"))
        runner = object()
        with patch.object(ShadowBotLauncher, "_desktop", return_value=object()), patch.object(
            ShadowBotLauncher, "_top_windows", return_value=[runner],
        ), patch.object(ShadowBotLauncher, "_main_windows", return_value=[]), patch.object(
            ShadowBotLauncher, "_visible_runners", return_value=[runner],
        ), patch.object(ShadowBotLauncher, "_runner_is_terminal", return_value=True), patch.object(
            ShadowBotLauncher, "_text", return_value="运行成功",
        ):
            state = launcher.inspect_state()

        self.assertTrue(state.available)
        self.assertFalse(state.running)

    def test_launch_closes_terminal_runner_before_requiring_main_window(self):
        launcher = ShadowBotLauncher(Path("C:/unused/ShadowBot.exe"))
        desktop = object()
        terminal_runner = object()
        main_window = object()
        with patch.object(
            launcher, "inspect_state", return_value=CommunityClientState(True, False, ("terminal",)),
        ), patch.object(ShadowBotLauncher, "_desktop", return_value=desktop), patch.object(
            ShadowBotLauncher, "_top_windows", side_effect=[[terminal_runner], [main_window]],
        ), patch.object(
            ShadowBotLauncher, "_main_windows", return_value=[main_window],
        ), patch.object(
            ShadowBotLauncher, "_dismiss_terminal_runners",
        ) as dismiss, patch.object(
            ShadowBotLauncher, "_confirm_parameter_dialog_if_present", return_value=True,
        ):
            receipt = launcher.launch("app-a", "Exact App")

        dismiss.assert_called_once_with([terminal_runner])
        self.assertEqual(receipt.method, "community-desktop-ui-unacknowledged")

    def test_desktop_ui_source_kind_two_is_verified_as_automatic_retry(self):
        original = self.insert_task(1, "app-a")
        launcher = DesktopUiResultLauncher(self.db, status=2)
        service, _ = self.service(launcher)

        service.scan_once(self.config)
        service.scan_once(self.config)
        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "succeeded")
        self.assertEqual(entry["actualExecutionCount"], 1)
        self.assertIn("第 1 次实际重试成功", entry["finalReason"])
        self.assertEqual(launcher.launched, [("app-a", "App app-a")])

    def test_accepted_local_task_id_enters_running_state_immediately(self):
        original = self.insert_task(1, "app-a")
        launcher = AcceptedLocalLauncher((1,))
        service, _ = self.service(launcher)

        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "retry_running")
        self.assertEqual(entry["retryTaskId"], launcher.retry_id)
        self.assertEqual(entry["actualExecutionCount"], 1)
        self.assertEqual(entry["executionAttempts"], 0)

    def test_accepted_legacy_task_without_database_result_is_not_guessed_failed(self):
        original = self.insert_task(1, "app-a")
        launcher = AcceptedLocalLauncher((3,))
        service, _ = self.service(launcher)

        service.scan_once(self.config)
        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "retry_running")
        self.assertEqual(entry["actualExecutionCount"], 1)
        self.assertEqual(entry["executionAttempts"], 0)

    def test_editing_failure_pauses_without_any_protocol_launch(self):
        original = self.insert_task(1, "app-a", error="运行时应用正在编辑，流程执行失败")
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher, CommunityClientState(True, True, ("studio",)))

        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "paused_editing")
        self.assertEqual(launcher.launched, [])
        self.assertEqual(entry["launchRequests"], 0)

    def test_editing_failure_resumes_once_after_editor_is_confirmed_closed(self):
        original = self.insert_task(1, "app-a", error="运行时应用正在编辑，流程执行失败")
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher, CommunityClientState(True, False, ("home",)))
        config = dict(self.config)
        config.update({"community_initial_quiet_seconds": 0, "editing_clear_stable_seconds": 0})

        service.scan_once(config)  # historical editing error enters the pause
        service._mutate(lambda state: state["tasks"][original].update({"nextEligibleAt": datetime.now().isoformat()}))
        service.scan_once(config)  # client is clear; write editingClearSince
        service.scan_once(config)  # write editingResumedAt and resume queue
        service.scan_once(config)  # resume marker permits the actual launch

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "launch_requested")
        self.assertTrue(entry.get("editingResumedAt"))
        self.assertEqual(launcher.launched, ["app-a"])

    def test_normal_task_quiet_window_keeps_failed_task_queued(self):
        base = datetime.now()
        original = self.insert_task(1, "app-a", created_at=base)
        self.insert_task(2, "app-b", status=2, created_at=base + timedelta(seconds=1), error=None)
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({"scheduled_task_quiet_seconds": 300, "community_initial_quiet_seconds": 0})

        service.scan_once(config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "deferred")
        self.assertEqual(entry["launchRequests"], 0)
        self.assertIn("静默窗口", entry["deferredReason"])

    def test_quiet_window_uses_task_log_finish_time_not_old_create_time(self):
        base = datetime.now() - timedelta(minutes=10)
        original = self.insert_task(1, "app-a", created_at=base)
        normal = self.insert_task(
            2, "app-b", status=2, created_at=base + timedelta(minutes=1), error=None,
        )
        (self.root / "task_logs" / f"{normal}.tasklog").write_text("just finished", encoding="utf-8")
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher)
        config = dict(self.config)
        config.update({"scheduled_task_quiet_seconds": 60, "community_initial_quiet_seconds": 0})

        service.scan_once(config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "deferred")
        self.assertEqual(launcher.launched, [])
        self.assertIn("静默窗口", entry["deferredReason"])

    def test_editor_reappearance_resets_the_thirty_second_clear_window(self):
        original = self.insert_task(1, "app-a", error="运行时应用正在编辑，流程执行失败")
        service, _ = self.service(ControlledLauncher(self.db, []))
        service.enqueue_new_failures(self.config, TaskStore(self.root))
        service._mutate(lambda state: state["tasks"][original].update({
            "status": "paused_editing",
            "editingClearSince": (datetime.now() - timedelta(seconds=60)).isoformat(),
        }))

        service._defer_for_community_state(
            original, "paused_editing", "检测到影刀正在编辑应用，已暂停自动重试", 30,
        )

        entry = service.state_store.load()["tasks"][original]
        self.assertNotIn("editingClearSince", entry)
        self.assertEqual(entry["status"], "paused_editing")

    def test_community_state_unavailable_never_opens_run_protocol(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["success"])
        service, _ = self.service(launcher, CommunityClientState(False, False, (), "debug port unavailable"))

        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "deferred")
        self.assertEqual(entry["launchRequests"], 0)
        self.assertEqual(launcher.launched, [])

    def test_unavailable_client_is_recovered_before_launch(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["success"])
        recovered = []

        def recover_window(wait_seconds):
            recovered.append(wait_seconds)
            return CommunityClientState(True, False, ("home",))

        launcher.recover_window = recover_window
        service, _ = self.service(
            launcher, CommunityClientState(False, False, (), "main window hidden"),
        )
        config = dict(self.config)
        config.update({
            "community_auto_recover_window": True,
            "community_window_recovery_interval_seconds": 60,
            "community_window_recovery_wait_seconds": 3,
        })

        service.scan_once(config)

        self.assertEqual(recovered, [3.0])
        self.assertEqual(launcher.launched, ["app-a"])
        self.assertEqual(service.state_store.load()["tasks"][original]["status"], "launch_requested")

    def test_client_reported_running_task_blocks_launch_even_when_database_is_delayed(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["success"])
        client = CommunityClientState(
            True, False, ("console",), running=True,
            running_task_id=task_id(950001), running_app_name="Normal scheduled app",
        )
        service, _ = self.service(launcher, client)

        service.scan_once(self.config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "queued")
        self.assertEqual(launcher.launched, [])

    def test_unverified_community_launch_is_retained_without_false_failure_notice(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["no_record"])
        service, notifier = self.service(launcher)
        config = dict(self.config)
        config.update({
            "task_start_timeout_seconds": 0,
            "enable_codex_analysis": True,
        })

        service.scan_once(config)
        service.scan_once(config)

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "deferred")
        self.assertEqual(entry["actualExecutionCount"], 0)
        self.assertEqual(entry["executionAttempts"], 0)
        self.assertNotIn("notificationStatus", entry)
        self.assertEqual(notifier.messages, [])

    def test_stale_original_failure_is_superseded(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, [])
        service, _ = self.service(launcher)
        service.enqueue_new_failures(self.config, TaskStore(self.root))
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute("UPDATE tasks SET status = 2 WHERE uuid = ?", (original,))
            connection.commit()

        service.schedule_next_launch(self.config, TaskStore(self.root))

        entry = service.state_store.load()["tasks"][original]
        self.assertEqual(entry["status"], "superseded")
        self.assertEqual(launcher.launched, [])

    def test_more_than_fifty_burst_failures_are_all_enqueued(self):
        for index in range(60):
            self.insert_task(index + 1, f"app-{index}")
        service, _ = self.service(ControlledLauncher(self.db, []))

        count = service.enqueue_new_failures(self.config, TaskStore(self.root))

        self.assertEqual(count, 60)
        self.assertEqual(len(service.state_store.load()["tasks"]), 60)

    def test_manual_run_never_matches_automatic_retry_result(self):
        app = "app-a"
        self.insert_task(1, app, status=2, source_kind=2)
        automatic = self.insert_task(2, app, status=2, source_kind=5)
        retry = TaskStore(self.root).find_new_retry_task(app, set(), [5])
        self.assertEqual(retry.uuid, automatic)

    def test_local_cli_source_kind_twelve_is_a_retry_result_not_a_new_trigger(self):
        app = "app-a"
        cli_task = self.insert_task(1, app, status=3, source_kind=12)
        store = TaskStore(self.root)

        retry = store.find_new_retry_task(app, set(), [5, 12])
        triggers = store.trigger_failures(3600, set(), [0, 8])

        self.assertEqual(retry.uuid, cli_task)
        self.assertEqual(triggers, [])

    def test_terminal_history_is_bounded_without_losing_seen_ids(self):
        state = StateStore(self.root / "state.json")
        entries = {
            task_id(index): {"status": "succeeded", "completedAt": (datetime.now() - timedelta(seconds=index)).isoformat()}
            for index in range(70)
        }
        state.save({"tasks": entries, "seenFailureIds": list(entries)})
        config = dict(self.config)
        config["state_max_terminal_tasks"] = 50
        service, _ = self.service(ControlledLauncher(self.db, []))

        service.prune_state(config)

        self.assertEqual(len(state.load()["tasks"]), 50)

    def test_corrupt_state_is_never_replaced_with_an_empty_queue(self):
        state = StateStore(self.root / "state.json")
        state.path.write_text("{not valid json", encoding="utf-8")

        with self.assertRaisesRegex(RuntimeError, "状态文件无法读取"):
            state.mutate(lambda value: value.setdefault("tasks", {}))

        self.assertEqual(state.path.read_text(encoding="utf-8"), "{not valid json")

    def test_analysis_and_notification_are_recoverable_background_work(self):
        original = self.insert_task(1, "app-a")
        state = StateStore(self.root / "state.json")
        state.save({"tasks": {
            original: {
                "status": "exhausted", "appId": "app-a", "appName": "App app-a", "sourceKind": 0,
                "original": {"uuid": original, "app_id": "app-a", "app_name": "App app-a", "error": "failed", "source_kind": 0},
                "attempts": [], "analysisStatus": "pending", "notificationStatus": "waiting_analysis",
            }
        }})
        notifier = FakeNotifier()
        config = dict(self.config)
        config["enable_codex_analysis"] = True
        service = RetryQueueService(
            config_loader=lambda: config, task_store_factory=TaskStore, state_store=state,
            launcher_factory=lambda _config: ControlledLauncher(self.db, []), notifier_factory=lambda: notifier,
            analyzer_factory=lambda _config: FakeAnalyzer(),
        )

        service.dispatch_analysis(config, TaskStore(self.root))
        service._analysis_worker.join(timeout=2)
        after_analysis = state.load()["tasks"][original]
        self.assertEqual(after_analysis["analysisStatus"], "completed")
        self.assertEqual(after_analysis["notificationStatus"], "pending")

        service.dispatch_notification(self.config)
        service._notification_worker.join(timeout=2)
        self.assertEqual(state.load()["tasks"][original]["notificationStatus"], "sent")
        self.assertEqual(len(notifier.messages), 1)

    def test_background_failures_return_to_durable_state(self):
        original = self.insert_task(1, "app-a")
        state = StateStore(self.root / "state.json")
        state.save({"tasks": {
            original: {
                "status": "exhausted", "appId": "app-a", "appName": "App app-a", "sourceKind": 0,
                "original": {"uuid": original, "app_id": "app-a", "app_name": "App app-a", "error": "failed", "source_kind": 0},
                "attempts": [], "analysisStatus": "pending", "notificationStatus": "waiting_analysis",
            },
            "success": {"status": "succeeded", "appId": "app-b", "appName": "App app-b", "notificationStatus": "pending"},
        }})
        config = dict(self.config)
        config["enable_codex_analysis"] = True
        service = RetryQueueService(
            config_loader=lambda: config, task_store_factory=TaskStore, state_store=state,
            launcher_factory=lambda _config: ControlledLauncher(self.db, []), notifier_factory=RaisingNotifier,
            analyzer_factory=lambda _config: RaisingAnalyzer(),
        )

        service.dispatch_analysis(config, TaskStore(self.root))
        service._analysis_worker.join(timeout=2)
        self.assertEqual(state.load()["tasks"][original]["analysisStatus"], "completed")

        service.dispatch_notification(config)
        service._notification_worker.join(timeout=2)
        self.assertEqual(state.load()["tasks"]["success"]["notificationStatus"], "pending")

    def test_heartbeat_reports_queue_age_and_retry_phase(self):
        original = self.insert_task(1, "app-a")
        launcher = ControlledLauncher(self.db, ["no_record"])
        service, _ = self.service(launcher)
        service.scan_once(self.config)
        service.record_completed_scan(0)

        with self.assertLogs(level="INFO") as output:
            service.emit_heartbeat()

        text = "\n".join(output.output)
        self.assertIn("最早等待=", text)
        self.assertIn("launch_requested", text)
        self.assertIn(original, service.state_store.load()["tasks"])

    def test_log_handler_keeps_only_the_last_three_days(self):
        path = self.root / "retained.log"
        cutoff = datetime.now() - timedelta(days=3)
        path.write_text(
            f"{(cutoff - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} INFO old\n"
            f"{(cutoff + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} INFO retained\n",
            encoding="utf-8",
        )
        handler = RecentDaysFileHandler(path, retention_days=3)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger = logging.getLogger("retention-test")
        logger.handlers = [handler]
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.info("new")
        handler.close()
        lines = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn("retained", lines[0])
        self.assertIn("new", lines[1])


if __name__ == "__main__":
    unittest.main()
