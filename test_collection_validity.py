"""Testes isolados da validade e persistência da coleta."""

import importlib.util
import contextlib
import io
import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = os.path.join(os.path.dirname(__file__), "neo_health.py")
SPEC = importlib.util.spec_from_file_location("neo_health", SCRIPT)
neo_health = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(neo_health)


def sample(status="ok", missing=None):
    return {
        "timestamp": datetime(2026, 9, 26),
        "system": {
            "load": (0.2, 0.3, 0.4),
            "memory": {"used_mb": 100, "percent": 20},
        },
        "disks": {"/": {"percent": 30, "free": 1000}},
        "temperatures": {"cpu": None, "nvme": None},
        "updates": None,
        "docker": {
            "thingsboard": {"memory": None, "database_mb": None},
            "postgresql": {
                "memory": None, "volume_mb": None, "tables": None,
            },
        },
        "collection_status": status,
        "missing_components": missing or {},
    }


PORTS = {8080, 1883, 8883}


def disk_sample():
    return {"total": 1000, "used": 100, "free": 900, "percent": 10}


def docker_healthy():
    return {
        "daemon": "running",
        "thingsboard": {"status": "running", "health": "healthy", "memory": 100.0},
        "postgresql": {
            "status": "running",
            "health": "healthy",
            "memory": 50.0,
            "database_mb": 300.0,
            "volume_mb": 400.0,
            "tables": [],
        },
        "missing_components": {},
    }


def docker_unavailable():
    return {
        "daemon": None,
        "thingsboard": {"status": None, "health": None},
        "postgresql": {"status": None, "health": None},
        "missing_components": {
            "docker": {"status": "failed", "reason": "daemon indisponível"},
            "postgresql": {"status": "failed", "reason": "Docker indisponível"},
        },
    }


class CollectionValidityTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "health.db")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_ok_collection_is_saved_and_schema_migration_is_idempotent(self):
        health = sample()
        self.assertEqual(neo_health.finalize_collection(health, True, self.db_path), 0)
        self.assertEqual(neo_health.finalize_collection(health, True, self.db_path), 0)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT collection_status, missing_components FROM health"
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], ("ok", None))

    def test_partial_collection_persists_structured_reason(self):
        missing = {"ports": {"status": "failed", "reason": "ss retornou erro"}}
        health = sample("partial", missing)
        self.assertEqual(neo_health.finalize_collection(health, True, self.db_path), 0)
        with sqlite3.connect(self.db_path) as conn:
            status, details = conn.execute(
                "SELECT collection_status, missing_components FROM health"
            ).fetchone()
        self.assertEqual(status, "partial")
        self.assertEqual(json.loads(details), missing)

    def test_essential_failure_does_not_save(self):
        health = sample()
        health["system"]["load"] = None
        with self.assertRaises(RuntimeError):
            neo_health.finalize_collection(health, True, self.db_path)
        self.assertFalse(os.path.exists(self.db_path))

    def test_cli_exit_codes_include_persistence_failure(self):
        with patch.object(neo_health, "main", side_effect=RuntimeError("load unavailable")):
            self.assertEqual(neo_health.cli(["--save", "--db", self.db_path]), 1)
        with patch.object(neo_health, "main", return_value=0):
            self.assertEqual(neo_health.cli(["--save", "--db", self.db_path]), 0)
        with patch.object(neo_health, "main", side_effect=sqlite3.OperationalError("disk full")):
            self.assertEqual(neo_health.cli(["--save", "--db", self.db_path]), 1)

    def test_save_failure_propagates(self):
        with patch.object(neo_health, "save_health", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaises(sqlite3.OperationalError):
                neo_health.finalize_collection(sample(), True, self.db_path)

    def test_existing_rows_default_to_ok(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE health (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL)")
            conn.execute("INSERT INTO health (timestamp) VALUES ('2026-09-25T00:00:00')")
            neo_health.initialize_health_schema(conn)
            neo_health.initialize_health_schema(conn)
            status = conn.execute("SELECT collection_status FROM health").fetchone()[0]
        self.assertEqual(status, "ok")

    def test_legacy_malformed_volume_column_is_migrated_idempotently(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE health (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, pg_volume_mbREAL)")
            neo_health.initialize_health_schema(conn)
            neo_health.initialize_health_schema(conn)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(health)")}
        self.assertIn("pg_volume_mb", columns)
        self.assertIn("collection_status", columns)

    def test_ss_failure_is_none_and_not_an_empty_port_set(self):
        with patch.object(neo_health.subprocess, "run", side_effect=FileNotFoundError):
            self.assertIsNone(neo_health.get_listening_ports())
        self.assertIsNone(neo_health.get_closed_ports(None, {8080, 1883, 8883}))
        self.assertEqual(neo_health.get_closed_ports(set(), {8080, 1883, 8883}), {8080, 1883, 8883})
        health = sample("partial", {"ports": {"status": "failed", "reason": "ss"}})
        self.assertEqual(health["collection_status"], "partial")

    def test_unsupported_sensor_and_apt_absence_do_not_make_collection_partial(self):
        health = sample()
        health["missing_components"]["cpu_temp"] = {
            "status": "unsupported", "reason": "sensor ausente"
        }
        neo_health.get_collection_status(health)
        self.assertEqual(health["collection_status"], "ok")

    def test_docker_unavailable_is_partial_and_never_queries_postgres(self):
        with patch.object(neo_health, "docker_is_running", return_value=False), patch.object(
            neo_health, "get_postgresql_info", side_effect=AssertionError("stale query")
        ):
            docker, available = neo_health.collect_docker()
        self.assertFalse(available)
        self.assertIsNone(docker["postgresql"].get("database_mb"))
        self.assertEqual(docker["missing_components"]["docker"]["status"], "failed")

    def test_docker_unavailable_main_saves_partial_without_old_values(self):
        docker = {
            "daemon": None,
            "thingsboard": {"status": None, "health": None},
            "postgresql": {"status": None, "health": None},
            "missing_components": {
                "docker": {"status": "failed", "reason": "daemon indisponível"},
                "postgresql": {"status": "failed", "reason": "Docker indisponível"},
            },
        }
        command_result = SimpleNamespace(returncode=1, stdout="", stderr="")
        with patch.object(neo_health, "run_command", return_value="up"), patch.object(
            neo_health.os, "getloadavg", return_value=(0.1, 0.2, 0.3)
        ), patch.object(
            neo_health, "get_memory", return_value=(1000, 100, 10, 0, 0)
        ), patch.object(
            neo_health, "get_disk", return_value={"total": 1000, "used": 100, "free": 900, "percent": 10}
        ), patch.object(neo_health, "get_temperatures", return_value={}), patch.object(
            neo_health, "get_updates", return_value=None
        ), patch.object(neo_health, "collect_docker", return_value=(docker, False)), patch.object(
            neo_health, "get_listening_ports", return_value=None
        ), patch.object(neo_health, "get_tailscale_ip", return_value=None), patch.object(
            neo_health, "get_backups", return_value=None
        ), patch.object(neo_health.subprocess, "run", return_value=command_result):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                result = neo_health.main(save=True, db_path=self.db_path)

        self.assertEqual(result, 0)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT collection_status, missing_components, tb_memory_mb, "
                "pg_memory_mb, tb_database_mb, pg_volume_mb FROM health"
            ).fetchone()
        self.assertEqual(row[0], "partial")
        self.assertEqual(json.loads(row[1])["docker"]["status"], "failed")
        self.assertIn("ports", json.loads(row[1]))
        self.assertNotIn("não está escutando", output.getvalue())
        self.assertEqual(row[2:], (None, None, None, None))

    def test_stopped_container_is_health_finding_not_collection_failure(self):
        with patch.object(neo_health, "docker_is_running", return_value=True), patch.object(
            neo_health, "get_container_info", side_effect=[("running", "healthy"), ("exited", "none")]
        ), patch.object(neo_health, "get_container_memory", return_value=20), patch.object(
            neo_health, "get_postgresql_info", side_effect=AssertionError("stopped PG queried")
        ):
            docker, available = neo_health.collect_docker()
        self.assertTrue(available)
        self.assertEqual(docker["postgresql"]["status"], "exited")
        self.assertEqual(docker["missing_components"], {})
    # ----------------------------------------------------------
    # Simulação de main() com o sistema externo substituído
    # ----------------------------------------------------------

    def run_collection(
        self,
        *,
        temperatures=None,
        disks_effect=None,
        docker=None,
        docker_available=True,
        systemd_state=True,
        ports=PORTS,
        backups=None,
        run_command_error=None,
    ):
        """Executa main(save=True) sem tocar o sistema real; devolve (exit, saída)."""
        command_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        listening_ports = None if ports is None else set(ports)

        with contextlib.ExitStack() as stack:
            if run_command_error is None:
                stack.enter_context(
                    patch.object(neo_health, "run_command", return_value="up")
                )
            else:
                stack.enter_context(
                    patch.object(neo_health, "run_command", side_effect=run_command_error)
                )
            stack.enter_context(
                patch.object(neo_health.os, "getloadavg", return_value=(0.1, 0.2, 0.3))
            )
            stack.enter_context(
                patch.object(neo_health, "get_memory", return_value=(1000, 100, 10, 0, 0))
            )
            if disks_effect is None:
                stack.enter_context(
                    patch.object(neo_health, "get_disk", return_value=disk_sample())
                )
            else:
                stack.enter_context(
                    patch.object(neo_health, "get_disk", side_effect=disks_effect)
                )
            stack.enter_context(
                patch.object(
                    neo_health,
                    "get_temperatures",
                    return_value={} if temperatures is None else temperatures,
                )
            )
            stack.enter_context(patch.object(neo_health, "get_updates", return_value=0))
            stack.enter_context(
                patch.object(
                    neo_health,
                    "collect_docker",
                    return_value=(
                        docker_healthy() if docker is None else docker,
                        docker_available,
                    ),
                )
            )
            stack.enter_context(
                patch.object(neo_health, "get_listening_ports", return_value=listening_ports)
            )
            stack.enter_context(
                patch.object(neo_health, "get_tailscale_ip", return_value="100.64.0.1")
            )
            stack.enter_context(
                patch.object(neo_health, "get_systemd_unit_state", return_value=systemd_state)
            )
            stack.enter_context(patch.object(neo_health, "get_backups", return_value=backups))
            stack.enter_context(
                patch.object(neo_health.subprocess, "run", return_value=command_result)
            )

            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = neo_health.main(save=True, db_path=self.db_path)

        return result, output.getvalue()

    def last_row(self, columns):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                f"SELECT {', '.join(columns)} FROM health ORDER BY id DESC LIMIT 1"
            ).fetchone()

    # ----------------------------------------------------------
    # P1 — falhas de ferramentas não essenciais não viram exit 1
    # ----------------------------------------------------------

    def test_p1_uptime_tool_failure_is_not_fatal(self):
        result, output = self.run_collection(
            run_command_error=subprocess.CalledProcessError(1, "uptime")
        )
        self.assertEqual(result, 0)
        status, details = self.last_row(["collection_status", "missing_components"])
        self.assertEqual(status, "partial")
        self.assertEqual(json.loads(details)["uptime"]["status"], "failed")
        self.assertIn("desconhecido", output)

    def test_p1_systemd_unavailable_is_not_fatal(self):
        result, output = self.run_collection(systemd_state=None)
        self.assertEqual(result, 0)
        status, details = self.last_row(["collection_status", "missing_components"])
        self.assertEqual(status, "partial")
        missing = json.loads(details)
        self.assertEqual(missing["tailscale"]["status"], "failed")
        self.assertEqual(missing["backup_timer"]["status"], "failed")
        self.assertIn("não foi possível consultar o systemd", output)

    def test_p1_unreadable_latest_backup_is_not_fatal(self):
        backups = {"count": 1, "latest": "/caminho/inexistente/thingsboard_x.sql.gz"}
        result, output = self.run_collection(backups=backups)
        self.assertEqual(result, 0)
        (status,) = self.last_row(["collection_status"])
        self.assertEqual(status, "ok")
        self.assertIn("não foi possível ler o backup mais recente", output)

    def test_p1_non_essential_helpers_degrade_without_raising(self):
        with patch.object(neo_health.subprocess, "run", side_effect=FileNotFoundError):
            self.assertIsNone(neo_health.get_systemd_unit_state("tailscaled"))
        with patch.object(neo_health, "run_command", side_effect=FileNotFoundError):
            self.assertIsNone(neo_health.run_command_or_none(["uptime", "-p"]))
        with patch.object(neo_health.os, "listdir", side_effect=PermissionError):
            self.assertIsNone(neo_health.get_backups(self.temp_dir.name))

    # ----------------------------------------------------------
    # P2 — o log de parcialidade reflete o status persistido
    # ----------------------------------------------------------

    def test_p2_partial_log_matches_persisted_status(self):
        result, output = self.run_collection(ports=None)
        self.assertEqual(result, 0)
        status, details = self.last_row(["collection_status", "missing_components"])
        self.assertEqual(status, "partial")
        self.assertIn("Coleta parcial: ", output)
        logged = json.loads(output.split("Coleta parcial: ", 1)[1].splitlines()[0])
        self.assertEqual(logged, json.loads(details))

    def test_p2_unsupported_sensor_is_not_reported_as_partial(self):
        result, output = self.run_collection(temperatures={})
        self.assertEqual(result, 0)
        status, details, cpu_temp, nvme_temp = self.last_row([
            "collection_status", "missing_components", "cpu_temp", "nvme_temp",
        ])
        self.assertEqual(status, "ok")
        self.assertEqual(json.loads(details)["cpu_temp"]["status"], "unsupported")
        self.assertNotIn("Coleta parcial", output)
        self.assertIsNone(cpu_temp)
        self.assertIsNone(nvme_temp)

    # ----------------------------------------------------------
    # D8 — /boot e /boot/efi não são essenciais
    # ----------------------------------------------------------

    def test_d8_boot_filesystems_are_not_essential(self):
        def disks_effect(path):
            if path in ("/boot", "/boot/efi"):
                raise subprocess.CalledProcessError(1, ["df", "-P", path])
            return disk_sample()

        result, output = self.run_collection(disks_effect=disks_effect)
        self.assertEqual(result, 0)
        status, details = self.last_row(["collection_status", "missing_components"])
        self.assertEqual(status, "ok")
        self.assertIsNone(json.loads(details).get("filesystem_root"))
        self.assertIn("/boot", output)

    # ----------------------------------------------------------
    # D9 — temperatura ausente permanece NULL
    # ----------------------------------------------------------

    def test_d9_absent_temperature_is_persisted_as_null(self):
        result, _ = self.run_collection(temperatures={"cpu": 41.0})
        self.assertEqual(result, 0)
        status, details, cpu_temp, nvme_temp = self.last_row([
            "collection_status", "missing_components", "cpu_temp", "nvme_temp",
        ])
        self.assertEqual(status, "ok")
        self.assertEqual(cpu_temp, 41.0)
        self.assertIsNone(nvme_temp)
        self.assertIsNone(details)


if __name__ == "__main__":
    unittest.main()