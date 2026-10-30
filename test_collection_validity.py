"""Testes isolados da validade e persistência da coleta."""

import importlib.util
import contextlib
import hashlib
import io
import json
import os
import sqlite3
import subprocess
import tarfile
import tempfile
import time
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
        updates=None,
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
            stack.enter_context(patch.object(
                neo_health, "get_updates",
                return_value=(
                    {"total": 0, "important": 0} if updates is None else updates
                ),
            ))
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

    def test_p2_partial_log_is_human_readable_and_matches_persisted_status(self):
        result, output = self.run_collection(ports=None)
        self.assertEqual(result, 0)
        status, details = self.last_row(["collection_status", "missing_components"])
        self.assertEqual(status, "partial")
        persisted = json.loads(details)

        # Texto legível: sem JSON interno, mas com toda a informação.
        self.assertIn("⚠ Coleta parcial", output)
        self.assertNotIn("{", output)
        self.assertNotIn('"status"', output)

        for name, item in persisted.items():
            for line in neo_health.describe_missing_components({name: item}):
                self.assertIn(line, output)

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
    # Atualizações: comuns são informação; segurança é destaque
    # ----------------------------------------------------------

    def test_common_updates_are_information_not_warning(self):
        result, output = self.run_collection(updates={"total": 11, "important": 0})
        self.assertEqual(result, 0)
        self.assertIn("ℹ 11 atualizações disponíveis", output)
        self.assertNotIn("⚠ 11 atualizações disponíveis", output)
        self.assertNotIn("atualizações de segurança", output)

    def test_security_updates_are_highlighted_separately(self):
        result, output = self.run_collection(updates={"total": 11, "important": 2})
        self.assertEqual(result, 0)
        self.assertIn("ℹ 11 atualizações disponíveis", output)
        self.assertIn("⚠ 2 atualizações de segurança", output)

    def test_single_common_update_uses_singular(self):
        result, output = self.run_collection(updates={"total": 1, "important": 0})
        self.assertEqual(result, 0)
        self.assertIn("ℹ 1 atualização disponível", output)
        self.assertNotIn("⚠ 1", output)

    def test_up_to_date_system_keeps_ok_indicator(self):
        result, output = self.run_collection(updates={"total": 0, "important": 0})
        self.assertEqual(result, 0)
        self.assertIn("✓ sistema atualizado", output)
        self.assertNotIn("atualizações disponíveis", output)

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


class MissingComponentsPresentationTests(unittest.TestCase):
    """A coleta parcial vira texto legível, sem JSON interno."""

    def test_single_component_preserves_reason(self):
        self.assertEqual(
            neo_health.describe_missing_components({
                "postgresql_volume": {
                    "status": "failed",
                    "reason": "consulta do volume indisponível",
                },
            }),
            [
                "  PostgreSQL volume: indisponível",
                "  Motivo: consulta do volume indisponível",
            ],
        )

    def test_multiple_components_show_each_reason(self):
        self.assertEqual(
            neo_health.describe_missing_components({
                "postgresql_volume": {
                    "status": "failed",
                    "reason": "consulta do volume indisponível",
                },
                "postgresql_memory": {
                    "status": "failed",
                    "reason": "docker stats indisponível",
                },
            }),
            [
                "  PostgreSQL volume: indisponível",
                "  Motivo: consulta do volume indisponível",
                "  PostgreSQL (memória): indisponível",
                "  Motivo: docker stats indisponível",
            ],
        )

    def test_unsupported_sensor_is_not_shown_as_failure(self):
        self.assertEqual(
            neo_health.describe_missing_components({
                "cpu_temp": {
                    "status": "unsupported",
                    "reason": "sensor ausente ou não suportado",
                },
            }),
            [
                "  CPU (temperatura): não suportado",
                "  Motivo: sensor ausente ou não suportado",
            ],
        )

    def test_absent_reason_invents_nothing(self):
        self.assertEqual(
            neo_health.describe_missing_components({
                "tailscale": {"status": "failed"},
            }),
            ["  Tailscale: indisponível"],
        )

    def test_unknown_component_keeps_readable_name(self):
        self.assertEqual(
            neo_health.describe_missing_components({
                "sensor_extra": {"status": "failed", "reason": "leitura inválida"},
            }),
            [
                "  Sensor extra: indisponível",
                "  Motivo: leitura inválida",
            ],
        )

    def test_rendered_lines_expose_no_json(self):
        text = "\n".join(neo_health.describe_missing_components({
            "docker": {"status": "failed", "reason": "daemon indisponível"},
            "ports": {"status": "failed", "reason": "ss indisponível ou saída inválida"},
        }))
        for proibido in ("{", "}", "status", "reason"):
            self.assertNotIn(proibido, text)


class PostgresqlVolumePrivilegeTests(unittest.TestCase):
    """Medir o volume físico exige root; falhas reais mantêm o diagnóstico."""

    PERMISSION_STDERR = (
        "du: cannot access '/var/lib/docker/volumes/tb-postgres-data/_data': "
        "Permission denied"
    )

    def collect(self, du_effect, access):
        """collect_docker() completo, com apenas o `du` interceptado."""

        def run_effect(command, **kwargs):
            if command[0] == "du":
                if isinstance(du_effect, Exception):
                    raise du_effect
                return du_effect
            if "pg_database_size" in " ".join(command):
                return SimpleNamespace(returncode=0, stdout="12345\n", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(neo_health, "docker_is_running", return_value=True), patch.object(
            neo_health, "get_container_info",
            side_effect=[("running", "healthy"), ("running", "healthy")],
        ), patch.object(
            neo_health, "get_container_memory", return_value=20.0
        ), patch.object(
            neo_health.subprocess, "run", side_effect=run_effect
        ), patch.object(
            neo_health.os, "access", return_value=access
        ):
            docker, available = neo_health.collect_docker()

        return docker, available

    def volume_entry(self, docker):
        return docker["missing_components"]["postgresql_volume"]

    def test_volume_without_privilege_reports_admin_requirement(self):
        docker, available = self.collect(
            SimpleNamespace(returncode=1, stdout="", stderr=self.PERMISSION_STDERR),
            access=False,
        )
        self.assertTrue(available)
        self.assertIsNone(docker["postgresql"]["volume_mb"])
        entry = self.volume_entry(docker)
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(
            entry["reason"], "medição requer privilégios de administrador"
        )
        # Apresentação: mesma estrutura de coleta parcial, motivo específico.
        self.assertEqual(
            neo_health.describe_missing_components({"postgresql_volume": entry}),
            [
                "  PostgreSQL volume: indisponível",
                "  Motivo: medição requer privilégios de administrador",
            ],
        )

    def test_permission_message_in_portuguese_is_detected(self):
        stderr = (
            "du: não foi possível acessar "
            "'/var/lib/docker/volumes/tb-postgres-data/_data': Permissão negada"
        )
        docker, _ = self.collect(
            SimpleNamespace(returncode=1, stdout="", stderr=stderr), access=True,
        )
        self.assertEqual(
            self.volume_entry(docker)["reason"],
            "medição requer privilégios de administrador",
        )

    def test_real_volume_error_is_not_reported_as_privilege(self):
        docker, _ = self.collect(
            SimpleNamespace(
                returncode=1, stdout="",
                stderr="du: cannot access 'x': No such file or directory",
            ),
            access=True,
        )
        self.assertEqual(
            self.volume_entry(docker)["reason"], "consulta do volume indisponível"
        )

    def test_missing_du_tool_is_not_reported_as_privilege(self):
        docker, _ = self.collect(FileNotFoundError("du"), access=True)
        self.assertEqual(
            self.volume_entry(docker)["reason"], "consulta do volume indisponível"
        )

    def test_volume_measured_with_privilege_has_no_missing_entry(self):
        docker, _ = self.collect(
            SimpleNamespace(
                returncode=0, stdout="451\t/var/lib/docker/volumes/x/_data\n",
                stderr="",
            ),
            access=True,
        )
        self.assertEqual(docker["postgresql"]["volume_mb"], 451)
        self.assertNotIn("postgresql_volume", docker["missing_components"])

    def test_admin_requirement_keeps_collection_partial_reason(self):
        docker, _ = self.collect(
            SimpleNamespace(returncode=1, stdout="", stderr=self.PERMISSION_STDERR),
            access=False,
        )
        health = {
            "system": {
                "load": (0.1, 0.2, 0.3),
                "memory": {"used_mb": 100, "percent": 20},
            },
            "disks": {"/": {"percent": 10, "free": 900}},
            "missing_components": docker["missing_components"],
        }
        neo_health.get_collection_status(health)
        self.assertEqual(health["collection_status"], "partial")
        self.assertEqual(
            health["missing_components"]["postgresql_volume"]["reason"],
            "medição requer privilégios de administrador",
        )


class SystemBackupTests(unittest.TestCase):
    """Verificação independente do backup diário do sistema."""

    MEMBERS = neo_health.SYSTEM_BACKUP_REQUIRED_MEMBERS

    BACKUP_COLUMNS = (
        "backup_status",
        "backup_artifact",
        "backup_size_bytes",
        "backup_age_hours",
        "backup_gzip_ok",
        "backup_sha256_ok",
        "backup_sha256",
        "backup_reason",
    )

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.backup_dir = os.path.join(self.temp_dir.name, "neo-system")
        os.mkdir(self.backup_dir)

        self.db_path = os.path.join(self.temp_dir.name, "health.db")

        # No ambiente de teste o volume operacional não existe: o estado do
        # volume é declarado, e nenhum caminho real é acessado.
        mounted = patch.object(
            neo_health, "backup_volume_is_mounted", return_value=True,
        )
        mounted.start()
        self.addCleanup(mounted.stop)

    # ---------------------------------------------------------
    # Apoio
    # ---------------------------------------------------------

    def make_backup(self, members=None, name=None, mtime=None):
        """Cria um artefato .tar.gz real com os membros pedidos."""
        members = self.MEMBERS if members is None else members
        name = name or f"neo-system_{datetime.now():%Y-%m-%d}.tar.gz"
        path = os.path.join(self.backup_dir, name)

        with tarfile.open(path, "w:gz") as archive:
            for member in members:
                data = b"conteudo"
                info = tarfile.TarInfo(member)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))

        if mtime is not None:
            os.utime(path, (mtime, mtime))

        return path

    def make_raw_backup(self, entries, name=None):
        """Cria um artefato com membros de tipos explícitos no TAR."""
        name = name or f"neo-system_{datetime.now():%Y-%m-%d}.tar.gz"
        path = os.path.join(self.backup_dir, name)

        with tarfile.open(path, "w:gz") as archive:
            for entry in entries:
                info = tarfile.TarInfo(entry["name"])
                info.type = entry.get("type", tarfile.REGTYPE)

                if info.type in (tarfile.REGTYPE, tarfile.AREGTYPE):
                    data = b"conteudo"
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
                else:
                    info.linkname = entry.get("linkname", "")
                    archive.addfile(info)

        return path

    def write_checksum(self, path, digest=None):
        """Escreve o .sha256 no formato do `sha256sum`."""
        if digest is None:
            with open(path, "rb") as handle:
                digest = hashlib.sha256(handle.read()).hexdigest()

        checksum = path + neo_health.SYSTEM_BACKUP_CHECKSUM_SUFFIX

        with open(checksum, "w", encoding="utf-8") as handle:
            handle.write(f"{digest}  {os.path.basename(path)}\n")

        return checksum

    def complete_backup(self, **kwargs):
        path = self.make_backup(**kwargs)
        self.write_checksum(path)
        return path

    def persisted_backup(self):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                "SELECT backup_status, backup_artifact, backup_size_bytes, "
                "backup_age_hours, backup_gzip_ok, backup_sha256_ok, "
                "backup_sha256, backup_reason FROM health ORDER BY id DESC "
                "LIMIT 1"
            ).fetchone()


    # ---------------------------------------------------------
    # Backup válido
    # ---------------------------------------------------------

    def test_valid_backup_is_ok(self):
        path = self.complete_backup()
        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "ok")
        self.assertEqual(backup["gzip_ok"], 1)
        self.assertEqual(backup["sha256_ok"], 1)
        self.assertEqual(backup["artifact"], os.path.basename(path))
        self.assertEqual(backup["size_bytes"], os.path.getsize(path))
        self.assertLess(backup["age_hours"], 1)
        self.assertIsNone(backup["reason"])

        with open(path, "rb") as handle:
            expected = hashlib.sha256(handle.read()).hexdigest()
        self.assertEqual(backup["sha256"], expected)

    def test_valid_backup_is_persisted(self):
        self.complete_backup()

        health = sample()
        health["system_backup"] = neo_health.collect_system_backup(
            self.backup_dir
        )

        self.assertEqual(
            neo_health.finalize_collection(health, True, self.db_path), 0
        )

        row = self.persisted_backup()

        self.assertEqual(row[0], "ok")
        self.assertTrue(row[1].startswith("neo-system_"))
        self.assertGreater(row[2], 0)
        self.assertLess(row[3], 1)
        self.assertEqual(row[4], 1)
        self.assertEqual(row[5], 1)
        self.assertEqual(len(row[6]), 64)
        self.assertIsNone(row[7])

    # ---------------------------------------------------------
    # Falhas de integridade
    # ---------------------------------------------------------

    def test_corrupted_gzip_fails(self):
        path = os.path.join(
            self.backup_dir, "neo-system_2026-10-30.tar.gz"
        )

        with open(path, "wb") as handle:
            handle.write(b"isto nao e um fluxo gzip valido")

        self.write_checksum(path)

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 0)
        # Sem gzip válido a comparação não chega a ocorrer.
        self.assertIsNone(backup["sha256_ok"])
        self.assertIn("gzip", backup["reason"])

    def test_gzip_header_with_corrupted_deflate_stream_fails(self):
        """Cabeçalho gzip válido e corpo deflate corrompido: zlib.error."""
        path = os.path.join(
            self.backup_dir, "neo-system_2026-10-30.tar.gz"
        )

        with open(path, "wb") as handle:
            handle.write(
                b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03"
                + b"\xff" * 10
            )

        self.write_checksum(path)

        # A exceção precisa ser contida aqui, não tratada como
        # indisponibilidade pela camada de cima.
        self.assertFalse(neo_health.gzip_integrity_ok(path))

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 0)
        self.assertIn("gzip", backup["reason"])

    def test_divergent_sha256_fails(self):
        path = self.complete_backup()
        self.write_checksum(path, digest="0" * 64)

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 1)
        self.assertEqual(backup["sha256_ok"], 0)
        self.assertIn("SHA-256", backup["reason"])

    def test_absent_checksum_is_unavailable_with_null_sha256(self):
        self.make_backup()

        backup = neo_health.collect_system_backup(self.backup_dir)

        # Não houve comparação: NULL, nunca 0 e nunca 1.
        self.assertEqual(backup["status"], "unavailable")
        self.assertEqual(backup["gzip_ok"], 1)
        self.assertIsNone(backup["sha256_ok"])
        self.assertEqual(len(backup["sha256"]), 64)
        self.assertIn("SHA-256", backup["reason"])

    def test_checksum_name_matching_artifact_is_ok(self):
        path = self.complete_backup(name="neo-system_2026-10-30.tar.gz")

        parsed = neo_health.read_checksum_file(
            path + neo_health.SYSTEM_BACKUP_CHECKSUM_SUFFIX
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed[1], "neo-system_2026-10-30.tar.gz")

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "ok")
        self.assertEqual(backup["sha256_ok"], 1)

    def test_checksum_referring_another_file_fails(self):
        """Hash correto, mas o checksum nomeia outro arquivo: divergência."""
        path = self.complete_backup(name="neo-system_2026-10-30.tar.gz")

        with open(path, "rb") as handle:
            digest = hashlib.sha256(handle.read()).hexdigest()

        checksum = path + neo_health.SYSTEM_BACKUP_CHECKSUM_SUFFIX

        with open(checksum, "w", encoding="utf-8") as handle:
            handle.write(f"{digest}  neo-system_2026-10-29.tar.gz\n")

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 1)
        # A comparação foi feita e não corresponde: 0, nunca NULL.
        self.assertEqual(backup["sha256_ok"], 0)
        self.assertIn("neo-system_2026-10-29.tar.gz", backup["reason"])

    def test_missing_essential_member_fails(self):
        path = self.complete_backup(
            members=("opt/neo-health/health.db", "etc/fstab"),
        )

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 1)
        self.assertEqual(backup["sha256_ok"], 1)
        self.assertIn(
            "home/jc/thingsboard/docker-compose.yml", backup["reason"]
        )
        self.assertIn("notification_state.db", backup["reason"])
        self.assertTrue(os.path.isfile(path))

    def test_prefixed_members_are_accepted(self):
        path = self.complete_backup(
            members=tuple(f"./{member}" for member in self.MEMBERS),
        )

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "ok")
        self.assertTrue(os.path.isfile(path))


    def test_essential_member_as_directory_is_not_accepted(self):
        target = "opt/neo-health/health.db"

        entries = [
            {"name": member}
            for member in self.MEMBERS
            if member != target
        ]
        entries.append({"name": target + "/", "type": tarfile.DIRTYPE})

        path = self.make_raw_backup(entries)
        self.write_checksum(path)

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 1)
        self.assertEqual(backup["sha256_ok"], 1)
        self.assertIn(target, backup["reason"])

    def test_essential_member_as_symlink_is_not_accepted(self):
        target = "home/jc/thingsboard/docker-compose.yml"

        entries = [
            {"name": member}
            for member in self.MEMBERS
            if member != target
        ]
        entries.append({
            "name": target,
            "type": tarfile.SYMTYPE,
            "linkname": "/etc/passwd",
        })

        path = self.make_raw_backup(entries)
        self.write_checksum(path)

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "failed")
        self.assertEqual(backup["gzip_ok"], 1)
        self.assertEqual(backup["sha256_ok"], 1)
        self.assertIn(target, backup["reason"])

    # ---------------------------------------------------------
    # Ausência e indisponibilidade
    # ---------------------------------------------------------

    def test_absent_backup_is_missing(self):
        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "missing")
        self.assertIsNone(backup["artifact"])
        self.assertIsNone(backup["gzip_ok"])
        self.assertIn("nenhum", backup["reason"])

    def test_backup_older_than_six_hours_is_missing(self):
        path = self.complete_backup()
        old = time.time() - (neo_health.BACKUP_MAX_AGE_HOURS + 1) * 3600
        os.utime(path, (old, old))

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "missing")
        self.assertGreater(
            backup["age_hours"], neo_health.BACKUP_MAX_AGE_HOURS
        )
        self.assertIsNone(backup["gzip_ok"])

    def test_absent_directory_on_mounted_volume_is_missing(self):
        """Volume montado e diretório ainda inexistente: ausência, não falha."""
        backup = neo_health.collect_system_backup(
            os.path.join(self.temp_dir.name, "inexistente")
        )

        self.assertEqual(backup["status"], "missing")
        self.assertIsNone(backup["artifact"])
        self.assertIsNone(backup["gzip_ok"])
        self.assertIn("ausente", backup["reason"])

    def test_absent_directory_on_unmounted_volume_stays_unavailable(self):
        """Sem o volume, nada se afirma: a classificação é indisponibilidade."""
        with patch.object(
            neo_health, "backup_volume_is_mounted", return_value=False
        ):
            backup = neo_health.collect_system_backup(
                os.path.join(self.temp_dir.name, "inexistente")
            )

        self.assertEqual(backup["status"], "unavailable")
        self.assertIn("montado", backup["reason"])

    def test_inaccessible_directory_is_unavailable(self):
        """Diretório existente, mas ilegível: indisponibilidade, não ausência."""
        self.assertTrue(os.path.isdir(self.backup_dir))

        with patch.object(
            neo_health.os, "listdir", side_effect=PermissionError("negado")
        ):
            backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "unavailable")
        self.assertIn("inacessível", backup["reason"])

    def test_unmounted_volume_is_unavailable(self):
        self.complete_backup()

        with patch.object(
            neo_health, "backup_volume_is_mounted", return_value=False
        ):
            backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "unavailable")
        self.assertIn("montado", backup["reason"])

    # ---------------------------------------------------------
    # Seleção do artefato
    # ---------------------------------------------------------

    def test_checksum_file_is_never_the_artifact(self):
        path = self.complete_backup()

        backups = neo_health.get_system_backups(self.backup_dir)

        self.assertEqual(backups["count"], 1)
        self.assertTrue(backups["latest"].endswith(".tar.gz"))
        self.assertEqual(backups["latest"], path)

    def test_latest_artifact_is_chosen(self):
        self.complete_backup(
            name="neo-system_2026-10-29.tar.gz",
            mtime=time.time() - 3 * 3600,
        )
        newer = self.complete_backup(name="neo-system_2026-10-30.tar.gz")

        backup = neo_health.collect_system_backup(self.backup_dir)

        self.assertEqual(backup["status"], "ok")
        self.assertEqual(backup["artifact"], os.path.basename(newer))


    # ---------------------------------------------------------
    # Migração do schema
    # ---------------------------------------------------------

    def test_legacy_database_receives_new_columns(self):
        # Schema de produção imediatamente anterior a esta mudança: todas
        # as colunas exceto as oito do backup do sistema.
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE health (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    load_1m REAL, load_5m REAL, load_15m REAL,
                    memory_used_mb INTEGER, memory_percent REAL,
                    cpu_temp REAL, nvme_temp REAL,
                    root_used_percent REAL, root_free_bytes INTEGER,
                    updates_available INTEGER,
                    tb_memory_mb REAL, pg_memory_mb REAL,
                    tb_database_mb REAL, pg_volume_mb REAL,
                    collection_status TEXT NOT NULL DEFAULT 'ok',
                    missing_components TEXT,
                    updates_important INTEGER
                )"""
            )
            conn.execute(
                "INSERT INTO health (timestamp, load_1m) "
                "VALUES ('2026-09-01T00:00:00', 0.5)"
            )

        self.complete_backup()

        health = sample()
        health["system_backup"] = neo_health.collect_system_backup(
            self.backup_dir
        )

        self.assertEqual(
            neo_health.finalize_collection(health, True, self.db_path), 0
        )

        placeholders = ", ".join(self.BACKUP_COLUMNS)

        with sqlite3.connect(self.db_path) as conn:
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(health)")
            }
            legacy = conn.execute(
                f"SELECT {placeholders} FROM health WHERE id = 1"
            ).fetchone()
            count = conn.execute("SELECT COUNT(*) FROM health").fetchone()[0]

        for name in self.BACKUP_COLUMNS:
            self.assertIn(name, columns)

        # O registro existente é preservado e permanece "não verificado".
        self.assertEqual(legacy, (None,) * len(self.BACKUP_COLUMNS))
        self.assertEqual(count, 2)

    def test_migration_is_idempotent(self):
        self.complete_backup()

        for _ in range(3):
            health = sample()
            health["system_backup"] = neo_health.collect_system_backup(
                self.backup_dir
            )
            self.assertEqual(
                neo_health.finalize_collection(health, True, self.db_path),
                0,
            )

        with sqlite3.connect(self.db_path) as conn:
            columns = [
                row[1] for row in conn.execute("PRAGMA table_info(health)")
            ]
            count = conn.execute("SELECT COUNT(*) FROM health").fetchone()[0]

        for name in self.BACKUP_COLUMNS:
            self.assertEqual(columns.count(name), 1)

        self.assertEqual(count, 3)

    # ---------------------------------------------------------
    # A coleta não depende do backup
    # ---------------------------------------------------------

    def test_backup_failure_does_not_interrupt_collection(self):
        # Diretório vazio: o backup está ausente.
        backup = neo_health.collect_system_backup(self.backup_dir)
        self.assertEqual(backup["status"], "missing")

        health = sample()
        health["system_backup"] = backup

        self.assertEqual(
            neo_health.finalize_collection(health, True, self.db_path), 0
        )

        with sqlite3.connect(self.db_path) as conn:
            status, missing, backup_status = conn.execute(
                "SELECT collection_status, missing_components, backup_status "
                "FROM health"
            ).fetchone()

        self.assertEqual(status, "ok")
        self.assertIsNone(missing)
        self.assertEqual(backup_status, "missing")

    def test_backup_state_does_not_change_collection_semantics(self):
        component = {"ports": {"status": "failed", "reason": "ss indisponível"}}

        health = sample("partial", component)
        health["system_backup"] = neo_health.collect_system_backup(
            self.backup_dir
        )

        self.assertEqual(
            neo_health.finalize_collection(health, True, self.db_path), 0
        )

        # O estado parcial vem do componente de coleta, nunca do backup.
        self.assertEqual(health["collection_status"], "partial")
        self.assertNotIn("backup", health["missing_components"])
        self.assertNotIn("system_backup", health["missing_components"])

        with sqlite3.connect(self.db_path) as conn:
            status, missing = conn.execute(
                "SELECT collection_status, missing_components FROM health"
            ).fetchone()

        self.assertEqual(status, "partial")
        self.assertEqual(json.loads(missing), component)

    def test_collection_without_backup_information_stays_null(self):
        health = sample()
        self.assertEqual(
            neo_health.finalize_collection(health, True, self.db_path), 0
        )

        self.assertEqual(self.persisted_backup(), (None,) * 8)


if __name__ == "__main__":
    unittest.main()