#!/usr/bin/env python3

import os
import socket
import sqlite3
import subprocess
import gzip
import sys
import argparse
import json
from datetime import datetime

DB_PATH = "/opt/neo-health/health.db"

# Pocket de segurança do Ubuntu: upgrades vindos daqui são "importantes".
SECURITY_POCKET_SUFFIX = "-security"

# Caminho físico do volume de dados do PostgreSQL. O Docker protege
# /var/lib/docker, então medir esse tamanho exige privilégio de root; quando
# o processo atual não puder acessá-lo, a medição é reportada como
# dependente de privilégios de administrador (sem virar falha fatal).
POSTGRESQL_VOLUME_PATH = "/var/lib/docker/volumes/tb-postgres-data/_data"

# Palavras que, na saída do `du`, indicam permissão insuficiente. O idioma
# acompanha o sistema (mensagens em inglês ou português).
PERMISSION_DENIED_MARKERS = (
    "permission denied",
    "permissão negada",
)

# ============================================================
# Neo Health Check
# ============================================================


def run_command(command):
    """Executa um comando do sistema e devolve sua saída."""
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def run_command_or_none(command):
    """Executa comando não essencial; devolve None quando a ferramenta falha."""
    try:
        return run_command(command)
    except (OSError, subprocess.SubprocessError):
        return None


def get_systemd_unit_state(unit):
    """True/False quando o systemd responde; None quando não foi consultável."""
    try:
        result = subprocess.run(["systemctl", "is-active", "--quiet", unit])
    except OSError:
        return None
    return result.returncode == 0


def get_collection_status_value(missing_components):
    """'partial' apenas com falha de coleta; 'unsupported' não é falha."""
    return (
        "partial"
        if any(item.get("status") == "failed" for item in missing_components.values())
        else "ok"
    )


# Nomes de exibição dos componentes internos da coleta parcial. O sufixo é
# neutro e não sugere causa: nenhuma informação é criada aqui.
MISSING_COMPONENT_LABELS = {
    "docker": "Docker",
    "docker_thingsboard": "ThingsBoard (container)",
    "docker_postgresql": "PostgreSQL (container)",
    "thingsboard_memory": "ThingsBoard (memória)",
    "postgresql": "PostgreSQL",
    "postgresql_memory": "PostgreSQL (memória)",
    "postgresql_database": "PostgreSQL database",
    "postgresql_volume": "PostgreSQL volume",
    "postgresql_tables": "PostgreSQL tabelas",
    "cpu_temp": "CPU (temperatura)",
    "nvme_temp": "NVMe (temperatura)",
    "filesystem_root": "Sistema de arquivos /",
    "ports": "Portas",
    "uptime": "Uptime",
    "tailscale": "Tailscale",
    "backup_timer": "Timer de backup",
}

# 'unsupported' não é falha de coleta; a distinção é preservada.
MISSING_STATUS_LABELS = {
    "failed": "indisponível",
    "unsupported": "não suportado",
}


def describe_missing_components(missing_components):
    """Traduz a estrutura interna da coleta parcial em linhas legíveis.

    Preserva toda a informação diagnóstica: cada componente aparece com seu
    estado e, quando a coleta registrou um motivo, o motivo original é
    reproduzido sem alteração. Nenhuma causa é inventada e o JSON interno
    não é exibido — ele continua apenas persistido no histórico.
    """
    lines = []

    for name, details in missing_components.items():
        label = MISSING_COMPONENT_LABELS.get(
            name, str(name).replace("_", " ").capitalize()
        )

        if not isinstance(details, dict):
            # Forma inesperada: o valor é preservado como está.
            lines.append(f"  {label}: {details}")
            continue

        status = details.get("status")
        state = MISSING_STATUS_LABELS.get(status, status)
        lines.append(f"  {label}: {state}" if state else f"  {label}")

        reason = details.get("reason")
        if reason:
            lines.append(f"  Motivo: {reason}")

    return lines


def section(title):
    print()
    print(f"── {title} " + "─" * max(0, 54 - len(title)))


def ok(name, message=""):
    print(f"  {name:<20} ✓ {message}")


def info(name, message=""):
    """Informação neutra: sem indicador de alerta."""
    print(f"  {name:<20} ℹ {message}")


def warn(name, message=""):
    print(f"  {name:<20} ⚠ {message}")


def fail(name, message=""):
    print(f"  {name:<20} ✗ {message}")


def get_memory():
    """Obtém memória RAM e swap total, disponível e utilizada em MB."""
    memory = {}

    with open("/proc/meminfo") as file:
        for line in file:
            key, value = line.split(":", 1)
            value = int(value.strip().split()[0])
            memory[key] = value

    total_kb = memory["MemTotal"]
    available_kb = memory["MemAvailable"]

    used_kb = total_kb - available_kb

    swap_total_kb = memory["SwapTotal"]
    swap_free_kb = memory["SwapFree"]
    swap_used_kb = swap_total_kb - swap_free_kb

    total_mb = total_kb // 1024
    used_mb = used_kb // 1024
    percent = used_kb * 100 / total_kb

    swap_total_mb = swap_total_kb // 1024
    swap_used_mb = swap_used_kb // 1024

    return (
        total_mb,
        used_mb,
        percent,
        swap_total_mb,
        swap_used_mb,
    )


def get_disk(path):
    """Obtém uso, percentual e espaço livre de um filesystem."""
    result = subprocess.run(
        ["df", "-P", path], capture_output=True, text=True, check=True
    )

    line = result.stdout.strip().splitlines()[-1]
    fields = line.split()

    total_kb = int(fields[1])
    used_kb = int(fields[2])
    free_kb = int(fields[3])
    percent = int(fields[4].rstrip("%"))

    return {
        "total": total_kb,
        "used": used_kb,
        "free": free_kb,
        "percent": percent,
    }


def get_collection_status(health):
    """Marca como parcial apenas falhas de consulta, não achados de saúde."""
    missing = health["missing_components"]
    required = (
        ("load", health["system"].get("load")),
        ("memory", health["system"].get("memory")),
        ("filesystem_root", health["disks"].get("/")),
    )
    for component, value in required:
        if value is None:
            missing[component] = {"status": "failed", "reason": "medição indisponível"}
    health["collection_status"] = get_collection_status_value(missing)


def validate_essential_collection(health):
    """Interrompe a coleta se faltar load, memória do host ou filesystem /."""
    load = health["system"].get("load")
    memory = health["system"].get("memory") or {}
    root = health["disks"].get("/")
    if not load or len(load) != 3 or any(value is None for value in load):
        raise RuntimeError("falha essencial: load 1/5/15 indisponível")
    if memory.get("used_mb") is None or memory.get("percent") is None:
        raise RuntimeError("falha essencial: memória do host indisponível")
    if root is None or root.get("percent") is None or root.get("free") is None:
        raise RuntimeError("falha essencial: filesystem / indisponível")


def initialize_health_schema(conn):
    """Adiciona os metadados de validade sem afetar bancos existentes."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS health (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            load_1m REAL, load_5m REAL, load_15m REAL,
            memory_used_mb INTEGER, memory_percent REAL,
            cpu_temp REAL, nvme_temp REAL,
            root_used_percent REAL, root_free_bytes INTEGER,
            updates_available INTEGER,
            updates_important INTEGER,
            tb_memory_mb REAL, pg_memory_mb REAL,
            tb_database_mb REAL, pg_volume_mb REAL
        )"""
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(health)")}
    if "pg_volume_mb" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN pg_volume_mb REAL")
        columns.add("pg_volume_mb")
    if "collection_status" not in columns:
        conn.execute(
            "ALTER TABLE health ADD COLUMN collection_status "
            "TEXT NOT NULL DEFAULT 'ok'"
        )
    if "missing_components" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN missing_components TEXT")
    if "updates_important" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN updates_important INTEGER")


def format_disk_free(kb):
    """Formata espaço livre de forma amigável."""
    if kb >= 1024 * 1024:
        return f"{kb / (1024 * 1024):.0f}G"
    elif kb >= 1024:
        return f"{kb / 1024:.0f}M"
    else:
        return f"{kb}K"


def format_memory_value(value):
    """Formata uma quantidade de memória armazenada em MB."""

    if value is None:
        return "desconhecido"

    if value >= 1024:
        return f"{value / 1024:.2f} GiB"

    return f"{value:.1f} MB"

def get_temperatures():
    """Obtém temperaturas relevantes através do lm-sensors."""
    try:
        result = subprocess.run(["sensors"], capture_output=True, text=True)
    except OSError:
        return {}

    if result.returncode != 0:
        return {}

    temperatures = {}

    for line in result.stdout.splitlines():

        # CPU Package
        if "Package id 0:" in line:
            parts = line.split()

            if len(parts) >= 4:
                try:
                    temperatures["cpu"] = float(
                        parts[3].replace("+", "").replace("°C", "")
                    )
                except ValueError:
                    pass

        # NVMe Composite
        elif "Composite:" in line:
            parts = line.split()

            if len(parts) >= 2:
                try:
                    temperatures["nvme"] = float(
                        parts[1].replace("+", "").replace("°C", "")
                    )
                except ValueError:
                    pass

    return temperatures


def get_container_info(container):
    """Obtém estado e health de um container Docker."""
    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
                container,
            ],
            capture_output=True,
            text=True,
        )
    except OSError:
        return None, None

    if result.returncode != 0:
        error = (result.stderr or "").lower()
        if "no such object" in error or "no such container" in error:
            return "missing", "none"
        return None, None

    output = result.stdout.strip()

    if "|" not in output:
        return None, None

    status, health = output.split("|", 1)

    return status, health


def docker_is_running():
    """Verifica se o daemon Docker está acessível."""
    try:
        result = subprocess.run(
            ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except OSError:
        return False

    return result.returncode == 0


def get_container_memory(container):
    """Obtém o consumo instantâneo de memória de um container Docker, em MB."""

    try:
        result = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.MemUsage}}",
                container,
            ],
            capture_output=True,
            text=True,
        )
    except OSError:
        return None

    if result.returncode != 0:
        return None

    value = result.stdout.strip()

    if "/" in value:
        value = value.split("/", 1)[0].strip()

    # Converte a unidade retornada pelo Docker para MB.
    try:
        if value.endswith("GiB"):
            return float(value[:-3]) * 1024

        if value.endswith("MiB"):
            return float(value[:-3])

        if value.endswith("KiB"):
            return float(value[:-3]) / 1024

        if value.endswith("GB"):
            return float(value[:-2]) * 1000

        if value.endswith("MB"):
            return float(value[:-2])

        if value.endswith("kB"):
            return float(value[:-2]) / 1000

        if value.endswith("B"):
            return float(value[:-1]) / (1024 * 1024)

    except ValueError:
        return None

    return None


def collect_docker():
    """Coleta somente dados atuais; falha de acesso não consulta valores antigos."""
    docker = {
        "daemon": None,
        "thingsboard": {"status": None, "health": None},
        "postgresql": {"status": None, "health": None},
    }
    if not docker_is_running():
        docker["missing_components"] = {
            "docker": {"status": "failed", "reason": "daemon indisponível"},
            "postgresql": {"status": "failed", "reason": "Docker indisponível"},
        }
        return docker, False

    tb_status, tb_health = get_container_info("thingsboard-thingsboard-ce-1")
    pg_status, pg_health = get_container_info("thingsboard-postgres-1")
    postgresql = (
        get_postgresql_info()
        if pg_status == "running"
        else {"database_mb": None, "volume_mb": None, "tables": []}
    )
    docker.update({
        "daemon": "running",
        "thingsboard": {
            "status": tb_status,
            "health": tb_health,
            "memory": get_container_memory("thingsboard-thingsboard-ce-1")
            if tb_status == "running" else None,
        },
        "postgresql": {
            "status": pg_status,
            "health": pg_health,
            "database_mb": postgresql["database_mb"],
            "volume_mb": postgresql["volume_mb"],
            "tables": postgresql["tables"],
            "memory": get_container_memory("thingsboard-postgres-1")
            if pg_status == "running" else None,
        },
    })
    missing = {}
    if tb_status is None:
        missing["docker_thingsboard"] = {
            "status": "failed", "reason": "falha ao consultar estado do container"
        }
    if pg_status is None:
        missing["docker_postgresql"] = {
            "status": "failed", "reason": "falha ao consultar estado do container"
        }
    if pg_status == "running":
        if postgresql["database_mb"] is None:
            missing["postgresql_database"] = {
                "status": "failed", "reason": "consulta/parser indisponível"
            }
        if postgresql["tables"] is None:
            missing["postgresql_tables"] = {
                "status": "failed", "reason": "consulta/parser indisponível"
            }
        if postgresql["volume_mb"] is None:
            missing["postgresql_volume"] = {
                "status": "failed",
                "reason": (
                    "medição requer privilégios de administrador"
                    if postgresql.get("volume_requires_admin")
                    else "consulta do volume indisponível"
                ),
            }
        if docker["postgresql"]["memory"] is None:
            missing["postgresql_memory"] = {
                "status": "failed", "reason": "docker stats indisponível"
            }
    if tb_status == "running" and docker["thingsboard"]["memory"] is None:
        missing["thingsboard_memory"] = {
            "status": "failed", "reason": "docker stats indisponível"
        }
    docker["missing_components"] = missing
    return docker, True


def finalize_collection(health, save=False, db_path=None):
    """Valida e, se solicitado, persiste a coleta antes de retornar sucesso."""
    get_collection_status(health)
    try:
        validate_essential_collection(health)
        if save:
            save_health(health, db_path=db_path)
    except Exception:
        health["collection_status"] = "failed"
        raise
    return 0

def postgresql_volume_requires_admin(result=None):
    """Diz se a medição do volume depende de privilégio que o processo não tem.

    Interpreta apenas o que já aconteceu: usa a saída do `du` que falhou e,
    quando a ferramenta nem executou, a permissão real do processo atual
    sobre o caminho do volume. Nenhum comando privilegiado é executado e
    falhas que não são de permissão continuam com o diagnóstico habitual.
    """
    if result is not None:
        detail = f"{result.stderr or ''}\n{result.stdout or ''}".lower()
        return any(marker in detail for marker in PERMISSION_DENIED_MARKERS)

    return not os.access(POSTGRESQL_VOLUME_PATH, os.R_OK | os.X_OK)


def get_postgresql_info():
    """Obtém tamanho do banco ThingsBoard e do volume PostgreSQL."""

    info = {
        "database_mb": None,
        "volume_mb": None,
        "tables": [],
    }

    # Tamanho lógico do banco ThingsBoard
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "thingsboard-postgres-1",
                "psql",
                "-U",
                "postgres",
                "-d",
                "thingsboard",
                "-tAc",
                "SELECT pg_database_size('thingsboard');",
            ],
            capture_output=True,
            text=True,
        )
    except OSError:
        result = None

    if result is not None and result.returncode == 0:
        try:
            bytes_size = int(result.stdout.strip())
            info["database_mb"] = bytes_size / (1024**2)
        except ValueError:
            print(
                f"ERRO ThingsBoard DB: stdout={result.stdout!r} "
                f"stderr={result.stderr!r}"
            )
    else:
        print(
            "ERRO psql: comando indisponível"
            if result is None
            else f"ERRO psql: returncode={result.returncode} "
                 f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )

    # Tamanho físico do volume PostgreSQL
    try:
        result = subprocess.run(
            ["du", "-sm", POSTGRESQL_VOLUME_PATH],
            capture_output=True,
            text=True,
        )
    except OSError:
        result = None

    if result is not None and result.returncode == 0:
        try:
            info["volume_mb"] = int(result.stdout.split()[0])
        except (ValueError, IndexError):
            print(
                f"ERRO Volume PostgreSQL: stdout={result.stdout!r} "
                f"stderr={result.stderr!r}"
            )
    elif postgresql_volume_requires_admin(result):
        # O PostgreSQL e o volume estão saudáveis: o que falta é privilégio
        # para medir o tamanho físico. Não é falha do banco nem do volume.
        info["volume_requires_admin"] = True

    # Informações das tabelas PostgreSQL
    info["tables"] = get_postgresql_tables()

    return info


def get_postgresql_tables():
    """Obtém estatísticas das tabelas do PostgreSQL."""

    query = """
    SELECT
        schemaname,
        relname,
        pg_total_relation_size(relid),
        n_live_tup,
        n_dead_tup,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        autovacuum_count,
        last_autovacuum,
        last_autoanalyze
    FROM pg_stat_user_tables
    ORDER BY schemaname, relname;
    """

    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "thingsboard-postgres-1",
                "psql",
                "-U",
                "postgres",
                "-d",
                "thingsboard",
                "-A",
                "-F",
                "|",
                "-t",
                "-c",
                query,
            ],
            capture_output=True,
            text=True,
        )
    except OSError:
        return None

    if result.returncode != 0:
        print(
            f"ERRO PostgreSQL tables: returncode={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        return None

    tables = []

    for line in result.stdout.splitlines():

        if not line.strip():
            continue

        fields = line.split("|")

        if len(fields) != 11:
            return None

        try:
            tables.append(
                {
                    "schema_name": fields[0],
                    "table_name": fields[1],
                    "total_bytes": int(fields[2]),
                    "live_tuples": int(fields[3]),
                    "dead_tuples": int(fields[4]),
                    "tuples_inserted": int(fields[5]),
                    "tuples_updated": int(fields[6]),
                    "tuples_deleted": int(fields[7]),
                    "autovacuum_count": int(fields[8]),
                    "last_autovacuum": fields[9] or None,
                    "last_autoanalyze": fields[10] or None,
                }
            )

        except ValueError:
            return None

    return tables

def get_listening_ports():
    """Retorna o conjunto de portas TCP em escuta."""
    try:
        result = subprocess.run(["ss", "-lnt"], capture_output=True, text=True)
    except OSError:
        return None

    if result.returncode != 0:
        return None

    lines = result.stdout.splitlines()
    if not lines or not lines[0].strip().startswith("State"):
        return None

    ports = set()

    for line in lines[1:]:
        fields = line.split()

        if len(fields) < 4:
            return None

        local_address = fields[3]

        if ":" not in local_address:
            return None

        port = local_address.rsplit(":", 1)[-1]

        if not port.isdigit():
            return None
        ports.add(int(port))

    return ports


def get_closed_ports(listening_ports, expected_ports):
    """None indica falha de consulta; set vazio é uma consulta válida."""
    if listening_ports is None:
        return None
    return set(expected_ports) - listening_ports


def get_tailscale_ip():
    """Obtém o IPv4 do Tailscale."""
    try:
        result = subprocess.run(
            ["tailscale", "ip", "-4"], capture_output=True, text=True
        )
    except OSError:
        return None

    if result.returncode != 0:
        return None

    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            return line

    return None


def safe_mtime(path):
    """mtime utilizável para ordenação; arquivo ilegível não interrompe a coleta."""
    try:
        return os.path.getmtime(path)
    except OSError:
        return -1


def get_backups(backup_dir):
    """Obtém informações sobre os backups do ThingsBoard."""
    backup_dir = os.path.expanduser(backup_dir)

    if not os.path.isdir(backup_dir):
        return None

    try:
        names = os.listdir(backup_dir)
    except OSError:
        return None

    files = []

    for name in names:
        if not name.startswith("thingsboard_") or not name.endswith(".sql.gz"):
            continue

        path = os.path.join(backup_dir, name)

        if os.path.isfile(path):
            files.append(path)

    if not files:
        return {
            "count": 0,
            "latest": None,
        }

    latest = max(files, key=safe_mtime)

    return {
        "count": len(files),
        "latest": latest,
    }


def format_age(seconds):
    """Formata idade em horas/dias."""
    hours = int(seconds // 3600)

    if hours < 24:
        return f"{hours}h"

    days = hours // 24
    remaining_hours = hours % 24

    if remaining_hours:
        return f"{days}d {remaining_hours}h"

    return f"{days}d"


def format_size(bytes_size):
    """Formata tamanho de arquivo."""
    if bytes_size >= 1024**3:
        return f"{bytes_size / (1024 ** 3):.1f}G"

    if bytes_size >= 1024**2:
        return f"{bytes_size / (1024 ** 2):.0f}M"

    if bytes_size >= 1024:
        return f"{bytes_size / 1024:.0f}K"

    return f"{bytes_size}B"


def is_important_suite(suite):
    """Diz se o pocket de origem caracteriza uma atualização importante.

    Critério: vem do pocket de segurança do Ubuntu (sufixo `-security`),
    o mesmo sinal usado pelo unattended-upgrades. É o único discriminador
    disponível na saída de `apt list --upgradable`; alterá-lo aqui muda a
    classificação em todo o sistema.
    """
    return bool(suite) and suite.endswith(SECURITY_POCKET_SUFFIX)


def parse_upgradable_list(stdout):
    """Conta pacotes atualizáveis e quantos vêm do pocket de segurança.

    Isolado do subprocesso para poder ser exercitado sem o APT.
    """
    total = 0
    important = 0

    for line in stdout.splitlines():
        if not line or line.startswith("Listing..."):
            continue

        total += 1

        name, _, remainder = line.partition("/")
        if not name or not remainder:
            continue

        suite = remainder.split(" ", 1)[0]

        if is_important_suite(suite):
            important += 1

    return {"total": total, "important": important}


def get_updates():
    """Obtém pacotes atualizáveis, separando os de origem de segurança.

    Devolve {"total": int, "important": int} ou None quando o APT não pôde
    ser consultado. `apt list --upgradable` expõe o pocket no campo
    `pacote/pocket`, o que permite distinguir atualizações importantes.
    """
    try:
        result = subprocess.run(
            ["apt", "list", "--upgradable"],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None

    if result.returncode != 0:
        return None

    return parse_upgradable_list(result.stdout)

def save_health(health, db_path=None):
    """Grava a coleta no histórico do neo-health."""

    db_path = db_path or DB_PATH

    load = health["system"]["load"]
    memory = health["system"]["memory"]
    disk = health["disks"]["/"]
    temperatures = health["temperatures"]
    docker = health["docker"]
    updates = health["updates"]

    tb_memory = docker["thingsboard"].get("memory")
    pg_memory = docker["postgresql"].get("memory")

    with sqlite3.connect(db_path) as conn:
        initialize_health_schema(conn)

        # ----------------------------------------------------
        # Histórico geral
        # ----------------------------------------------------

        cursor = conn.execute(
            """
            INSERT INTO health (
                timestamp,
                load_1m,
                load_5m,
                load_15m,
                memory_used_mb,
                memory_percent,
                cpu_temp,
                nvme_temp,
                root_used_percent,
                root_free_bytes,
                updates_available,
                updates_important,
                tb_memory_mb,
                pg_memory_mb,
                tb_database_mb,
                pg_volume_mb,
                collection_status,
                missing_components
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                health["timestamp"].isoformat(),
                load[0],
                load[1],
                load[2],
                memory["used_mb"],
                memory["percent"],
                temperatures.get("cpu"),
                temperatures.get("nvme"),
                disk["percent"],
                disk["free"] * 1024,
                updates["total"] if updates else None,
                updates["important"] if updates else None,
                tb_memory,
                pg_memory,
                docker["postgresql"].get("database_mb"),
                docker["postgresql"].get("volume_mb"),
                health["collection_status"],
                json.dumps(health["missing_components"], ensure_ascii=False)
                if health["missing_components"] else None,
            ),
        )

        health_id = cursor.lastrowid

        # ----------------------------------------------------
        # Histórico das tabelas PostgreSQL
        # ----------------------------------------------------

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS postgresql_table_health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                health_id INTEGER,
                timestamp TEXT NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                total_bytes INTEGER,
                live_tuples INTEGER,
                dead_tuples INTEGER,
                tuples_inserted INTEGER,
                tuples_updated INTEGER,
                tuples_deleted INTEGER,
                autovacuum_count INTEGER,
                last_autovacuum TEXT,
                last_autoanalyze TEXT
            )
            """
        )

        for table in docker["postgresql"].get("tables") or []:

            conn.execute(
                """
                INSERT INTO postgresql_table_health (
                    health_id,
                    timestamp,
                    schema_name,
                    table_name,
                    total_bytes,
                    live_tuples,
                    dead_tuples,
                    tuples_inserted,
                    tuples_updated,
                    tuples_deleted,
                    autovacuum_count,
                    last_autovacuum,
                    last_autoanalyze
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    health_id,
                    health["timestamp"].isoformat(),
                    table["schema_name"],
                    table["table_name"],
                    table["total_bytes"],
                    table["live_tuples"],
                    table["dead_tuples"],
                    table["tuples_inserted"],
                    table["tuples_updated"],
                    table["tuples_deleted"],
                    table["autovacuum_count"],
                    table["last_autovacuum"],
                    table["last_autoanalyze"],
                ),
            )

###################################
#
#  main()
#
###################################

def main(save=False, db_path=None):

    print()
    print("=" * 60)
    print("              NEO — RELATÓRIO DE SAÚDE")
    print(f"              {datetime.now():%d/%m/%Y %H:%M:%S}")
    print("=" * 60)

    # --------------------------------------------------------
    # SISTEMA
    # --------------------------------------------------------

    section("SISTEMA")

    hostname = socket.gethostname()
    uptime = run_command_or_none(["uptime", "-p"])

    # Load average do kernel: número de tarefas em execução ou aguardando
    # (1/5/15 minutos), exatamente como o sistema operacional o reporta.
    # NÃO é percentual de uso de CPU: por isso o valor não é dividido pelo
    # número de CPUs nem recebe o símbolo "%". O mesmo valor bruto é o que
    # já é persistido em health.db (load_1m/load_5m/load_15m).
    load = os.getloadavg()

    load_text = (
        f"{load[0]:.1f} / "
        f"{load[1]:.1f} / "
        f"{load[2]:.1f}  (1/5/15m)"
    )

    (
        memory_total,
        memory_used,
        memory_percent,
        swap_total,
        swap_used,
    ) = get_memory()

    memory = {
        "total_mb": memory_total,
        "used_mb": memory_used,
        "percent": memory_percent,
    }

    print(f"  {'Host':<20} {hostname}")
    print(f"  {'Uptime':<20} {uptime if uptime is not None else 'desconhecido'}")
    print(f"  {'Load':<20} {load_text}")

    if memory_percent >= 90:
        warn("Memory", f"{memory_used} / {memory_total} MB " f"({memory_percent:.0f}%)")
    else:
        ok("Memory", f"{memory_used} / {memory_total} MB " f"({memory_percent:.0f}%)")

    if swap_total > 0:
        swap_percent = swap_used * 100 / swap_total

        if swap_used > 0:
            warn("Swap", f"{swap_used} / {swap_total} MB " f"({swap_percent:.0f}%)")
        else:
            ok("Swap", f"0 / {swap_total} MB (0%)")

    disks = {"/": get_disk("/")}
    for path in ("/boot", "/boot/efi"):
        try:
            disks[path] = get_disk(path)
        except (OSError, subprocess.SubprocessError, ValueError, IndexError):
            disks[path] = None

    for path, disk in disks.items():

        if disk is None:
            warn(path, "medição indisponível")
            continue

        name = path

        message = (
            f"{disk['percent']}% usado " f"({format_disk_free(disk['free'])} livres)"
        )

        if disk["percent"] >= 90:
            fail(name, message)
        elif disk["percent"] >= 80:
            warn(name, message)
        else:
            ok(name, message)

    # --------------------------------------------------------
    # TEMPERATURAS
    # --------------------------------------------------------

    section("TEMPERATURAS")

    temperatures = get_temperatures()

    cpu_temp = temperatures.get("cpu")
    nvme_temp = temperatures.get("nvme")

    if cpu_temp is None:
        warn("CPU", "sensor não disponível")
    elif cpu_temp >= 100:
        fail("CPU", f"{cpu_temp:.1f} °C")
    elif cpu_temp >= 90:
        warn("CPU", f"{cpu_temp:.1f} °C")
    elif cpu_temp >= 80:
        warn("CPU", f"{cpu_temp:.1f} °C")
    else:
        ok("CPU", f"{cpu_temp:.1f} °C")

    if nvme_temp is None:
        warn("NVMe", "sensor não disponível")
    elif nvme_temp >= 84:
        fail("NVMe", f"{nvme_temp:.1f} °C")
    elif nvme_temp >= 80:
        warn("NVMe", f"{nvme_temp:.1f} °C")
    elif nvme_temp >= 70:
        warn("NVMe", f"{nvme_temp:.1f} °C")
    else:
        ok("NVMe", f"{nvme_temp:.1f} °C")

    # --------------------------------------------------------
    # ATUALIZAÇÕES
    # --------------------------------------------------------

    section("ATUALIZAÇÕES")

    updates = get_updates()

    if updates is None:

        warn("Pacotes", "não foi possível verificar")

    elif updates["total"] == 0:

        ok("Pacotes", "sistema atualizado")

    else:

        total = updates["total"]
        important = updates["important"]

        # Atualizações comuns são informação, nunca alerta. As de segurança
        # são destacadas à parte, mantendo a classificação "-security".
        if total == 1:
            info("Pacotes", "1 atualização disponível")
        else:
            info("Pacotes", f"{total} atualizações disponíveis")

        if important == 1:
            warn("Segurança", "1 atualização de segurança")
        elif important > 1:
            warn("Segurança", f"{important} atualizações de segurança")

    # --------------------------------------------------------
    # DOCKER
    # --------------------------------------------------------

    section("DOCKER")

    docker, docker_available = collect_docker()
    if docker_available:

        ok("Docker daemon", "running")
        postgresql = docker["postgresql"]
        tb_status = docker["thingsboard"]["status"]
        tb_health = docker["thingsboard"]["health"]
        pg_status = docker["postgresql"]["status"]
        pg_health = docker["postgresql"]["health"]
        tb_memory = docker["thingsboard"].get("memory")
        pg_memory = docker["postgresql"].get("memory")

        if tb_status == "running":
            ok("ThingsBoard", "running")
        else:
            fail("ThingsBoard", tb_status or "missing")

        if pg_status == "running" and pg_health == "healthy":
            ok("PostgreSQL", "running / healthy")
            if postgresql["database_mb"] is not None:
                ok("ThingsBoard DB", f"{postgresql['database_mb']:.0f} MB")

            if postgresql["volume_mb"] is not None:
                ok("PG volume", f"{postgresql['volume_mb']:.0f} MB")

            if tb_memory:
                ok("TB memória", format_memory_value(tb_memory))

            if pg_memory:
                ok("PG memória", format_memory_value(pg_memory))

        elif pg_status == "running":
            warn("PostgreSQL", f"running / health={pg_health}")
        else:
            fail("PostgreSQL", pg_status or "missing")

    else:

        fail("Docker daemon", "não está acessível")
        postgresql = docker["postgresql"]

    # --------------------------------------------------------
    # SERVIÇOS / PORTAS
    # --------------------------------------------------------

    section("SERVIÇOS / PORTAS")

    listening_ports = get_listening_ports()

    ports = {
        8080: "ThingsBoard HTTP",
        1883: "MQTT",
        8883: "MQTT/TLS",
    }

    closed_ports = get_closed_ports(listening_ports, ports)
    if closed_ports is None:
        warn("Portas", "não foi possível consultar ss")
    else:
        for port, name in ports.items():

            if port not in closed_ports:
                ok(name, f":{port}")
            else:
                fail(name, f":{port} não está escutando")

    # --------------------------------------------------------
    # TAILSCALE
    # --------------------------------------------------------

    section("TAILSCALE")

    tailscale_state = get_systemd_unit_state("tailscaled")

    tailscale = {
        "active": tailscale_state,
        "ip": None,
    }

    if tailscale_state is None:
        warn("tailscaled", "não foi possível consultar o systemd")

    elif tailscale_state:

        ts_ip = get_tailscale_ip()

        if ts_ip:
            tailscale["ip"] = ts_ip
            ok("tailscaled", ts_ip)
        else:
            warn("tailscaled", "ativo, mas sem IPv4 Tailscale")

    else:
        fail("tailscaled", "não está ativo")

    # --------------------------------------------------------
    # BACKUPS
    # --------------------------------------------------------

    section("BACKUPS")

    backup_dir = os.path.expanduser("/home/jc/thingsboard/backups")
    backups = get_backups(backup_dir)

    if backups is None:

        fail("Diretório", f"{backup_dir} não existe")

    elif backups["count"] == 0:

        fail("Backups", "nenhum encontrado")

    else:

        latest = backups["latest"]
        count = backups["count"]

        try:
            age = datetime.now().timestamp() - os.path.getmtime(latest)
            size = os.path.getsize(latest)
        except OSError:
            fail("Backups", "não foi possível ler o backup mais recente")
        else:
            print(f"  {'Último backup':<20} {os.path.basename(latest)}")
            print(f"  {'Idade':<20} {format_age(age)}")
            print(f"  {'Tamanho':<20} {format_size(size)}")
            print(f"  {'Quantidade':<20} {count}")

            try:
                with gzip.open(latest, "rb") as f:
                    while f.read(1024 * 1024):
                        pass

                ok("Integridade", "gzip OK")

            except (OSError, EOFError):
                fail("Integridade", "backup corrompido")

            age_hours = age / 3600

            if age_hours >= 48:
                fail("Atualidade", f"último backup há {age_hours:.0f}h")

            elif age_hours >= 26:
                warn("Atualidade", f"último backup há {age_hours:.0f}h")

            else:
                ok("Atualidade", "backup recente")

    # --------------------------------------------------------
    # BACKUP TIMER
    # --------------------------------------------------------

    section("BACKUP TIMER")

    timer_state = get_systemd_unit_state("thingsboard-backup.timer")
    timer_query_failed = timer_state is None

    timer = {
        "active": timer_state,
        "next_backup": None,
    }

    if timer_state is None:
        warn("Timer", "não foi possível consultar o systemd")

    elif timer_state:

        try:
            result = subprocess.run(
                [
                    "systemctl",
                    "list-timers",
                    "thingsboard-backup.timer",
                    "--no-legend",
                    "--no-pager",
                ],
                capture_output=True,
                text=True,
            )
        except OSError:
            result = None

        if result is None:
            timer_query_failed = True
            warn("Next backup", "não foi possível consultar os timers")

        else:
            fields = result.stdout.split()

            if len(fields) >= 3:
                next_backup = " ".join(fields[:3])
                timer["next_backup"] = next_backup
            else:
                next_backup = "desconhecido"

            ok("Timer", "active")
            ok("Next backup", next_backup)

    else:

        fail("Timer", "inactive")

    missing_components = dict(docker.get("missing_components", {}))
    if temperatures.get("cpu") is None:
        missing_components["cpu_temp"] = {
            "status": "unsupported", "reason": "sensor ausente ou não suportado"
        }
    if closed_ports is None:
        missing_components["ports"] = {
            "status": "failed", "reason": "ss indisponível ou saída inválida"
        }
    if uptime is None:
        missing_components["uptime"] = {
            "status": "failed", "reason": "comando uptime indisponível"
        }
    if tailscale_state is None:
        missing_components["tailscale"] = {
            "status": "failed", "reason": "systemd indisponível para tailscaled"
        }
    if timer_query_failed:
        missing_components["backup_timer"] = {
            "status": "failed", "reason": "systemd indisponível para o timer"
        }

    collection_status = get_collection_status_value(missing_components)

    health = {
        "timestamp": datetime.now(),
        "system": {
            "hostname": hostname,
            "uptime": uptime,
            "load": load,
            "memory": memory,
        },
        "disks": disks,
        "temperatures": temperatures,
        "updates": updates,
        "docker": docker,
        "ports": ports,
        "tailscale": tailscale,
        "backups": backups,
        "timer": timer,
        "collection_status": collection_status,
        "missing_components": missing_components,
    }

    if collection_status == "partial":
        # Texto legível para o operador; o JSON interno permanece apenas no
        # histórico (missing_components) e nunca é impresso.
        print()
        print("⚠ Coleta parcial")
        for line in describe_missing_components(missing_components):
            print(line)

    return finalize_collection(health, save=save, db_path=db_path)


def cli(argv=None):
    parser = argparse.ArgumentParser(description="Coleta de saúde do Neo")
    parser.add_argument(
        "--save",
        action="store_true",
        help="salva a coleta no histórico do neo-health",
    )
    parser.add_argument("--db", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        return main(save=args.save, db_path=args.db)
    except Exception as error:
        print(f"Falha na coleta essencial do neo-health: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())

