#!/usr/bin/env python3

import os
import socket
import sqlite3
import subprocess
import gzip
import sys
from datetime import datetime

# ============================================================
# Neo Health Check
# ============================================================


def run_command(command):
    """Executa um comando do sistema e devolve sua saída."""
    result = subprocess.run(command, capture_output=True, text=True)
    return result.stdout.strip()


def section(title):
    print()
    print(f"── {title} " + "─" * max(0, 54 - len(title)))


def ok(name, message=""):
    print(f"  {name:<20} ✓ {message}")


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
    result = subprocess.run(["sensors"], capture_output=True, text=True)

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

    if result.returncode != 0:
        return None, None

    output = result.stdout.strip()

    if "|" not in output:
        return None, None

    status, health = output.split("|", 1)

    return status, health


def docker_is_running():
    """Verifica se o daemon Docker está acessível."""
    result = subprocess.run(
        ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    return result.returncode == 0


def get_container_memory(container):
    """Obtém o consumo instantâneo de memória de um container Docker, em MB."""

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

def get_postgresql_info():
    """Obtém tamanho do banco ThingsBoard e do volume PostgreSQL."""

    info = {
        "database_mb": None,
        "volume_mb": None,
        "tables": [],
    }

    # Tamanho lógico do banco ThingsBoard
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

    if result.returncode == 0:
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
            f"ERRO psql: returncode={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )

    # Tamanho físico do volume PostgreSQL
    result = subprocess.run(
        ["du", "-sm", "/var/lib/docker/volumes/tb-postgres-data/_data"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        try:
            info["volume_mb"] = int(result.stdout.split()[0])
        except (ValueError, IndexError):
            print(
                f"ERRO Volume PostgreSQL: stdout={result.stdout!r} "
                f"stderr={result.stderr!r}"
            )

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

    if result.returncode != 0:
        print(
            f"ERRO PostgreSQL tables: returncode={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        return []

    tables = []

    for line in result.stdout.splitlines():

        if not line.strip():
            continue

        fields = line.split("|")

        if len(fields) != 11:
            continue

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
            continue

    return tables

def get_listening_ports():
    """Retorna o conjunto de portas TCP em escuta."""
    result = subprocess.run(["ss", "-lnt"], capture_output=True, text=True)

    if result.returncode != 0:
        return set()

    ports = set()

    for line in result.stdout.splitlines()[1:]:
        fields = line.split()

        if len(fields) < 4:
            continue

        local_address = fields[3]

        if ":" not in local_address:
            continue

        port = local_address.rsplit(":", 1)[-1]

        if port.isdigit():
            ports.add(int(port))

    return ports


def get_tailscale_ip():
    """Obtém o IPv4 do Tailscale."""
    result = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True)

    if result.returncode != 0:
        return None

    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            return line

    return None


def get_backups(backup_dir):
    """Obtém informações sobre os backups do ThingsBoard."""
    backup_dir = os.path.expanduser(backup_dir)

    if not os.path.isdir(backup_dir):
        return None

    files = []

    for name in os.listdir(backup_dir):
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

    latest = max(files, key=os.path.getmtime)

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


def get_updates():
    """Obtém a quantidade de pacotes atualizáveis."""
    result = subprocess.run(
        ["apt", "list", "--upgradable"], capture_output=True, text=True, timeout=15
    )

    if result.returncode != 0:
        return None

    count = 0

    for line in result.stdout.splitlines():
        if line and not line.startswith("Listing..."):
            count += 1

    return count

def save_health(health):
    """Grava a coleta no histórico do neo-health."""

    db_path = "/opt/neo-health/health.db"

    load = health["system"]["load"]
    memory = health["system"]["memory"]
    disk = health["disks"]["/"]
    temperatures = health["temperatures"]
    docker = health["docker"]

    tb_memory = docker["thingsboard"].get("memory")
    pg_memory = docker["postgresql"].get("memory")

    with sqlite3.connect(db_path) as conn:

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
                tb_memory_mb,
                pg_memory_mb,
                tb_database_mb,
                pg_volume_mb
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                health["updates"],
                tb_memory,
                pg_memory,
                docker["thingsboard"].get("database_mb"),
                docker["postgresql"].get("volume_mb"),
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

        for table in docker["postgresql"].get("tables", []):

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

def main(save=False):

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
    uptime = run_command(["uptime", "-p"])

    cpu_count = os.cpu_count()
    load = os.getloadavg()

    load_text = (
        f"{load[0] / cpu_count * 100:.1f}% / "
        f"{load[1] / cpu_count * 100:.1f}% / "
        f"{load[2] / cpu_count * 100:.1f}%  (1/5/15m)"
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
    print(f"  {'Uptime':<20} {uptime}")
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

    disks = {
        "/": get_disk("/"),
        "/boot": get_disk("/boot"),
        "/boot/efi": get_disk("/boot/efi"),
    }

    for path, disk in disks.items():

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

    elif updates == 0:

        ok("Pacotes", "sistema atualizado")

    else:

        warn("Pacotes", f"{updates} atualizações disponíveis")

    # --------------------------------------------------------
    # DOCKER
    # --------------------------------------------------------

    section("DOCKER")

    postgresql = get_postgresql_info()

    tb_memory = get_container_memory("thingsboard-thingsboard-ce-1")
    pg_memory = get_container_memory("thingsboard-postgres-1")

    docker = {
        "daemon": None,
        "thingsboard": {
            "status": None,
            "health": None,
        },
        "postgresql": {
            "status": None,
            "health": None,
        },
    }

    if docker_is_running():

        ok("Docker daemon", "running")

        tb_status, tb_health = get_container_info("thingsboard-thingsboard-ce-1")

        pg_status, pg_health = get_container_info("thingsboard-postgres-1")

        docker["daemon"] = "running"

        docker["thingsboard"] = {
            "status": tb_status,
            "health": tb_health,
        }

        docker["postgresql"] = {
            "status": pg_status,
            "health": pg_health,
        }

        docker["postgresql"]["database_mb"] = postgresql["database_mb"]
        docker["postgresql"]["volume_mb"] = postgresql["volume_mb"]
        docker["postgresql"]["tables"] = postgresql["tables"]
        docker["thingsboard"]["memory"] = tb_memory
        docker["postgresql"]["memory"] = pg_memory

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

    for port, name in ports.items():

        if port in listening_ports:
            ok(name, f":{port}")
        else:
            fail(name, f":{port} não está escutando")

    # --------------------------------------------------------
    # TAILSCALE
    # --------------------------------------------------------

    section("TAILSCALE")

    tailscale_active = (
        subprocess.run(["systemctl", "is-active", "--quiet", "tailscaled"]).returncode
        == 0
    )
    tailscale = {
        "active": tailscale_active,
        "ip": None,
    }

    if tailscale_active:

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

        age = datetime.now().timestamp() - os.path.getmtime(latest)

        print(f"  {'Último backup':<20} {os.path.basename(latest)}")
        print(f"  {'Idade':<20} {format_age(age)}")
        print(f"  {'Tamanho':<20} {format_size(os.path.getsize(latest))}")
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

    timer_active = (
        subprocess.run(
            ["systemctl", "is-active", "--quiet", "thingsboard-backup.timer"]
        ).returncode
        == 0
    )

    timer = {
        "active": timer_active,
        "next_backup": None,
    }

    if timer_active:

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
    }

    if save:
        save_health(health)


if __name__ == "__main__":
    save = len(sys.argv) > 1 and sys.argv[1] == "--save"
    main(save=save)

