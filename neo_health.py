#!/usr/bin/env python3

import contextlib
import os
import re
import socket
import sqlite3
import subprocess
import gzip
import hashlib
import sys
import argparse
import json
import tarfile
import time
import uuid
import zlib
from datetime import datetime

DB_PATH = "/opt/neo-health/health.db"

# Política única de contenção das conexões ao health.db: sob contenção
# transitória o SQLite espera em vez de falhar imediatamente. Não altera
# journal_mode (DELETE) nem synchronous (FULL), que permanecem os padrões.
DB_BUSY_TIMEOUT_MS = 30000

# Retry limitado, só para operações IDEMPOTENTES sob contenção transitória.
DB_RETRY_ATTEMPTS = 5
DB_RETRY_BASE_DELAY = 0.2
DB_RETRY_MAX_DELAY = 2.0

# Marcadores de contenção transitória do SQLite (lock/busy). Qualquer outro
# erro operacional NÃO é repetido.
DB_CONTENTION_MARKERS = ("database is locked", "database is busy")


def connect_health_db(db_path):
    """Abre uma conexão ao health.db com a política única de contenção.

    `timeout` (segundos) e `PRAGMA busy_timeout` (ms) usam o mesmo valor,
    para que contenção transitória espere em vez de falhar. Cada chamada
    devolve uma conexão PRÓPRIA — nunca compartilhada entre threads ou
    processos. `journal_mode` (DELETE) e `synchronous` (FULL) não são
    alterados.
    """

    conn = sqlite3.connect(db_path, timeout=DB_BUSY_TIMEOUT_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS}")
    return conn


@contextlib.contextmanager
def health_db_connection(db_path):
    """Conexão ao health.db que controla a transação E FECHA ao sair.

    `with sqlite3.connect(...)` controla commit/rollback mas NÃO fecha a
    conexão; este gerenciador garante o fechamento, inclusive em exceção.
    """

    conn = connect_health_db(db_path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def is_contention_error(error):
    """Diz se o erro é contenção transitória (locked/busy) do SQLite."""

    if not isinstance(error, sqlite3.OperationalError):
        return False

    message = str(error).lower()
    return any(marker in message for marker in DB_CONTENTION_MARKERS)


def run_with_retry(operation, *, description, attempts=None,
                   base_delay=None, max_delay=None):
    """Executa `operation` repetindo SÓ sob contenção transitória.

    `operation` precisa ser IDEMPOTENTE: repetir não pode duplicar efeitos.
    Erros que não sejam de contenção propagam imediatamente. O backoff é
    progressivo e limitado. Esgotadas as tentativas, propaga um
    OperationalError com mensagem clara, encadeando o erro original.
    """

    attempts = DB_RETRY_ATTEMPTS if attempts is None else attempts
    base_delay = DB_RETRY_BASE_DELAY if base_delay is None else base_delay
    max_delay = DB_RETRY_MAX_DELAY if max_delay is None else max_delay

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except sqlite3.OperationalError as error:
            if not is_contention_error(error):
                raise
            if attempt == attempts:
                raise sqlite3.OperationalError(
                    f"{description}: banco de dados ocupado após "
                    f"{attempts} tentativas ({error})"
                ) from error
            time.sleep(min(base_delay * (2 ** (attempt - 1)), max_delay))

    raise AssertionError("run_with_retry: estado inalcançável")

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
# BACKUP DIÁRIO DO SISTEMA
#
# O artefato é produzido por /usr/local/sbin/neo-backup-system.sh, que o
# daily_workflow do neo-analysis executa imediatamente antes desta coleta.
# A verificação aqui é INDEPENDENTE daquele processo: o diretório é relido,
# o gzip é percorrido até o fim, o SHA-256 é recalculado a partir do
# arquivo .sha256 e o conteúdo do TAR é conferido. Nada nesta seção pode
# interromper a coleta essencial.
# ============================================================

SYSTEM_BACKUP_DIR = "/mnt/neo-backup/neo-system"

# Volume que precisa estar montado para o backup existir de fato. Um
# diretório vazio (ou com conteúdo antigo do disco raiz) não é um backup
# ausente: é uma verificação que não pôde ser executada.
BACKUP_VOLUME_PATH = "/mnt/neo-backup"

SYSTEM_BACKUP_PREFIX = "neo-system_"
SYSTEM_BACKUP_SUFFIX = ".tar.gz"
SYSTEM_BACKUP_CHECKSUM_SUFFIX = ".sha256"

# O backup é executado imediatamente antes da coleta, então a idade do
# artefato nesta verificação é praticamente zero. Um limite curto detecta
# a falha do dia no próprio dia, sem falso positivo.
BACKUP_MAX_AGE_HOURS = 6

# Bloco de leitura usado no gzip e no cálculo do SHA-256.
HASH_CHUNK_SIZE = 1024 * 1024

# Membros cuja ausência caracteriza backup sem conteúdo essencial.
SYSTEM_BACKUP_REQUIRED_MEMBERS = (
    "opt/neo-health/health.db",
    "opt/neo-analysis/notification_state.db",
    "home/jc/thingsboard/docker-compose.yml",
    "etc/systemd/system/neo-analysis.service",
    "etc/fstab",
)

# O motivo é persistido e depois exibido: mesmo limite dos demais motivos.
MAX_BACKUP_REASON_LENGTH = 200

# Identifica um token que se parece com um caminho de arquivo — Unix
# (/mnt/neo-backup/neo-system), Windows (C:\... ou \\servidor\...) ou um
# membro de TAR (opt/neo-health/health.db). Exige ao menos um separador, e
# recusa tokens encostados em parênteses ou em outros caracteres de
# palavra: assim "(gzip/tar)" e textos sem separador não são tocados.
#
# Espaços em nomes de diretório (ex.: "/mnt/backup externo/arquivo.tar.gz"
# ou "C:\Users\João Campos\backup\arquivo.tar.gz") exigem cuidado: aceitar
# espaços em QUALQUER segmento faria a expressão engolir a prosa vizinha
# ("diretório de backup inacessível: ..."). Por isso os espaços só entram
# em segmentos intermediários — sempre cercados por separadores — e no
# primeiro segmento quando existe um prefixo absoluto inequívoco (/, C:\
# ou \\). O primeiro segmento de um caminho relativo e o ÚLTIMO segmento
# permanecem sem espaços, o que preserva a fronteira com o texto ao redor.
#
# Caminhos RELATIVOS são ambíguos: "e/ou", "TCP/IP", "entrada/saída" e
# "leitura/escrita" têm a mesma forma de "opt/neo-health/health.db". Um
# token relativo só é tratado como caminho quando é inequivocamente um
# caminho do sistema de arquivos: começa por uma raiz conhecida, tem 3+
# segmentos, ou tem 2 segmentos com extensão de arquivo. Assim os membros
# de TAR continuam reconhecidos e as expressões textuais são preservadas.
BACKUP_PATH_ROOTS = (
    "bin|boot|dev|etc|home|lib32|lib64|lib|lost\\+found|media|mnt|opt|"
    "proc|root|run|sbin|srv|sys|tmp|usr|var"
)

BACKUP_PATH_TOKEN = re.compile(
    rf"""
    (?<![\w(])                                 # não encosta em palavra nem '('
    (?:
        (?:
            [A-Za-z]:[\\/]                     # prefixo de unidade (C:\)
          | \\\\                               # prefixo UNC (\\servidor)
          | /                                  # prefixo Unix absoluto
        )
        [\w.\-]+(?:\s+[\w.\-]+)*               # 1º segmento (pode ter espaços)
        (?:[\\/][\w.\-]+(?:\s+[\w.\-]+)*)*     # segmentos intermediários
        [\\/][\w.\-]+                          # último segmento (sem espaços)
      |
        (?:
            (?:{BACKUP_PATH_ROOTS})[\\/]       # raiz de sistema conhecida
            (?:[\w.\-]+(?:\s+[\w.\-]+)*[\\/])* # intermediários
            [\w.\-]+                           # último segmento (sem espaços)
          |
            [\w.\-]+                           # 1º segmento relativo
            (?:[\\/][\w.\-]+(?:\s+[\w.\-]+)*)+ # 2+ intermediários → 3+ segmentos
            [\\/][\w.\-]+                      # último segmento (sem espaços)
          |
            [\w.\-]+[\\/]                      # 2 segmentos, com extensão no
            [\w.\-]*\.[A-Za-z][\w.\-]*         # ... último (nome de arquivo)
        )
    )
    (?![)\w])                                  # não encosta em ')' nem palavra
    """,
    re.VERBOSE,
)


def backup_path_basename(path):
    """Último segmento de um caminho Unix ou Windows; '' quando não houver.

    Preserva a identidade lógica do componente (o nome do arquivo ou do
    diretório) sem expor a estrutura de diretórios que o contém.
    """
    segments = [
        segment
        for segment in str(path).replace("\\", "/").split("/")
        if segment
    ]

    return segments[-1] if segments else ""


def redact_backup_paths(text):
    """Reduz caminhos a seu último segmento, sem expor diretórios internos."""
    return BACKUP_PATH_TOKEN.sub(
        lambda match: backup_path_basename(match.group(0)), text
    )


# A tabela canônica das verificações de backup. A partir desta fase ela é
# a fonte de verdade; as colunas backup_* da tabela `health` continuam
# existindo apenas por compatibilidade com consumidores existentes.
SYSTEM_BACKUP_CHECKS_TABLE = "system_backup_checks"

# Estados possíveis de uma verificação, na mesma semântica de
# collect_system_backup(). NULL (não verificado) não é um estado.
SYSTEM_BACKUP_STATUSES = ("ok", "failed", "missing", "unavailable")

# Prefixo do identificador derivado das linhas históricas migradas.
LEGACY_VERIFICATION_PREFIX = "legacy-"

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
    # Backup diário do sistema. Sem DEFAULT: NULL significa "não
    # verificado", e nenhuma verificação que não ocorreu é registrada
    # como 'ok'. Assim os registros existentes permanecem intocados.
    if "backup_status" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_status TEXT")
    if "backup_artifact" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_artifact TEXT")
    if "backup_size_bytes" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_size_bytes INTEGER")
    if "backup_age_hours" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_age_hours REAL")
    if "backup_gzip_ok" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_gzip_ok INTEGER")
    if "backup_sha256_ok" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_sha256_ok INTEGER")
    if "backup_sha256" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_sha256 TEXT")
    if "backup_reason" not in columns:
        conn.execute("ALTER TABLE health ADD COLUMN backup_reason TEXT")

    # ------------------------------------------------------------
    # Fonte canônica das verificações de backup do sistema
    #
    # A tabela `system_backup_checks` passa a ser a fonte de verdade. As
    # colunas backup_* de `health` são mantidas apenas por compatibilidade
    # e não são mais a referência. `verification_id` identifica a EXECUÇÃO
    # da verificação e é UNIQUE: repetir a persistência da mesma execução
    # não cria outra linha, enquanto duas verificações legítimas do mesmo
    # artefato continuam sendo registros distintos. O `id` AUTOINCREMENT é
    # a sequência determinística de processamento.
    # ------------------------------------------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_backup_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            verification_id TEXT NOT NULL UNIQUE,
            timestamp TEXT NOT NULL,
            status TEXT,
            artifact TEXT,
            size_bytes INTEGER,
            age_hours REAL,
            gzip_ok INTEGER,
            sha256_ok INTEGER,
            sha256 TEXT,
            reason TEXT,
            health_id INTEGER
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS system_backup_checks_health_idx "
        "ON system_backup_checks(health_id)"
    )
    migrate_legacy_backup_checks(conn)


def migrate_legacy_backup_checks(conn):
    """Migra o histórico de health.backup_* para a tabela canônica.

    Executada dentro da inicialização do schema, em transação apropriada:
    uma interrupção não deixa migração parcial. É idempotente porque o
    `verification_id` é derivado do id da coleta (`legacy-<id>`), então
    reexecutar não duplica. Linhas cujo `backup_status` é NULL nunca
    tiveram verificação: nenhum registro artificial é criado para elas e
    as linhas originais de `health` permanecem intocadas.

    Linhas que JÁ têm uma verificação canônica vinculada (`health_id`)
    são ignoradas: a migração é apenas para o histórico anterior, e a
    mesma coleta nunca aparece duas vezes na tabela canônica.
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(health)")}

    if "backup_status" not in columns:
        return 0

    cursor = conn.execute(
        f"""
        INSERT OR IGNORE INTO {SYSTEM_BACKUP_CHECKS_TABLE} (
            verification_id, timestamp, status, artifact, size_bytes,
            age_hours, gzip_ok, sha256_ok, sha256, reason, health_id
        )
        SELECT '{LEGACY_VERIFICATION_PREFIX}' || id, timestamp,
               backup_status, backup_artifact, backup_size_bytes,
               backup_age_hours, backup_gzip_ok, backup_sha256_ok,
               backup_sha256, backup_reason, id
        FROM health
        WHERE backup_status IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {SYSTEM_BACKUP_CHECKS_TABLE}
              WHERE {SYSTEM_BACKUP_CHECKS_TABLE}.health_id = health.id
          )
        """
    )

    return cursor.rowcount if cursor.rowcount is not None else 0


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


def sanitize_backup_reason(reason):
    """Normaliza o motivo, que é persistido e posteriormente exibido.

    Além de colapsar espaços e limitar o tamanho, remove a estrutura de
    diretórios: caminhos absolutos (Unix ou Windows), membros de TAR e
    qualquer caminho interno são reduzidos ao último segmento. O motivo
    continua útil para diagnóstico — a categoria da falha e a identidade
    lógica do componente permanecem — sem expor caminhos do sistema.
    """
    if reason is None:
        return None

    text = " ".join(str(reason).split())
    text = redact_backup_paths(text)

    if len(text) > MAX_BACKUP_REASON_LENGTH:
        text = text[:MAX_BACKUP_REASON_LENGTH] + "…"

    return text or None


def backup_volume_is_mounted(backup_dir=None):
    """Diz se o volume que contém o backup está montado.

    Backup ausente porque o volume não foi montado não é a mesma coisa que
    backup não gerado: sem o volume, a verificação não pôde ser executada.
    """
    path = os.path.abspath(backup_dir or SYSTEM_BACKUP_DIR)

    try:
        return os.path.ismount(os.path.dirname(path))
    except OSError:
        return False


def get_system_backups(backup_dir=None):
    """Obtém o backup do sistema mais recente.

    Devolve {"count": int, "latest": caminho} ou None quando o diretório
    não pôde ser lido. O arquivo .sha256 nunca é candidato: o nome do
    artefato precisa terminar em .tar.gz.
    """
    backup_dir = os.path.expanduser(backup_dir or SYSTEM_BACKUP_DIR)

    if not os.path.isdir(backup_dir):
        return None

    try:
        names = os.listdir(backup_dir)
    except OSError:
        return None

    files = []

    for name in names:
        if not name.startswith(SYSTEM_BACKUP_PREFIX):
            continue

        if not name.endswith(SYSTEM_BACKUP_SUFFIX):
            continue

        path = os.path.join(backup_dir, name)

        if os.path.isfile(path):
            files.append(path)

    if not files:
        return {
            "count": 0,
            "latest": None,
        }

    return {
        "count": len(files),
        "latest": max(files, key=safe_mtime),
    }


def gzip_integrity_ok(path):
    """Percorre o gzip inteiro; corrupção em qualquer ponto reprova.

    `zlib.error` precisa ser tratado aqui: um fluxo com cabeçalho gzip
    válido mas corpo deflate corrompido abre normalmente e falha apenas
    durante a leitura. Sem esta exceção, o erro escaparia da verificação
    e a corrupção seria classificada como indisponibilidade em vez de
    falha de integridade.
    """
    try:
        with gzip.open(path, "rb") as handle:
            while handle.read(HASH_CHUNK_SIZE):
                pass

    except (OSError, EOFError, zlib.error):
        return False

    return True


def sha256_of_file(path):
    """SHA-256 do arquivo, lido em blocos; None se não pôde ser calculado."""
    digest = hashlib.sha256()

    try:
        with open(path, "rb") as handle:
            while True:
                block = handle.read(HASH_CHUNK_SIZE)
                if not block:
                    break
                digest.update(block)

    except OSError:
        return None

    return digest.hexdigest()


def read_checksum_file(path):
    """Lê o arquivo .sha256 no formato produzido por `sha256sum`.

    Devolve (hash, nome_referenciado) ou None quando o arquivo não existe,
    não pôde ser lido ou está malformado. A ausência do checksum não é
    divergência: é impossibilidade de comparar, e o resultado persistido
    fica NULL — nunca 0 e nunca 1.

    O nome referenciado é devolvido de propósito: o checksum só vale para
    o arquivo que ele nomeia. Cabe ao chamador compará-lo com o artefato
    analisado — um nome diferente é divergência, não ausência.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            line = handle.readline().strip()

    except OSError:
        return None

    if not line:
        return None

    parts = line.split()

    if len(parts) < 2:
        return None

    digest = parts[0].lstrip("*")
    name = parts[1].lstrip("*")

    if len(digest) != 64 or not all(
        character in "0123456789abcdefABCDEF" for character in digest
    ):
        return None

    return digest.lower(), name


def normalize_tar_member(name):
    """Normaliza o nome de um membro do TAR para comparação."""
    normalized = str(name).replace("\\", "/")

    while normalized.startswith("./"):
        normalized = normalized[2:]

    return normalized.lstrip("/").rstrip("/")


def tar_missing_members(path, required=None):
    """Devolve os membros essenciais ausentes, ou None se o TAR não abrir.

    Cada membro essencial precisa existir como ARQUIVO REGULAR. Diretório,
    link simbólico, hard link, FIFO ou dispositivo com o mesmo nome não
    substitui o arquivo: só o arquivo carrega o conteúdo que o backup
    promete guardar.
    """
    required = required or SYSTEM_BACKUP_REQUIRED_MEMBERS

    try:
        with tarfile.open(path, "r:gz") as archive:
            members = [
                (normalize_tar_member(info.name), info)
                for info in archive.getmembers()
            ]

    except (OSError, EOFError, zlib.error, tarfile.TarError):
        return None

    missing = []

    for expected in required:
        # Comparação exata ou por sufixo de caminho: o TAR pode ter sido
        # gravado a partir de uma raiz diferente, com prefixo.
        if any(
            (name == expected or name.endswith("/" + expected))
            and info.isfile()
            for name, info in members
        ):
            continue

        missing.append(expected)

    return missing


def collect_system_backup(backup_dir=None):
    """Verifica o backup diário do sistema, sem nunca levantar exceção.

    Estados:
      ok          todas as verificações passaram;
      failed      corrupção (gzip ou TAR), SHA-256 divergente, checksum que
                  nomeia outro arquivo ou conteúdo essencial ausente;
      missing     não existe backup recente — inclusive quando o volume
                  está montado e o diretório de backup ainda não existe;
      unavailable verificação impossível: volume não montado, diretório
                  inacessível, artefato ilegível ou checksum ausente.

    A ausência do arquivo .sha256 não é divergência: é impossibilidade de
    comparar, então backup_sha256_ok fica NULL.

    A identidade da execução é obtida de new_verification_id() uma única
    vez, no início da verificação, e reusada em todas as persistências
    desta mesma execução. `verification_id` identifica a EXECUÇÃO
    individual da verificação — não o componente verificado nem o dia —,
    de modo que duas execuções legítimas nunca compartilham identificador
    e repetir a persistência de uma execução permanece idempotente.
    """
    backup_dir = os.path.expanduser(backup_dir or SYSTEM_BACKUP_DIR)

    result = {
        # Identificador da EXECUÇÃO da verificação, obtido da única
        # fábrica canônica (new_verification_id()). É gerado uma única vez
        # por execução e reusado em todos os pontos de persistência desta
        # verificação, de modo que uma repetição não cria outra linha.
        "verification_id": new_verification_id(),
        "timestamp": datetime.now().isoformat(),
        "status": "unavailable",
        "artifact": None,
        "size_bytes": None,
        "age_hours": None,
        "gzip_ok": None,
        "sha256_ok": None,
        "sha256": None,
        "reason": None,
    }

    if not backup_volume_is_mounted(backup_dir):
        result["reason"] = sanitize_backup_reason(
            "volume de backup não está montado"
        )
        return result

    if not os.path.isdir(backup_dir):
        # Volume montado e diretório inexistente: nenhum backup foi
        # produzido ainda. Isso é ausência, não impossibilidade de
        # verificar — distinta de um diretório que existe e não pode ser
        # lido, tratado abaixo.
        result["status"] = "missing"
        result["reason"] = sanitize_backup_reason(
            "diretório de backup ausente"
        )
        return result

    backups = get_system_backups(backup_dir)

    if backups is None:
        # O diretório existe (verificado acima) e mesmo assim não pôde ser
        # lido: permissão ou falha de I/O. Nada se afirma sobre o backup.
        # O caminho NÃO entra no motivo: o diagnóstico útil é a categoria
        # da falha, não a localização do diretório.
        result["reason"] = sanitize_backup_reason(
            "diretório de backup inacessível"
        )
        return result

    if not backups["count"]:
        result["status"] = "missing"
        result["reason"] = sanitize_backup_reason("nenhum backup encontrado")
        return result

    latest = backups["latest"]
    result["artifact"] = os.path.basename(latest)

    try:
        size = os.path.getsize(latest)
        mtime = os.path.getmtime(latest)

    except OSError:
        result["reason"] = sanitize_backup_reason(
            "não foi possível ler o backup mais recente"
        )
        return result

    result["size_bytes"] = size

    age_seconds = max(datetime.now().timestamp() - mtime, 0.0)
    result["age_hours"] = round(age_seconds / 3600, 2)

    if age_seconds > BACKUP_MAX_AGE_HOURS * 3600:
        result["status"] = "missing"
        result["reason"] = sanitize_backup_reason(
            f"backup mais recente há {result['age_hours']:.0f}h"
        )
        return result

    result["gzip_ok"] = 1 if gzip_integrity_ok(latest) else 0

    if not result["gzip_ok"]:
        result["status"] = "failed"
        result["reason"] = sanitize_backup_reason("backup corrompido (gzip)")
        return result

    expected = read_checksum_file(
        latest + SYSTEM_BACKUP_CHECKSUM_SUFFIX
    )
    result["sha256"] = sha256_of_file(latest)

    if expected is None or result["sha256"] is None:
        # Não houve comparação: o resultado é NULL, nunca 0 e nunca 1.
        result["sha256_ok"] = None
        result["reason"] = sanitize_backup_reason(
            "verificação SHA-256 indisponível"
            if expected is None
            else "não foi possível calcular o SHA-256 do backup"
        )
        return result

    # O checksum vale para o arquivo que ele nomeia. Um nome diferente
    # significa que a referência não corresponde ao artefato analisado:
    # é divergência (0), não ausência (NULL). O nome referenciado não é
    # exposto — o que importa é a categoria da divergência.
    if expected[1] != result["artifact"]:
        result["sha256_ok"] = 0
        result["status"] = "failed"
        result["reason"] = sanitize_backup_reason(
            "checksum refere outro arquivo"
        )
        return result

    result["sha256_ok"] = 1 if result["sha256"] == expected[0] else 0

    if not result["sha256_ok"]:
        result["status"] = "failed"
        result["reason"] = sanitize_backup_reason("SHA-256 divergente")
        return result

    missing = tar_missing_members(latest)

    if missing is None:
        result["status"] = "failed"
        result["reason"] = sanitize_backup_reason("backup corrompido (tar)")
        return result

    if missing:
        result["status"] = "failed"
        # Só o último segmento de cada membro essencial: identifica o
        # componente ausente sem reproduzir a árvore de diretórios.
        result["reason"] = sanitize_backup_reason(
            "conteúdo essencial ausente: "
            + ", ".join(backup_path_basename(member) for member in missing)
        )
        return result

    result["status"] = "ok"
    return result


def print_system_backup(backup):
    """Imprime o resultado da verificação do backup do sistema."""
    if not backup:
        warn("Backup do sistema", "não verificado")
        return

    if backup.get("artifact"):
        print(f"  {'Arquivo':<20} {backup['artifact']}")

    if backup.get("age_hours") is not None:
        print(f"  {'Idade':<20} {format_age(backup['age_hours'] * 3600)}")

    if backup.get("size_bytes") is not None:
        print(f"  {'Tamanho':<20} {format_size(backup['size_bytes'])}")

    status = backup.get("status")
    reason = backup.get("reason")

    if status == "ok":
        ok("Integridade", "gzip e SHA-256 OK")
        ok("Conteúdo", f"{len(SYSTEM_BACKUP_REQUIRED_MEMBERS)} membros essenciais")
        ok("Atualidade", "backup recente")
        return

    if status == "failed":
        if backup.get("gzip_ok") == 0:
            fail("Integridade", "backup corrompido (gzip)")
        elif backup.get("sha256_ok") == 0:
            fail("Integridade", "SHA-256 divergente")
        else:
            fail("Conteúdo", reason or "backup inválido")
        return

    if status == "missing":
        fail("Atualidade", reason or "nenhum backup recente")
        return

    warn("Verificação", reason or "não foi possível verificar o backup")


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


def new_verification_id():
    """Identificador único e estável de uma execução da verificação.

    É a única fábrica do `verification_id`: identifica uma EXECUÇÃO
    individual da verificação de backup (não um componente nem um dia).
    Deve ser chamada uma única vez por execução e o valor reusado em
    todas as persistências daquela mesma execução.
    """
    return str(uuid.uuid4())


def _linked_health_id(conn, health_id):
    """Devolve `health_id` somente se a coleta correspondente existir.

    Guard de integridade referencial (E3, Alternativa C): não há FK física
    entre `system_backup_checks.health_id` e `health.id`, então o vínculo é
    validado na aplicação antes de gravar. `health_id` é opcional e NULL é
    um estado legítimo (verificação independente quando a coleta essencial
    falha). Um valor informado que não exista em `health` seria uma
    referência órfã — e o resumo semanal a descartaria silenciosamente como
    "não verificada". Para nunca gravar referência inexistente e preservar
    a verificação, o vínculo inválido é descartado (NULL), espelhando o
    comportamento de `ON DELETE SET NULL`.
    """
    if health_id is None:
        return None

    row = conn.execute(
        "SELECT 1 FROM health WHERE id = ?", (health_id,)
    ).fetchone()

    return health_id if row is not None else None


def _upsert_system_backup_check(
    conn, verification_id, timestamp, backup, health_id=None
):
    """Grava (ou completa) uma verificação na tabela canônica.

    Idempotente por `verification_id`: a repetição da mesma execução não
    cria outra linha. O `health_id` só é associado quando informado, e
    nunca é apagado por uma gravação posterior sem vínculo — a verificação
    independente (coleta essencial malsucedida) permanece com
    `health_id` NULL até existir uma coleta correspondente.

    O vínculo passa por `_linked_health_id` (E3): uma referência a coleta
    inexistente é descartada (NULL) e nunca gravada como órfã.
    """
    health_id = _linked_health_id(conn, health_id)

    conn.execute(
        f"""
        INSERT INTO {SYSTEM_BACKUP_CHECKS_TABLE} (
            verification_id, timestamp, status, artifact, size_bytes,
            age_hours, gzip_ok, sha256_ok, sha256, reason, health_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(verification_id) DO UPDATE SET
            health_id = COALESCE(
                excluded.health_id, {SYSTEM_BACKUP_CHECKS_TABLE}.health_id
            )
        """,
        (
            verification_id,
            timestamp,
            backup.get("status"),
            backup.get("artifact"),
            backup.get("size_bytes"),
            backup.get("age_hours"),
            backup.get("gzip_ok"),
            backup.get("sha256_ok"),
            backup.get("sha256"),
            backup.get("reason"),
            health_id,
        ),
    )


def persist_system_backup_check(
    backup, verification_id=None, health_id=None, db_path=None
):
    """Persiste a verificação na fonte canônica, de forma independente.

    Executada imediatamente após collect_system_backup(), antes de
    finalize_collection: a verificação é preservada mesmo quando a coleta
    essencial falha depois. Usa conexão e transação próprias e não depende
    do sucesso da coleta essencial. Devolve o `verification_id` gravado,
    ou None quando não há verificação a gravar.
    """
    if not backup:
        return None

    verification_id = verification_id or backup.get("verification_id")

    if not verification_id:
        return None

    timestamp = backup.get("timestamp") or datetime.now().isoformat()
    db_path = db_path or DB_PATH

    def persist():
        with health_db_connection(db_path) as conn:
            initialize_health_schema(conn)
            _upsert_system_backup_check(
                conn,
                verification_id,
                timestamp,
                backup,
                health_id=health_id,
            )

    # Retry SÓ sob contenção: a gravação é idempotente por `verification_id`
    # (ON CONFLICT DO UPDATE), então repetir não cria linha duplicada. Cada
    # tentativa abre a PRÓPRIA conexão/transação — falha faz rollback limpo.
    run_with_retry(
        persist,
        description="persistir a verificação do backup do sistema",
    )

    return verification_id


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

    # Verificação independente do backup diário do sistema. Ausente em
    # chamadas antigas (testes, uso manual): todas as colunas ficam NULL.
    system_backup = health.get("system_backup") or {}

    # Sem retry: `INSERT INTO health` NÃO é idempotente — repetir criaria uma
    # coleta duplicada. A contenção transitória é absorvida pelo busy_timeout
    # da conexão; esgotado o timeout, a falha propaga e a coleta é marcada
    # como falha (o check de backup já foi preservado pela persistência
    # independente).
    with health_db_connection(db_path) as conn:
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
                missing_components,
                backup_status,
                backup_artifact,
                backup_size_bytes,
                backup_age_hours,
                backup_gzip_ok,
                backup_sha256_ok,
                backup_sha256,
                backup_reason
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
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
                system_backup.get("status"),
                system_backup.get("artifact"),
                system_backup.get("size_bytes"),
                system_backup.get("age_hours"),
                system_backup.get("gzip_ok"),
                system_backup.get("sha256_ok"),
                system_backup.get("sha256"),
                system_backup.get("reason"),
            ),
        )

        health_id = cursor.lastrowid

        # Associa a verificação do backup à coleta, na MESMA transação da
        # linha de `health`. Se a persistência independente já ocorreu, a
        # linha canônica é completada com o vínculo; se save_health foi
        # chamado diretamente, o registro é criado aqui. Idempotente por
        # `verification_id`: nunca há registro duplicado.
        if system_backup.get("verification_id"):
            _upsert_system_backup_check(
                conn,
                system_backup["verification_id"],
                system_backup.get("timestamp")
                or health["timestamp"].isoformat(),
                system_backup,
                health_id=health_id,
            )

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
    # BACKUP DO SISTEMA
    #
    # Verificação independente do artefato produzido pelo workflow
    # imediatamente antes desta coleta. Não altera collection_status
    # nem missing_components: falha de backup é achado, não falha de
    # coleta, e nada aqui interrompe a coleta essencial.
    # --------------------------------------------------------

    section("BACKUP DO SISTEMA")

    try:
        system_backup = collect_system_backup()
    except Exception as error:  # nunca interrompe a coleta essencial
        system_backup = {
            "verification_id": new_verification_id(),
            "timestamp": datetime.now().isoformat(),
            "status": "unavailable",
            "artifact": None,
            "size_bytes": None,
            "age_hours": None,
            "gzip_ok": None,
            "sha256_ok": None,
            "sha256": None,
            "reason": sanitize_backup_reason(
                f"verificação do backup falhou: {type(error).__name__}"
            ),
        }

    print_system_backup(system_backup)

    # Persistência INDEPENDENTE, imediatamente após a verificação e antes
    # de finalize_collection: a verificação é preservada mesmo quando a
    # coleta essencial falhar depois. Falha aqui é registrada e nunca
    # interrompe o workflow nem mascara o erro original da coleta.
    if save:
        try:
            persist_system_backup_check(system_backup, db_path=db_path)
        except Exception as error:
            print(
                "Falha ao persistir a verificação do backup do sistema: "
                f"{error}",
                file=sys.stderr,
            )

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
        "system_backup": system_backup,
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

