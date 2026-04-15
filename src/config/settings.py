from __future__ import annotations

import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()


def _get_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    return float(value)


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "sim", "s"}:
        return True
    if normalized in {"0", "false", "no", "n", "nao"}:
        return False

    raise ValueError(f"Variavel {name} invalida para bool: {value}")


def _mask_token(token: Optional[str], head: int = 4, tail: int = 3) -> str:
    if not token:
        return "MISSING"
    if len(token) <= head + tail:
        return "*" * len(token)
    return f"{token[:head]}...{token[-tail:]} (len={len(token)})"


LOG_PREFIX: str = _get_str("PIPELINE_LOG_PREFIX", "[PAYMENTS]") or "[PAYMENTS]"
API_URL: str = _get_str("MONDAY_BASE_URL", "https://api.monday.com/v2") or "https://api.monday.com/v2"
TOKEN_MONDAY: Optional[str] = _get_str("MONDAY_API_TOKEN")

HEADERS: Dict[str, str] = {
    "Authorization": TOKEN_MONDAY or "",
    "Content-Type": "application/json",
}

MAX_RETRIES: int = _get_int("MONDAY_MAX_RETRIES", 5)
REQUEST_TIMEOUT: int = _get_int("MONDAY_REQUEST_TIMEOUT", 60)
BASE_DELAY: float = _get_float("MONDAY_BACKOFF_BASE", 1.0)
BACKOFF_FACTOR: float = _get_float("MONDAY_BACKOFF_FACTOR", 2.0)
MAX_DELAY: float = _get_float("MONDAY_BACKOFF_CAP", 30.0)
JITTER_MIN: float = _get_float("MONDAY_JITTER_MIN", 0.0)
JITTER_MAX: float = _get_float("MONDAY_JITTER_MAX", 0.5)

PAGE_LIMIT: int = _get_int("MONDAY_PAGE_LIMIT", 500)
SLEEP_BETWEEN_REQUESTS: float = _get_float("MONDAY_SLEEP_BETWEEN_REQUESTS", 0.2)
MOSTRAR_PROGRESSO: bool = _get_bool("PIPELINE_SHOW_PROGRESS", True)

ENRICH_BATCH_SIZE: int = _get_int("MONDAY_ENRICH_BATCH_SIZE", 20)
ENRICH_SLEEP_BETWEEN_BATCHES_SEC: float = _get_float(
    "MONDAY_ENRICH_SLEEP_BETWEEN_BATCHES_SEC", 0.35
)
CREATE_SLEEP_SECONDS: float = _get_float("MONDAY_CREATE_SLEEP_SECONDS", 0.2)
ACTION_SLEEP_SECONDS: float = _get_float("MONDAY_ACTION_SLEEP_SECONDS", 0.15)

CREATE_DRY_RUN: bool = _get_bool("PIPELINE_CREATE_DRY_RUN", False)
DELETE_DUPLICATE_DRY_RUN: bool = _get_bool("PIPELINE_DELETE_DUPLICATE_DRY_RUN", False)
DELETE_WRONG_BOARD_DRY_RUN: bool = _get_bool("PIPELINE_DELETE_WRONG_BOARD_DRY_RUN", False)
DELETE_NO_ORIGIN_DRY_RUN: bool = _get_bool("PIPELINE_DELETE_NO_ORIGIN_DRY_RUN", False)
MOVE_WRONG_GROUP_DRY_RUN: bool = _get_bool("PIPELINE_MOVE_WRONG_GROUP_DRY_RUN", False)

BOARDS_ORIGEM: Dict[str, Dict[str, str]] = {
    "PY_2025_JAN_JUN": {
        "board_id": "9927439992",
        "board_name": "[Py] Pagamentos Realizados Jan - Jun 2025",
    },
    "PY_2025_JUL_DEZ": {
        "board_id": "18126969654",
        "board_name": "[Py] Pagamentos Realizados Jul - Dez 2025",
    },
    "PY_2026_JAN_JUN": {
        "board_id": "18393715465",
        "board_name": "[Py] Pagamentos Realizados Jan - Jun 2026",
    },
}

BOARDS_DESTINO: Dict[str, Dict[str, Any]] = {
    "ENEVA": {
        "board_id": "18407796162",
        "group_pagamentos": "group_mkvcrrcf",
        "group_movbco": "group_mm27prx",
        "id_column": "text_mm27j5am",
        "cc_keywords": ["ENEVA"],
    },
    "FS_BIO_CPT01": {
        "board_id": "18407797595",
        "group_pagamentos": "group_mkvcrrcf",
        "group_movbco": "group_mm27ggzy",
        "id_column": "text_mm27yvtr",
        "cc_keywords": ["FS BIOENERGIA", "PERFURACAO PTB BA"],
    },
    "SPTS": {
        "board_id": "18407798241",
        "group_pagamentos": "group_mkvcrrcf",
        "group_movbco": "group_mm27gxat",
        "id_column": "text_mm27yvg9",
        "cc_keywords": ["SPT"],
    },
    "ATP": {
        "board_id": "18407798364",
        "group_pagamentos": "group_mkvcrrcf",
        "group_movbco": "group_mm274k00",
        "id_column": "text_mm27n83k",
        "cc_keywords": ["ATP", "DESPARAFINACAO"],
    },
    "FLUIDOS_MAR": {
        "board_id": "18407798753",
        "group_pagamentos": "group_mkvcrrcf",
        "group_movbco": "group_mm27sj4y",
        "id_column": "text_mm27vzqs",
        "cc_keywords": ["SERGIPE MAR", "FLUIDO"],
    },
}

COL_AF: str = "name"
COL_ID_ORIGEM: str = "text_mktkv6ct"
COL_CC: str = "dropdown_mkqjnn18"
COL_NR_TITULO: str = "text_mknh7b0j"

ORIGEM_COLUMN_IDS_LEITURA_MINIMA = {COL_ID_ORIGEM, COL_CC}
ORIGEM_COLUMN_IDS_ENRIQUECIMENTO = [
    "text_mknh23aa",
    "text_mknhys8v",
    "date_mknhf7dr",
    "date_mknhk9yy",
    "numeric_mknhx7xx",
    "numeric_mknh5gyx",
    "dropdown_mkqj16vc",
    "dropdown_mkqjnn18",
    "text_mknh7b0j",
    "text_mknh5an4",
    "dropdown_mkqj1npx",
]

DEST_COLUMN_ID_MAP = {
    "DEST_nome_pessoa": "text_mknh23aa",
    "DEST_nome_curto": "text_mknhys8v",
    "DEST_dt_venc_original": "date_mknhf7dr",
    "DEST_dt_realizacao": "date_mknhk9yy",
    "DEST_vl_titulo_atualizado": "numeric_mknhx7xx",
    "DEST_vl_liquido": "numeric_mknh5gyx",
    "DEST_forma_pagamento": "dropdown_mkqj16vc",
    "DEST_centro_custo": "dropdown_mkqjnn18",
    "DEST_nr_titulo": "text_mknh7b0j",
    "DEST_observacao": "text_mknh5an4",
    "DEST_tipo_operacao": "dropdown_mkqj1npx",
}


def check_required_envs() -> None:
    missing = [
        name
        for name, value in [
            ("MONDAY_API_TOKEN", TOKEN_MONDAY),
        ]
        if not value
    ]

    if missing:
        missing_lines = "\n".join(f"- {name}" for name in missing)
        raise RuntimeError(f"Variaveis obrigatorias ausentes:\n{missing_lines}")

    print(f"{LOG_PREFIX} [INFO] Configuracao carregada com sucesso")
    print(f"{LOG_PREFIX} [INFO] Monday URL: {API_URL}")
    print(f"{LOG_PREFIX} [INFO] Token: {_mask_token(TOKEN_MONDAY)}")
    print(
        f"{LOG_PREFIX} [INFO] Dry-runs => "
        f"create={CREATE_DRY_RUN} | "
        f"dup_delete={DELETE_DUPLICATE_DRY_RUN} | "
        f"wrong_board_delete={DELETE_WRONG_BOARD_DRY_RUN} | "
        f"no_origin_delete={DELETE_NO_ORIGIN_DRY_RUN} | "
        f"wrong_group_move={MOVE_WRONG_GROUP_DRY_RUN}"
    )
