from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict

import pandas as pd

from src.config.settings import BOARDS_DESTINO, DEST_COLUMN_ID_MAP, LOG_PREFIX


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def _clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _as_number(value: Any) -> str:
    s = _clean_text(value)
    if not s:
        return ""

    s_norm = s.replace(" ", "")
    if "," in s_norm and "." in s_norm:
        s_norm = s_norm.replace(".", "").replace(",", ".")
    elif "," in s_norm:
        s_norm = s_norm.replace(",", ".")

    try:
        num = Decimal(s_norm)
    except (InvalidOperation, ValueError):
        return ""
    return format(num, "f")


def _as_date_dict(value: Any) -> Dict[str, str] | None:
    s = _clean_text(value)
    if not s:
        return None
    return {"date": s}


def _get_dest_id_column_id(destino_key: str) -> str:
    cfg = BOARDS_DESTINO.get(destino_key, {})
    return cfg.get("id_column", "")


def _build_column_values_from_row(row: pd.Series) -> dict:
    colvals: Dict[str, Any] = {}

    if _clean_text(row.get("DEST_nome_pessoa")):
        colvals[DEST_COLUMN_ID_MAP["DEST_nome_pessoa"]] = _clean_text(row.get("DEST_nome_pessoa"))
    if _clean_text(row.get("DEST_nome_curto")):
        colvals[DEST_COLUMN_ID_MAP["DEST_nome_curto"]] = _clean_text(row.get("DEST_nome_curto"))
    if _clean_text(row.get("DEST_nr_titulo")):
        colvals[DEST_COLUMN_ID_MAP["DEST_nr_titulo"]] = _clean_text(row.get("DEST_nr_titulo"))
    if _clean_text(row.get("DEST_observacao")):
        colvals[DEST_COLUMN_ID_MAP["DEST_observacao"]] = _clean_text(row.get("DEST_observacao"))

    d1 = _as_date_dict(row.get("DEST_dt_venc_original"))
    if d1:
        colvals[DEST_COLUMN_ID_MAP["DEST_dt_venc_original"]] = d1

    d2 = _as_date_dict(row.get("DEST_dt_realizacao"))
    if d2:
        colvals[DEST_COLUMN_ID_MAP["DEST_dt_realizacao"]] = d2

    n1 = _as_number(row.get("DEST_vl_titulo_atualizado"))
    if n1:
        colvals[DEST_COLUMN_ID_MAP["DEST_vl_titulo_atualizado"]] = n1

    n2 = _as_number(row.get("DEST_vl_liquido"))
    if n2:
        colvals[DEST_COLUMN_ID_MAP["DEST_vl_liquido"]] = n2

    if _clean_text(row.get("DEST_forma_pagamento")):
        colvals[DEST_COLUMN_ID_MAP["DEST_forma_pagamento"]] = {
            "labels": [_clean_text(row.get("DEST_forma_pagamento"))]
        }
    if _clean_text(row.get("DEST_centro_custo")):
        colvals[DEST_COLUMN_ID_MAP["DEST_centro_custo"]] = {
            "labels": [_clean_text(row.get("DEST_centro_custo"))]
        }
    if _clean_text(row.get("DEST_tipo_operacao")):
        colvals[DEST_COLUMN_ID_MAP["DEST_tipo_operacao"]] = {
            "labels": [_clean_text(row.get("DEST_tipo_operacao"))]
        }

    destino_key = _clean_text(row.get("matched_destino_key"))
    id_col = _get_dest_id_column_id(destino_key)
    id_val = _clean_text(row.get("ID"))
    if id_col and id_val:
        colvals[id_col] = id_val

    return colvals


def build_df_payload(df_enriched: pd.DataFrame) -> pd.DataFrame:
    if df_enriched is None or df_enriched.empty:
        return pd.DataFrame()

    df_payload_base = df_enriched[df_enriched["READY_TO_CREATE"] == True].copy()  # noqa: E712
    if df_payload_base.empty:
        return pd.DataFrame()

    df_payload_base["AF"] = df_payload_base["AF"].fillna("").astype(str).str.strip()
    df_payload_base["ID"] = df_payload_base["ID"].fillna("").astype(str).str.strip()
    df_payload_base = df_payload_base[(df_payload_base["AF"] != "") & (df_payload_base["ID"] != "")].copy()

    payload_rows = []
    for _, row in df_payload_base.iterrows():
        payload_rows.append(
            {
                "matched_destino_key": row.get("matched_destino_key", ""),
                "BOARD_ID_DESTINO_FINAL": str(row.get("BOARD_ID_DESTINO_FINAL", "")).strip(),
                "GROUP_ID_DESTINO_FINAL": str(row.get("GROUP_ID_DESTINO_FINAL", "")).strip(),
                "item_name": row["AF"],
                "ID": row["ID"],
                "column_values": _build_column_values_from_row(row),
            }
        )

    df_payload = pd.DataFrame(payload_rows)
    log_info(f"Payload pronto para create: {len(df_payload)}")
    return df_payload
