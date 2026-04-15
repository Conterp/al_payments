from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    ENRICH_BATCH_SIZE,
    ENRICH_SLEEP_BETWEEN_BATCHES_SEC,
    LOG_PREFIX,
    ORIGEM_COLUMN_IDS_ENRIQUECIMENTO,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import _extract_text_map


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def _build_enrich_items_query() -> str:
    return """
    query ($item_ids: [ID!], $column_ids: [String!]) {
      items(ids: $item_ids) {
        id
        name
        column_values(ids: $column_ids) { id text }
      }
    }
    """


def _pick_group_destino(row: pd.Series) -> str:
    grupo_origem = str(row.get("GRUPO_NAME", "") or "").strip().upper()
    if grupo_origem == "MOVBCO":
        return row.get("matched_group_movbco_destino", "")
    return row.get("matched_group_pagamentos_destino", "")


def build_df_enriched(df_input: pd.DataFrame) -> pd.DataFrame:
    if df_input.empty:
        log_warn("ENRICH: dataframe de entrada vazio")
        return pd.DataFrame()

    base = df_input.copy()
    base["ID_ITEM_MONDAY_ORIGEM"] = base["ID_ITEM_MONDAY_ORIGEM"].fillna("").astype(str).str.strip()

    boards_presentes = set(base["BOARD_NAME"].dropna().astype(str).tolist())
    process_order = sorted(boards_presentes)

    rows: List[Dict[str, Any]] = []
    for board_name in process_order:
        df_board = base[base["BOARD_NAME"] == board_name].copy()
        board_ids = df_board["ID_ITEM_MONDAY_ORIGEM"].dropna().astype(str).str.strip()
        board_ids = [x for x in dict.fromkeys(board_ids.tolist()) if x]

        if not board_ids:
            continue

        log_info(f"ENRICH {board_name}: inicio | ids={len(board_ids)}")
        for i in tqdm(range(0, len(board_ids), ENRICH_BATCH_SIZE), desc=f"Enrich {board_name}"):
            batch = board_ids[i : i + ENRICH_BATCH_SIZE]
            batch_n = (i // ENRICH_BATCH_SIZE) + 1

            data = execute_monday_query(
                query=_build_enrich_items_query(),
                variables={"item_ids": batch, "column_ids": ORIGEM_COLUMN_IDS_ENRIQUECIMENTO},
                operation_name=f"enrich_{board_name}_batch_{batch_n}",
            )

            items = data.get("items", []) or []
            for it in items:
                col_map = _extract_text_map(it.get("column_values", []))
                rows.append(
                    {
                        "ID_ITEM_MONDAY_ORIGEM": str(it.get("id", "")).strip(),
                        "AF": (it.get("name") or "").strip(),
                        "DEST_nome_pessoa": (col_map.get("text_mknh23aa") or "").strip(),
                        "DEST_nome_curto": (col_map.get("text_mknhys8v") or "").strip(),
                        "DEST_dt_venc_original": (col_map.get("date_mknhf7dr") or "").strip(),
                        "DEST_dt_realizacao": (col_map.get("date_mknhk9yy") or "").strip(),
                        "DEST_vl_titulo_atualizado": (col_map.get("numeric_mknhx7xx") or "").strip(),
                        "DEST_vl_liquido": (col_map.get("numeric_mknh5gyx") or "").strip(),
                        "DEST_forma_pagamento": (col_map.get("dropdown_mkqj16vc") or "").strip(),
                        "DEST_centro_custo": (col_map.get("dropdown_mkqjnn18") or "").strip(),
                        "DEST_nr_titulo": (col_map.get("text_mknh7b0j") or "").strip(),
                        "DEST_observacao": (col_map.get("text_mknh5an4") or "").strip(),
                        "DEST_tipo_operacao": (col_map.get("dropdown_mkqj1npx") or "").strip(),
                    }
                )

            if ENRICH_SLEEP_BETWEEN_BATCHES_SEC > 0:
                time.sleep(ENRICH_SLEEP_BETWEEN_BATCHES_SEC)

    df_enrich = pd.DataFrame(rows)
    if df_enrich.empty:
        log_warn("ENRICH: nenhum retorno")
        return pd.DataFrame()

    df_enrich = df_enrich.drop_duplicates(subset=["ID_ITEM_MONDAY_ORIGEM"], keep="first")
    df_enriched = base.merge(df_enrich, on="ID_ITEM_MONDAY_ORIGEM", how="left", suffixes=("", "_enrich"))

    if "AF_enrich" in df_enriched.columns:
        df_enriched["AF"] = df_enriched["AF_enrich"].where(
            df_enriched["AF_enrich"].fillna("").astype(str).str.strip().ne(""),
            df_enriched["AF"],
        )
        df_enriched = df_enriched.drop(columns=["AF_enrich"])

    df_enriched["BOARD_ID_DESTINO_FINAL"] = df_enriched["matched_board_id_destino"]
    df_enriched["GROUP_ID_DESTINO_FINAL"] = df_enriched.apply(_pick_group_destino, axis=1)

    dest_cols = [
        "DEST_nome_pessoa",
        "DEST_nome_curto",
        "DEST_dt_venc_original",
        "DEST_dt_realizacao",
        "DEST_vl_titulo_atualizado",
        "DEST_vl_liquido",
        "DEST_forma_pagamento",
        "DEST_centro_custo",
        "DEST_nr_titulo",
        "DEST_observacao",
        "DEST_tipo_operacao",
    ]
    df_enriched["ENRICH_OK"] = ~df_enriched[dest_cols].isna().all(axis=1)
    df_enriched["READY_TO_CREATE"] = (
        df_enriched["BOARD_ID_DESTINO_FINAL"].fillna("").astype(str).str.strip().ne("")
        & df_enriched["GROUP_ID_DESTINO_FINAL"].fillna("").astype(str).str.strip().ne("")
        & df_enriched["AF"].fillna("").astype(str).str.strip().ne("")
        & df_enriched["ENRICH_OK"]
    )

    if "ID_NORM" in df_enriched.columns:
        df_enriched = (
            df_enriched.sort_values(
                by=["ID_NORM", "ID_ITEM_MONDAY_ORIGEM"],
                ascending=[True, True],
                kind="stable",
            )
            .drop_duplicates(subset=["ID_NORM"], keep="first")
            .copy()
        )

    log_info(f"ENRICH concluido. Linhas={len(df_enriched)}")
    return df_enriched
