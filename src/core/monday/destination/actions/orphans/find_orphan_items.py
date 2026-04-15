from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd

from src.config.settings import LOG_PREFIX
from src.core.monday.destination.payload.build_missing_ids import _build_cc_targets, _match_cc_destino


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def _expected_group_id(row: pd.Series, expected_match: Dict[str, str]) -> str:
    nr_titulo = str(row.get("NR_TITULO", "") or "").strip().upper()
    if "MOVBCO" in nr_titulo:
        return expected_match.get("matched_group_movbco_destino") or ""
    return expected_match.get("matched_group_pagamentos_destino") or ""


def build_df_orphans(
    df_destination_audit: pd.DataFrame,
    df_origem_matched: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df_destination_audit.empty:
        df_empty = pd.DataFrame()
        return df_empty, df_empty, df_empty

    df_dest_rules = df_destination_audit.copy()
    origem_ids = set(df_origem_matched["ID_NORM"].fillna("").astype(str).str.strip().tolist())
    origem_ids = {origin_id for origin_id in origem_ids if origin_id != ""}

    cc_targets = _build_cc_targets()
    expected_rows = df_dest_rules.apply(
        lambda row: _match_cc_destino(row.get("CENTRO_CUSTO", ""), cc_targets),
        axis=1,
    )
    df_expected = pd.DataFrame(list(expected_rows))
    df_dest_rules = pd.concat(
        [df_dest_rules.reset_index(drop=True), df_expected.reset_index(drop=True)],
        axis=1,
    )

    df_dest_rules["EXPECTED_GROUP_ID"] = df_dest_rules.apply(
        lambda row: _expected_group_id(row, row.to_dict()),
        axis=1,
    )
    df_dest_rules["IS_ID_EMPTY"] = (
        df_dest_rules["ID_NORM"].fillna("").astype(str).str.strip().eq("")
    )
    df_dest_rules["IS_NO_ORIGIN"] = (
        (~df_dest_rules["IS_ID_EMPTY"]) & (~df_dest_rules["ID_NORM"].isin(origem_ids))
    )

    df_dest_rules["IS_WRONG_BOARD"] = (
        (~df_dest_rules["IS_ID_EMPTY"])
        & (~df_dest_rules["IS_NO_ORIGIN"])
        & (df_dest_rules["match_status"] == "matched")
        & (
            df_dest_rules["BOARD_ID_DESTINO"].astype(str)
            != df_dest_rules["matched_board_id_destino"].astype(str)
        )
    )

    df_dest_rules["IS_WRONG_GROUP"] = (
        (~df_dest_rules["IS_ID_EMPTY"])
        & (~df_dest_rules["IS_NO_ORIGIN"])
        & (df_dest_rules["match_status"] == "matched")
        & (~df_dest_rules["IS_WRONG_BOARD"])
        & (
            df_dest_rules["GROUP_ID_DESTINO"].astype(str)
            != df_dest_rules["EXPECTED_GROUP_ID"].astype(str)
        )
    )

    df_wrong_board = df_dest_rules[df_dest_rules["IS_WRONG_BOARD"]].copy()
    df_wrong_group = df_dest_rules[df_dest_rules["IS_WRONG_GROUP"]].copy()
    df_no_origin = df_dest_rules[
        df_dest_rules["IS_ID_EMPTY"] | df_dest_rules["IS_NO_ORIGIN"]
    ].copy()

    log_info(
        f"Orphans detectados | wrong_board={len(df_wrong_board)} | "
        f"wrong_group={len(df_wrong_group)} | no_origin={len(df_no_origin)}"
    )

    return df_wrong_board, df_wrong_group, df_no_origin
