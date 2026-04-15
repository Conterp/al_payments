from __future__ import annotations

from typing import Dict

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


def build_df_duplicates(df_destination_audit: pd.DataFrame) -> pd.DataFrame:
    if df_destination_audit.empty:
        return df_destination_audit.copy()

    df_dest_rules = df_destination_audit.copy()
    df_dest_rules["ID_ITEM_NUM"] = pd.to_numeric(
        df_dest_rules["ID_ITEM_MONDAY_DESTINO"],
        errors="coerce",
    )

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
    df_dest_rules["IS_BOARD_OK"] = (
        df_dest_rules["BOARD_ID_DESTINO"].astype(str)
        == df_dest_rules["matched_board_id_destino"].astype(str)
    )
    df_dest_rules["IS_GROUP_OK"] = (
        df_dest_rules["GROUP_ID_DESTINO"].astype(str)
        == df_dest_rules["EXPECTED_GROUP_ID"].astype(str)
    )

    df_duplicates = df_dest_rules[
        df_dest_rules["ID_NORM"].fillna("").astype(str).str.strip() != ""
    ].copy()
    df_duplicates = df_duplicates[df_duplicates["IS_BOARD_OK"] == True].copy()  # noqa: E712
    df_duplicates = df_duplicates[
        df_duplicates.duplicated(subset=["ID_NORM"], keep=False)
    ].copy()

    if df_duplicates.empty:
        df_duplicates["DUP_RANK"] = pd.Series(dtype="int64")
        df_duplicates["ACTION"] = pd.Series(dtype="object")
        return df_duplicates

    df_duplicates = df_duplicates.sort_values(
        by=["ID_NORM", "IS_GROUP_OK", "ID_ITEM_NUM"],
        ascending=[True, False, True],
        kind="stable",
    )

    df_duplicates["DUP_RANK"] = df_duplicates.groupby("ID_NORM").cumcount() + 1
    df_duplicates["ACTION"] = df_duplicates["DUP_RANK"].apply(
        lambda rank: "keep" if rank == 1 else "delete"
    )

    log_info(f"Duplicados detectados: {len(df_duplicates)}")
    return df_duplicates


def build_df_duplicates_delete(df_duplicates: pd.DataFrame) -> pd.DataFrame:
    if "ACTION" not in df_duplicates.columns:
        return pd.DataFrame()
    return df_duplicates[df_duplicates["ACTION"] == "delete"].copy()
