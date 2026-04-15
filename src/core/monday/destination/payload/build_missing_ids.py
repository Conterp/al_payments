from __future__ import annotations

import unicodedata
from typing import Any, Dict, List

import pandas as pd

from src.config.settings import BOARDS_DESTINO, LOG_PREFIX


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    normalized_text = str(value).strip()
    if not normalized_text:
        return ""
    normalized_text = " ".join(normalized_text.split())
    normalized_text = (
        unicodedata.normalize("NFKD", normalized_text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return normalized_text.upper()


def normalize_id(value: Any) -> str:
    normalized_id = _normalize_text(value)
    return normalized_id.replace(" ", "") if normalized_id else ""


def normalize_cc_for_match(value: Any) -> str:
    return _normalize_text(value)


def _build_cc_targets() -> List[Dict[str, str]]:
    cc_targets: List[Dict[str, str]] = []
    for destino_key, destino_cfg in BOARDS_DESTINO.items():
        for cc_keyword in destino_cfg.get("cc_keywords", []):
            cc_keyword_norm = normalize_cc_for_match(cc_keyword)
            if cc_keyword_norm:
                cc_targets.append(
                    {
                        "matched_destino_key": destino_key,
                        "keyword_raw": cc_keyword,
                        "keyword_norm": cc_keyword_norm,
                        "matched_board_id_destino": destino_cfg["board_id"],
                        "matched_group_pagamentos_destino": destino_cfg["group_pagamentos"],
                        "matched_group_movbco_destino": destino_cfg["group_movbco"],
                    }
                )
    return cc_targets


def _match_cc_destino(cc_value: Any, cc_targets: List[Dict[str, str]]) -> Dict[str, Any]:
    cc_norm = normalize_cc_for_match(cc_value)
    if not cc_norm:
        return {"match_status": "ignore"}

    cc_hits = [target for target in cc_targets if target["keyword_norm"] in cc_norm]
    if len(cc_hits) != 1:
        return {"match_status": "ignore"}

    return {"match_status": "matched", **cc_hits[0]}


def _get_str_series(df_input: pd.DataFrame, col_name: str) -> pd.Series:
    if col_name in df_input.columns:
        return df_input[col_name].fillna("").astype(str).str.strip()
    return pd.Series("", index=df_input.index, dtype="object")


def normalize_origem_destino(
    df_origem: pd.DataFrame,
    df_destino: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    df_origem_norm = df_origem.copy()
    df_destino_norm = df_destino.copy()

    if "ID" not in df_origem_norm.columns:
        df_origem_norm["ID"] = ""
    if "ID" not in df_destino_norm.columns:
        df_destino_norm["ID"] = ""
    if "CENTRO_CUSTO" not in df_origem_norm.columns:
        df_origem_norm["CENTRO_CUSTO"] = ""

    df_origem_norm["ID_NORM"] = df_origem_norm["ID"].map(normalize_id)
    df_destino_norm["ID_NORM"] = df_destino_norm["ID"].map(normalize_id)
    df_origem_norm["cc_norm"] = df_origem_norm["CENTRO_CUSTO"].map(normalize_cc_for_match)

    return {"df_origem_norm": df_origem_norm, "df_destino_norm": df_destino_norm}


def filter_origem_matched(df_origem_norm: pd.DataFrame) -> pd.DataFrame:
    if df_origem_norm.empty:
        return df_origem_norm.copy()

    cc_targets = _build_cc_targets()
    matched_rows = df_origem_norm["CENTRO_CUSTO"].apply(
        lambda cc_value: _match_cc_destino(cc_value, cc_targets)
    )
    df_matched_rules = pd.DataFrame(list(matched_rows))

    df_origem_with_rules = pd.concat(
        [df_origem_norm.reset_index(drop=True), df_matched_rules.reset_index(drop=True)],
        axis=1,
    )
    df_origem_matched = df_origem_with_rules[
        df_origem_with_rules["match_status"] == "matched"
    ].copy()

    log_info(f"Origem matched apos filtro de CC: {len(df_origem_matched)}")
    return df_origem_matched


def build_df_diff_ids(
    df_origem_matched: pd.DataFrame,
    df_destino_norm: pd.DataFrame,
) -> pd.DataFrame:
    if df_origem_matched.empty:
        return pd.DataFrame()

    df_origem_ref = df_origem_matched.copy()
    df_destino_ref = df_destino_norm.copy()

    df_origem_ref["BOARD_KEY_EXPECTED"] = _get_str_series(
        df_input=df_origem_ref,
        col_name="matched_destino_key",
    )
    df_destino_ref["BOARD_KEY_ATUAL"] = _get_str_series(
        df_input=df_destino_ref,
        col_name="BOARD_NAME_DESTINO",
    )

    df_origem_ref = df_origem_ref[
        df_origem_ref["ID_NORM"].fillna("").astype(str).str.strip() != ""
    ].copy()

    destino_pairs = set(
        zip(
            df_destino_ref["ID_NORM"].fillna("").astype(str).str.strip(),
            df_destino_ref["BOARD_KEY_ATUAL"].fillna("").astype(str).str.strip(),
        )
    )

    origem_pairs = list(
        zip(
            df_origem_ref["ID_NORM"].fillna("").astype(str).str.strip(),
            df_origem_ref["BOARD_KEY_EXPECTED"].fillna("").astype(str).str.strip(),
        )
    )
    keep_mask = [pair not in destino_pairs for pair in origem_pairs]
    df_diff_ids = df_origem_ref.loc[keep_mask].copy()

    sort_cols = [
        col_name
        for col_name in ["BOARD_NAME", "GRUPO_NAME", "CENTRO_CUSTO", "AF", "ID"]
        if col_name in df_diff_ids.columns
    ]
    if sort_cols:
        df_diff_ids = df_diff_ids.sort_values(sort_cols).reset_index(drop=True)

    log_info(f"Diff IDs (faltando no destino): {len(df_diff_ids)}")
    return df_diff_ids


def dedupe_by_id(
    df_input: pd.DataFrame,
    id_col: str = "ID_NORM",
    id_item_col: str = "ID_ITEM_MONDAY_ORIGEM",
) -> pd.DataFrame:
    if df_input.empty:
        return df_input.copy()

    df_dedup_base = df_input.copy()
    if id_col not in df_dedup_base.columns:
        df_dedup_base[id_col] = df_dedup_base["ID"].map(normalize_id)

    df_dedup_base[id_item_col] = pd.to_numeric(df_dedup_base[id_item_col], errors="coerce")
    df_dedup_result = df_dedup_base.sort_values(
        by=[id_col, id_item_col],
        ascending=[True, True],
        kind="stable",
    ).drop_duplicates(subset=[id_col], keep="first")
    return df_dedup_result.copy()
