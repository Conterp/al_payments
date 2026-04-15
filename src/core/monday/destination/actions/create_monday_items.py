from __future__ import annotations

import json
import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import CREATE_SLEEP_SECONDS, LOG_PREFIX
from src.core.monday.destination.payload.build_missing_ids import normalize_id
from src.core.monday.execute_monday_query import execute_monday_query


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def _to_json_string(payload_obj: dict) -> str:
    return json.dumps(payload_obj, ensure_ascii=False)


def _build_create_item_mutation() -> str:
    return """
    mutation (
      $board_id: ID!,
      $group_id: String!,
      $item_name: String!,
      $column_values: JSON!
    ) {
      create_item(
        board_id: $board_id,
        group_id: $group_id,
        item_name: $item_name,
        column_values: $column_values
      ) { id }
    }
    """


def _validate_create_base(df_payload: pd.DataFrame) -> pd.DataFrame:
    if df_payload.empty:
        return df_payload.copy()

    df_create_base = df_payload.copy()
    df_create_base["item_name"] = df_create_base["item_name"].fillna("").astype(str).str.strip()
    df_create_base["ID"] = df_create_base["ID"].fillna("").astype(str).str.strip()
    df_create_base["BOARD_ID_DESTINO_FINAL"] = (
        df_create_base["BOARD_ID_DESTINO_FINAL"].fillna("").astype(str).str.strip()
    )
    df_create_base["GROUP_ID_DESTINO_FINAL"] = (
        df_create_base["GROUP_ID_DESTINO_FINAL"].fillna("").astype(str).str.strip()
    )

    valid_mask = (
        df_create_base["item_name"].ne("")
        & df_create_base["ID"].ne("")
        & df_create_base["BOARD_ID_DESTINO_FINAL"].ne("")
        & df_create_base["GROUP_ID_DESTINO_FINAL"].ne("")
    )
    return df_create_base[valid_mask].copy()


def build_df_create_results(df_payload_ok: pd.DataFrame, dry_run: bool = False) -> pd.DataFrame:
    if df_payload_ok.empty:
        log_warn("CREATE: df_payload_ok vazio")
        return pd.DataFrame()

    df_create_exec = _validate_create_base(df_payload_ok)
    if df_create_exec.empty:
        log_warn("CREATE: sem linhas validas apos validacao")
        return pd.DataFrame()

    if "ID" in df_create_exec.columns:
        before_len = len(df_create_exec)
        df_create_exec["ID_NORM"] = df_create_exec["ID"].map(normalize_id)
        df_create_exec = (
            df_create_exec.sort_values(by=["ID_NORM", "item_name"], ascending=[True, True], kind="stable")
            .drop_duplicates(subset=["ID_NORM"], keep="first")
            .copy()
        )
        dropped_len = before_len - len(df_create_exec)
        if dropped_len > 0:
            log_warn(f"CREATE: removidos {dropped_len} duplicados por ID antes de criar")

    df_create_exec = df_create_exec.sort_values(
        by=["matched_destino_key", "BOARD_ID_DESTINO_FINAL", "GROUP_ID_DESTINO_FINAL", "item_name"],
        kind="stable",
    ).copy()

    create_mutation = _build_create_item_mutation()
    result_rows: List[Dict[str, Any]] = []

    grouped_create = df_create_exec.groupby(
        ["matched_destino_key", "BOARD_ID_DESTINO_FINAL", "GROUP_ID_DESTINO_FINAL"],
        dropna=False,
        sort=True,
    )

    for (dest_key, board_id, group_id), df_create_group in grouped_create:
        success_count = 0
        error_count = 0

        for _, create_row in tqdm(
            df_create_group.iterrows(),
            total=len(df_create_group),
            desc=f"Create {dest_key}",
        ):
            item_name = create_row["item_name"]
            column_values = create_row.get("column_values", {}) or {}

            if dry_run:
                result_rows.append(
                    {
                        "matched_destino_key": dest_key,
                        "BOARD_ID_DESTINO_FINAL": board_id,
                        "GROUP_ID_DESTINO_FINAL": group_id,
                        "item_name": item_name,
                        "ID": create_row.get("ID", ""),
                        "status": "dry_run",
                        "item_id_destino": None,
                        "error": None,
                    }
                )
                success_count += 1
                continue

            try:
                mutation_data = execute_monday_query(
                    query=create_mutation,
                    variables={
                        "board_id": str(board_id),
                        "group_id": str(group_id),
                        "item_name": item_name,
                        "column_values": _to_json_string(column_values),
                    },
                    operation_name="create_item_destino",
                )
                item_destino_id = ((mutation_data or {}).get("create_item") or {}).get("id")
                result_rows.append(
                    {
                        "matched_destino_key": dest_key,
                        "BOARD_ID_DESTINO_FINAL": board_id,
                        "GROUP_ID_DESTINO_FINAL": group_id,
                        "item_name": item_name,
                        "ID": create_row.get("ID", ""),
                        "status": "created",
                        "item_id_destino": item_destino_id,
                        "error": None,
                    }
                )
                success_count += 1
            except Exception as exc:
                result_rows.append(
                    {
                        "matched_destino_key": dest_key,
                        "BOARD_ID_DESTINO_FINAL": board_id,
                        "GROUP_ID_DESTINO_FINAL": group_id,
                        "item_name": item_name,
                        "ID": create_row.get("ID", ""),
                        "status": "error",
                        "item_id_destino": None,
                        "error": str(exc)[:500],
                    }
                )
                error_count += 1

            if CREATE_SLEEP_SECONDS > 0:
                time.sleep(CREATE_SLEEP_SECONDS)

        log_info(
            f"CREATE {dest_key}: sucesso={success_count} | erro={error_count} | total={len(df_create_group)}"
        )

    return pd.DataFrame(result_rows)
