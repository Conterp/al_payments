from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import ACTION_SLEEP_SECONDS, LOG_PREFIX, MOSTRAR_PROGRESSO
from src.core.monday.execute_monday_query import execute_monday_query


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def _build_move_item_group_mutation() -> str:
    return """
    mutation ($item_id: ID!, $group_id: String!) {
      move_item_to_group(item_id: $item_id, group_id: $group_id) {
        id
      }
    }
    """


def build_df_wrong_group_move_results(
    df_wrong_group: pd.DataFrame,
    dry_run: bool,
) -> pd.DataFrame:
    if df_wrong_group.empty:
        return pd.DataFrame()

    mutation = _build_move_item_group_mutation()
    rows: List[Dict[str, Any]] = []

    iterator = df_wrong_group.iterrows()
    if MOSTRAR_PROGRESSO:
        iterator = tqdm(iterator, total=len(df_wrong_group), desc="Move wrong_group")

    for _, row in iterator:
        item_id = str(row.get("ID_ITEM_MONDAY_DESTINO", "")).strip()
        target_group_id = str(row.get("EXPECTED_GROUP_ID", "")).strip()

        if not item_id or not target_group_id:
            rows.append(
                {
                    "status": "error",
                    "item_id": item_id,
                    "target_group_id": target_group_id,
                    "error": "missing_item_or_group",
                }
            )
            continue

        if dry_run:
            rows.append(
                {
                    "status": "dry_run",
                    "item_id": item_id,
                    "target_group_id": target_group_id,
                    "error": None,
                }
            )
            continue

        try:
            execute_monday_query(
                query=mutation,
                variables={"item_id": item_id, "group_id": target_group_id},
                operation_name="move_wrong_group",
            )
            rows.append(
                {
                    "status": "moved",
                    "item_id": item_id,
                    "target_group_id": target_group_id,
                    "error": None,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "status": "error",
                    "item_id": item_id,
                    "target_group_id": target_group_id,
                    "error": str(exc)[:500],
                }
            )

        if ACTION_SLEEP_SECONDS > 0:
            time.sleep(ACTION_SLEEP_SECONDS)

    result = pd.DataFrame(rows)
    log_info(f"Move wrong_group: total={len(result)}")
    return result
