from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import ACTION_SLEEP_SECONDS, LOG_PREFIX, MOSTRAR_PROGRESSO
from src.core.monday.execute_monday_query import execute_monday_query


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def _build_delete_item_mutation() -> str:
    return """
    mutation ($item_id: ID!) {
      delete_item(item_id: $item_id) {
        id
      }
    }
    """


def run_delete_items(
    df_input: pd.DataFrame,
    reason_label: str,
    dry_run: bool,
) -> pd.DataFrame:
    if df_input.empty:
        return pd.DataFrame()

    mutation = _build_delete_item_mutation()
    rows: List[Dict[str, Any]] = []

    iterator = df_input.iterrows()
    if MOSTRAR_PROGRESSO:
        iterator = tqdm(iterator, total=len(df_input), desc=f"Delete {reason_label}")

    for _, row in iterator:
        item_id = str(row.get("ID_ITEM_MONDAY_DESTINO", "")).strip()
        if not item_id:
            rows.append({"status": "error", "reason": reason_label, "item_id": item_id, "error": "missing_item_id"})
            continue

        if dry_run:
            rows.append({"status": "dry_run", "reason": reason_label, "item_id": item_id, "error": None})
            continue

        try:
            execute_monday_query(
                query=mutation,
                variables={"item_id": item_id},
                operation_name=f"delete_item_{reason_label}",
            )
            rows.append({"status": "deleted", "reason": reason_label, "item_id": item_id, "error": None})
        except Exception as exc:
            rows.append({"status": "error", "reason": reason_label, "item_id": item_id, "error": str(exc)[:500]})

        if ACTION_SLEEP_SECONDS > 0:
            time.sleep(ACTION_SLEEP_SECONDS)

    result = pd.DataFrame(rows)
    log_info(f"Delete {reason_label}: total={len(result)}")
    return result
