from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import BOARDS_DESTINO, COL_CC, COL_NR_TITULO, LOG_PREFIX, MOSTRAR_PROGRESSO, PAGE_LIMIT
from src.core.monday.destination.payload.build_missing_ids import normalize_cc_for_match, normalize_id
from src.core.monday.destination.fetch.fetch_destination_items import (
    _build_req_destino_query_initial,
    _build_req_destino_query_next,
)
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import _extract_text_map


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def fetch_board_items_destino_validacao(
    board_id: str,
    id_column: str,
    limit: int = PAGE_LIMIT,
    max_cursor_restarts: int = 3,
) -> List[Dict[str, Any]]:
    all_items_by_id: Dict[str, Dict[str, Any]] = {}

    restart = 0
    while restart <= max_cursor_restarts:
        restart += 1

        col_ids = [id_column, COL_CC, COL_NR_TITULO]
        data = execute_monday_query(
            query=_build_req_destino_query_initial(),
            variables={"board_id": [board_id], "limit": limit, "column_ids": col_ids},
            operation_name=f"dest_valid_initial_{board_id}_r{restart}",
        )

        boards = data.get("boards", [])
        if not boards:
            return list(all_items_by_id.values())

        items_page = boards[0].get("items_page") or {}
        items = items_page.get("items", []) or []
        cursor = items_page.get("cursor")

        for it in items:
            item_id = str(it.get("id", "")).strip()
            if item_id:
                all_items_by_id[item_id] = it

        cursor_expired = False
        seen_cursors = set()

        while cursor:
            if cursor in seen_cursors:
                break
            seen_cursors.add(cursor)

            try:
                page = execute_monday_query(
                    query=_build_req_destino_query_next(),
                    variables={"cursor": cursor, "limit": limit, "column_ids": col_ids},
                    operation_name=f"dest_valid_next_{board_id}_r{restart}",
                ).get("next_items_page", {})
            except Exception as exc:
                if "CursorExpiredError" in str(exc):
                    cursor_expired = True
                    break
                raise

            page_items = page.get("items", []) or []
            for it in page_items:
                item_id = str(it.get("id", "")).strip()
                if item_id:
                    all_items_by_id[item_id] = it

            cursor = page.get("cursor")

        if not cursor_expired:
            break

    return list(all_items_by_id.values())


def build_df_destination_audit(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    audit_rows = []
    board_iterator = BOARDS_DESTINO.items()

    if show_progress:
        board_iterator = tqdm(board_iterator, total=len(BOARDS_DESTINO), desc="Destino auditoria")

    for destino_key, destino_cfg in board_iterator:
        board_id = destino_cfg["board_id"]
        id_column = destino_cfg["id_column"]

        board_items = fetch_board_items_destino_validacao(
            board_id=board_id,
            id_column=id_column,
            limit=PAGE_LIMIT,
            max_cursor_restarts=3,
        )

        for item in board_items:
            col_map = _extract_text_map(item.get("column_values", []))
            group = item.get("group") or {}

            audit_rows.append(
                {
                    "AF": (item.get("name") or "").strip(),
                    "ID": (col_map.get(id_column) or "").strip(),
                    "ID_NORM": normalize_id(col_map.get(id_column)),
                    "NR_TITULO": (col_map.get(COL_NR_TITULO) or "").strip(),
                    "CENTRO_CUSTO": (col_map.get(COL_CC) or "").strip(),
                    "cc_norm": normalize_cc_for_match(col_map.get(COL_CC)),
                    "ID_ITEM_MONDAY_DESTINO": str(item.get("id", "")).strip(),
                    "BOARD_KEY": destino_key,
                    "BOARD_ID_DESTINO": board_id,
                    "GRUPO_NAME_DESTINO": group.get("title"),
                    "GROUP_ID_DESTINO": group.get("id"),
                }
            )

    df_dest_audit = pd.DataFrame(audit_rows)
    log_info(f"Destino auditoria carregado. total={len(df_dest_audit)}")
    return df_dest_audit
