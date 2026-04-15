from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import BOARDS_DESTINO, LOG_PREFIX, MOSTRAR_PROGRESSO, PAGE_LIMIT
from src.core.monday.execute_monday_query import execute_monday_query
from src.core.monday.origin.fetch_origin_items import _extract_text_map


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def _build_req_destino_query_initial() -> str:
    return """
    query ($board_id: [ID!], $limit: Int!, $column_ids: [String!]) {
      boards(ids: $board_id) {
        items_page(limit: $limit) {
          cursor
          items {
            id
            name
            group { id title }
            column_values(ids: $column_ids) { id text }
          }
        }
      }
    }
    """


def _build_req_destino_query_next() -> str:
    return """
    query ($cursor: String!, $limit: Int!, $column_ids: [String!]) {
      next_items_page(cursor: $cursor, limit: $limit) {
        cursor
        items {
          id
          name
          group { id title }
          column_values(ids: $column_ids) { id text }
        }
      }
    }
    """


def fetch_board_items_destino(
    board_id: str,
    id_column: str,
    limit: int = PAGE_LIMIT,
    max_cursor_restarts: int = 3,
) -> List[Dict[str, Any]]:
    all_items_by_id: Dict[str, Dict[str, Any]] = {}

    restart = 0
    while restart <= max_cursor_restarts:
        restart += 1

        initial_data = execute_monday_query(
            query=_build_req_destino_query_initial(),
            variables={"board_id": [board_id], "limit": limit, "column_ids": [id_column]},
            operation_name=f"destino_initial_{board_id}_r{restart}",
        )

        boards = initial_data.get("boards", [])
        if not boards:
            return list(all_items_by_id.values())

        items_page = boards[0].get("items_page") or {}
        board_items = items_page.get("items", []) or []
        cursor = items_page.get("cursor")

        for item in board_items:
            item_id = str(item.get("id", "")).strip()
            if item_id:
                all_items_by_id[item_id] = item

        seen_cursors = set()
        cursor_expired = False

        while cursor:
            if cursor in seen_cursors:
                log_warn(f"Destino {board_id}: cursor repetido detectado. Interrompendo.")
                break
            seen_cursors.add(cursor)

            try:
                next_data = execute_monday_query(
                    query=_build_req_destino_query_next(),
                    variables={"cursor": cursor, "limit": limit, "column_ids": [id_column]},
                    operation_name=f"destino_next_{board_id}_r{restart}",
                )
                page = next_data.get("next_items_page", {})
            except Exception as exc:
                if "CursorExpiredError" in str(exc):
                    cursor_expired = True
                    break
                raise

            page_items = page.get("items", []) or []
            for item in page_items:
                item_id = str(item.get("id", "")).strip()
                if item_id:
                    all_items_by_id[item_id] = item

            next_cursor = page.get("cursor")
            if next_cursor and len(page_items) == 0:
                log_warn(f"Destino {board_id}: pagina vazia com cursor ativo. Interrompendo.")
                break
            cursor = next_cursor

        if not cursor_expired:
            break

    return list(all_items_by_id.values())


def build_df_destino(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    destino_rows: List[Dict[str, Any]] = []
    board_iterator = BOARDS_DESTINO.items()

    if show_progress:
        board_iterator = tqdm(board_iterator, total=len(BOARDS_DESTINO), desc="Lendo destino")

    for destino_key, destino_cfg in board_iterator:
        board_id = destino_cfg["board_id"]
        id_column = destino_cfg["id_column"]

        board_items = fetch_board_items_destino(
            board_id=board_id,
            id_column=id_column,
            limit=PAGE_LIMIT,
            max_cursor_restarts=3,
        )

        for item in board_items:
            col_map = _extract_text_map(item.get("column_values", []))
            group = item.get("group") or {}
            destino_rows.append(
                {
                    "AF": (item.get("name") or "").strip(),
                    "ID": (col_map.get(id_column) or "").strip(),
                    "ID_ITEM_MONDAY_DESTINO": str(item.get("id", "")).strip(),
                    "BOARD_NAME_DESTINO": destino_key,
                    "ID_BOARD_DES": board_id,
                    "GRUPO_NAME_DESTINO": group.get("title"),
                    "ID_GROUP": group.get("id"),
                }
            )

    destino_cols = [
        "AF",
        "ID",
        "ID_ITEM_MONDAY_DESTINO",
        "BOARD_NAME_DESTINO",
        "ID_BOARD_DES",
        "GRUPO_NAME_DESTINO",
        "ID_GROUP",
    ]
    df_destino = pd.DataFrame(destino_rows, columns=destino_cols)
    log_info(f"REQ DESTINO concluido. Total linhas: {len(df_destino)}")
    return df_destino
