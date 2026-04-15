from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import pandas as pd
from tqdm.auto import tqdm

from src.config.settings import (
    BOARDS_ORIGEM,
    COL_CC,
    COL_ID_ORIGEM,
    LOG_PREFIX,
    MOSTRAR_PROGRESSO,
    ORIGEM_COLUMN_IDS_LEITURA_MINIMA,
    PAGE_LIMIT,
)
from src.core.monday.execute_monday_query import execute_monday_query


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def _extract_text_map(column_values: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for column_value in column_values:
        out[column_value["id"]] = column_value.get("text")
    return out


def _build_columns_query() -> str:
    return """
    query ($board_id: [ID!]) { boards(ids: $board_id) { id columns { id } } }
    """


def _build_req_origem_query_initial() -> str:
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


def _build_req_origem_query_next() -> str:
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


def fetch_board_column_ids(board_id: str) -> Set[str]:
    query_data = execute_monday_query(
        query=_build_columns_query(),
        variables={"board_id": [board_id]},
        operation_name=f"origem_columns_{board_id}",
    )
    boards = query_data.get("boards", [])
    if not boards:
        return set()
    return {column.get("id") for column in boards[0].get("columns", []) if column.get("id")}


def fetch_board_items_origem(board_id: str, limit: int = PAGE_LIMIT) -> List[Dict[str, Any]]:
    available_cols = fetch_board_column_ids(board_id)
    effective_column_ids = sorted(list(ORIGEM_COLUMN_IDS_LEITURA_MINIMA.intersection(available_cols)))

    initial_data = execute_monday_query(
        query=_build_req_origem_query_initial(),
        variables={"board_id": [board_id], "limit": limit, "column_ids": effective_column_ids},
        operation_name=f"origem_initial_{board_id}",
    )

    boards = initial_data.get("boards", [])
    if not boards:
        return []

    items_page = boards[0].get("items_page") or {}
    board_items = items_page.get("items", []) or []
    cursor = items_page.get("cursor")

    seen_cursors = set()
    while cursor:
        if cursor in seen_cursors:
            log_warn(f"Origem {board_id}: cursor repetido detectado. Interrompendo.")
            break
        seen_cursors.add(cursor)

        next_data = execute_monday_query(
            query=_build_req_origem_query_next(),
            variables={"cursor": cursor, "limit": limit, "column_ids": effective_column_ids},
            operation_name=f"origem_next_{board_id}",
        )
        page = next_data.get("next_items_page", {})

        page_items = page.get("items", []) or []
        board_items.extend(page_items)

        next_cursor = page.get("cursor")
        if next_cursor and len(page_items) == 0:
            log_warn(f"Origem {board_id}: pagina vazia com cursor ativo. Interrompendo.")
            break
        cursor = next_cursor

    return board_items


def build_df_origem(show_progress: bool = MOSTRAR_PROGRESSO) -> pd.DataFrame:
    origem_rows: List[Dict[str, Any]] = []
    board_iterator = BOARDS_ORIGEM.items()

    if show_progress:
        board_iterator = tqdm(board_iterator, total=len(BOARDS_ORIGEM), desc="Lendo origem")

    for origem_key, origem_cfg in board_iterator:
        board_id = origem_cfg["board_id"]
        board_items = fetch_board_items_origem(board_id=board_id, limit=PAGE_LIMIT)

        for item in board_items:
            col_map = _extract_text_map(item.get("column_values", []))
            group = item.get("group") or {}
            origem_rows.append(
                {
                    "AF": (item.get("name") or "").strip(),
                    "ID": (col_map.get(COL_ID_ORIGEM) or "").strip(),
                    "ID_ITEM_MONDAY_ORIGEM": str(item.get("id", "")).strip(),
                    "BOARD_NAME": origem_key,
                    "ID_BOARD_OR": board_id,
                    "GRUPO_NAME": group.get("title"),
                    "ID_GROUP": group.get("id"),
                    "CENTRO_CUSTO": (col_map.get(COL_CC) or "").strip(),
                }
            )

    origem_cols = [
        "AF",
        "ID",
        "ID_ITEM_MONDAY_ORIGEM",
        "BOARD_NAME",
        "ID_BOARD_OR",
        "GRUPO_NAME",
        "ID_GROUP",
        "CENTRO_CUSTO",
    ]
    df_origem = pd.DataFrame(origem_rows, columns=origem_cols)

    log_info(f"REQ ORIGEM concluido. Total linhas: {len(df_origem)}")
    return df_origem
