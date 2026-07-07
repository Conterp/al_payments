from __future__ import annotations

from typing import Any, Dict, List, Tuple
import json
import time
from typing import Tuple

import pandas as pd


def _result_count(df: pd.DataFrame, ok_status: str) -> Tuple[int, int]:
    if df is None or df.empty or "status" not in df.columns:
        return 0, 0
    success = int((df["status"] == ok_status).sum())
    error = int((df["status"] == "error").sum())
    return success, error


def build_df_execution_summary(
    pipeline_start_ts: float,
    df_create_result: pd.DataFrame,
    df_duplicate_delete: pd.DataFrame,
    df_delete_duplicate_result: pd.DataFrame,
    df_wrong_board: pd.DataFrame,
    df_delete_wrong_board_result: pd.DataFrame,
    df_wrong_group: pd.DataFrame,
    df_move_wrong_group_result: pd.DataFrame,
    df_no_origin: pd.DataFrame,
    df_delete_no_origin_result: pd.DataFrame,
    create_dry_run: bool,
    duplicate_dry_run: bool,
    wrong_board_dry_run: bool,
    wrong_group_dry_run: bool,
    no_origin_dry_run: bool,
) -> pd.DataFrame:
    rows = []

    if df_create_result is not None and not df_create_result.empty:
        planned_create = len(df_create_result)
        success_create = int((df_create_result["status"] == ("dry_run" if create_dry_run else "created")).sum())
        error_create = int((df_create_result["status"] == "error").sum())
    else:
        planned_create = 0
        success_create = 0
        error_create = 0

    rows.append(
        {
            "ACTION": "CREATE DESTINATION ITEMS",
            "PLANNED": planned_create,
            "SUCCESS": success_create,
            "ERROR": error_create,
        }
    )

    dup_success, dup_error = _result_count(
        df_delete_duplicate_result,
        "dry_run" if duplicate_dry_run else "deleted",
    )
    rows.append(
        {
            "ACTION": "DELETE DUPLICATES",
            "PLANNED": len(df_duplicate_delete) if df_duplicate_delete is not None else 0,
            "SUCCESS": dup_success,
            "ERROR": dup_error,
        }
    )

    wb_success, wb_error = _result_count(
        df_delete_wrong_board_result,
        "dry_run" if wrong_board_dry_run else "deleted",
    )
    rows.append(
        {
            "ACTION": "DELETE WRONG BOARD",
            "PLANNED": len(df_wrong_board) if df_wrong_board is not None else 0,
            "SUCCESS": wb_success,
            "ERROR": wb_error,
        }
    )

    wg_success, wg_error = _result_count(
        df_move_wrong_group_result,
        "dry_run" if wrong_group_dry_run else "moved",
    )
    rows.append(
        {
            "ACTION": "MOVE WRONG GROUP",
            "PLANNED": len(df_wrong_group) if df_wrong_group is not None else 0,
            "SUCCESS": wg_success,
            "ERROR": wg_error,
        }
    )

    no_success, no_error = _result_count(
        df_delete_no_origin_result,
        "dry_run" if no_origin_dry_run else "deleted",
    )
    rows.append(
        {
            "ACTION": "DELETE NO ORIGIN",
            "PLANNED": len(df_no_origin) if df_no_origin is not None else 0,
            "SUCCESS": no_success,
            "ERROR": no_error,
        }
    )

    summary_df = pd.DataFrame(rows).reset_index(drop=True)
    summary_df.insert(0, "STEP", range(len(summary_df)))

    elapsed_sec = int(time.time() - pipeline_start_ts)
    duration_text = f"{elapsed_sec // 60}m {elapsed_sec % 60}s"
    summary_df = pd.concat(
        [
            summary_df,
            pd.DataFrame(
                [
                    {
                        "STEP": len(summary_df),
                        "ACTION": "PIPELINE DURATION",
                        "PLANNED": duration_text,
                        "SUCCESS": "",
                        "ERROR": "",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    return summary_df


def build_df_expected_by_dest(df_origem_matched: pd.DataFrame) -> pd.DataFrame:
    if df_origem_matched is None or df_origem_matched.empty:
        return pd.DataFrame(columns=["DESTINO_KEY", "EXPECTED_ROWS"])

    df_expected_by_dest = (
        df_origem_matched["matched_destino_key"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .value_counts()
        .rename_axis("DESTINO_KEY")
        .reset_index(name="EXPECTED_ROWS")
        .sort_values("DESTINO_KEY")
        .reset_index(drop=True)
    )
    return df_expected_by_dest


def build_df_actual_by_dest(df_dest_audit: pd.DataFrame) -> pd.DataFrame:
    if df_dest_audit is None or df_dest_audit.empty:
        return pd.DataFrame(columns=["DESTINO_KEY", "ACTUAL_ROWS"])

    df_actual_by_dest = (
        df_dest_audit["BOARD_KEY"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .value_counts()
        .rename_axis("DESTINO_KEY")
        .reset_index(name="ACTUAL_ROWS")
        .sort_values("DESTINO_KEY")
        .reset_index(drop=True)
    )
    return df_actual_by_dest


def build_df_reconcile_by_dest(
    df_expected_by_dest: pd.DataFrame,
    df_actual_by_dest: pd.DataFrame,
) -> pd.DataFrame:
    if df_expected_by_dest is None:
        df_expected_by_dest = pd.DataFrame(columns=["DESTINO_KEY", "EXPECTED_ROWS"])
    if df_actual_by_dest is None:
        df_actual_by_dest = pd.DataFrame(columns=["DESTINO_KEY", "ACTUAL_ROWS"])

    df_reconcile_by_dest = df_expected_by_dest.merge(
        df_actual_by_dest,
        on="DESTINO_KEY",
        how="outer",
    ).fillna(0)

    df_reconcile_by_dest["EXPECTED_ROWS"] = df_reconcile_by_dest["EXPECTED_ROWS"].astype(int)
    df_reconcile_by_dest["ACTUAL_ROWS"] = df_reconcile_by_dest["ACTUAL_ROWS"].astype(int)
    df_reconcile_by_dest["DELTA"] = (
        df_reconcile_by_dest["ACTUAL_ROWS"] - df_reconcile_by_dest["EXPECTED_ROWS"]
    )

    return df_reconcile_by_dest.sort_values("DESTINO_KEY").reset_index(drop=True)

def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    df_safe = df.astype(object).where(pd.notnull(df), None)
    return df_safe.to_dict(orient="records")


def _to_number(value: Any) -> int:
    if value in (None, ""):
        return 0

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _find_by_action(records: List[Dict[str, Any]], action: str) -> Dict[str, Any]:
    return next(
        (item for item in records if item.get("ACTION") == action),
        {},
    )


def _find_by_destino(records: List[Dict[str, Any]], destino_key: str) -> Dict[str, Any]:
    return next(
        (item for item in records if item.get("DESTINO_KEY") == destino_key),
        {},
    )


def _get_action_metric(
    records: List[Dict[str, Any]],
    action: str,
    metric: str,
) -> int:
    item = _find_by_action(records, action)
    return _to_number(item.get(metric))


def _get_destino_metric(
    records: List[Dict[str, Any]],
    destino_key: str,
    metric: str,
) -> int:
    item = _find_by_destino(records, destino_key)
    return _to_number(item.get(metric))


def build_summary_payload(
    df_execution_summary: pd.DataFrame,
    df_reconciliation_summary: pd.DataFrame,
) -> Dict[str, Any]:
    execution_summary = _df_to_records(df_execution_summary)
    reconciliation_summary = _df_to_records(df_reconciliation_summary)

    execution_rows = [
        item
        for item in execution_summary
        if item.get("ACTION") != "PIPELINE DURATION"
    ]

    pipeline_duration = _find_by_action(
        execution_summary,
        "PIPELINE DURATION",
    ).get("PLANNED", "")

    return {
        "pipeline": "payments",

        "execution_summary": execution_summary,
        "reconciliation_summary": reconciliation_summary,

        "create_destination_items_planned": _get_action_metric(execution_summary, "CREATE DESTINATION ITEMS", "PLANNED"),
        "create_destination_items_success": _get_action_metric(execution_summary, "CREATE DESTINATION ITEMS", "SUCCESS"),
        "create_destination_items_error": _get_action_metric(execution_summary, "CREATE DESTINATION ITEMS", "ERROR"),

        "delete_duplicates_planned": _get_action_metric(execution_summary, "DELETE DUPLICATES", "PLANNED"),
        "delete_duplicates_success": _get_action_metric(execution_summary, "DELETE DUPLICATES", "SUCCESS"),
        "delete_duplicates_error": _get_action_metric(execution_summary, "DELETE DUPLICATES", "ERROR"),

        "delete_wrong_board_planned": _get_action_metric(execution_summary, "DELETE WRONG BOARD", "PLANNED"),
        "delete_wrong_board_success": _get_action_metric(execution_summary, "DELETE WRONG BOARD", "SUCCESS"),
        "delete_wrong_board_error": _get_action_metric(execution_summary, "DELETE WRONG BOARD", "ERROR"),

        "move_wrong_group_planned": _get_action_metric(execution_summary, "MOVE WRONG GROUP", "PLANNED"),
        "move_wrong_group_success": _get_action_metric(execution_summary, "MOVE WRONG GROUP", "SUCCESS"),
        "move_wrong_group_error": _get_action_metric(execution_summary, "MOVE WRONG GROUP", "ERROR"),

        "delete_no_origin_planned": _get_action_metric(execution_summary, "DELETE NO ORIGIN", "PLANNED"),
        "delete_no_origin_success": _get_action_metric(execution_summary, "DELETE NO ORIGIN", "SUCCESS"),
        "delete_no_origin_error": _get_action_metric(execution_summary, "DELETE NO ORIGIN", "ERROR"),

        "pipeline_duration": pipeline_duration,

        "execution_total_planned": sum(_to_number(item.get("PLANNED")) for item in execution_rows),
        "execution_total_success": sum(_to_number(item.get("SUCCESS")) for item in execution_rows),
        "execution_total_error": sum(_to_number(item.get("ERROR")) for item in execution_rows),
        "execution_has_error": any(_to_number(item.get("ERROR")) > 0 for item in execution_rows),

        "atp_expected_rows": _get_destino_metric(reconciliation_summary, "ATP", "EXPECTED_ROWS"),
        "atp_actual_rows": _get_destino_metric(reconciliation_summary, "ATP", "ACTUAL_ROWS"),
        "atp_delta": _get_destino_metric(reconciliation_summary, "ATP", "DELTA"),

        "eneva_expected_rows": _get_destino_metric(reconciliation_summary, "ENEVA", "EXPECTED_ROWS"),
        "eneva_actual_rows": _get_destino_metric(reconciliation_summary, "ENEVA", "ACTUAL_ROWS"),
        "eneva_delta": _get_destino_metric(reconciliation_summary, "ENEVA", "DELTA"),

        "fluidos_mar_expected_rows": _get_destino_metric(reconciliation_summary, "FLUIDOS_MAR", "EXPECTED_ROWS"),
        "fluidos_mar_actual_rows": _get_destino_metric(reconciliation_summary, "FLUIDOS_MAR", "ACTUAL_ROWS"),
        "fluidos_mar_delta": _get_destino_metric(reconciliation_summary, "FLUIDOS_MAR", "DELTA"),

        "fs_bio_cpt01_expected_rows": _get_destino_metric(reconciliation_summary, "FS_BIO_CPT01", "EXPECTED_ROWS"),
        "fs_bio_cpt01_actual_rows": _get_destino_metric(reconciliation_summary, "FS_BIO_CPT01", "ACTUAL_ROWS"),
        "fs_bio_cpt01_delta": _get_destino_metric(reconciliation_summary, "FS_BIO_CPT01", "DELTA"),

        "spts_expected_rows": _get_destino_metric(reconciliation_summary, "SPTS", "EXPECTED_ROWS"),
        "spts_actual_rows": _get_destino_metric(reconciliation_summary, "SPTS", "ACTUAL_ROWS"),
        "spts_delta": _get_destino_metric(reconciliation_summary, "SPTS", "DELTA"),

        "reconciliation_total_expected_rows": sum(_to_number(item.get("EXPECTED_ROWS")) for item in reconciliation_summary),
        "reconciliation_total_actual_rows": sum(_to_number(item.get("ACTUAL_ROWS")) for item in reconciliation_summary),
        "reconciliation_total_delta": sum(_to_number(item.get("DELTA")) for item in reconciliation_summary),
        "reconciliation_has_divergence": any(_to_number(item.get("DELTA")) != 0 for item in reconciliation_summary),
    }

if __name__ == "__main__":
    from src.core.webhook.send_to_n8n import send_summary_to_n8n

    df_execution_summary_test = pd.DataFrame(
        [
            {
                "STEP": 0,
                "ACTION": "CREATE DESTINATION ITEMS",
                "PLANNED": 2,
                "SUCCESS": 2,
                "ERROR": 0,
            },
            {
                "STEP": 1,
                "ACTION": "DELETE DUPLICATES",
                "PLANNED": 1,
                "SUCCESS": 1,
                "ERROR": 0,
            },
            {
                "STEP": 2,
                "ACTION": "DELETE WRONG BOARD",
                "PLANNED": 0,
                "SUCCESS": 0,
                "ERROR": 0,
            },
            {
                "STEP": 3,
                "ACTION": "MOVE WRONG GROUP",
                "PLANNED": 3,
                "SUCCESS": 2,
                "ERROR": 1,
            },
            {
                "STEP": 4,
                "ACTION": "DELETE NO ORIGIN",
                "PLANNED": 1,
                "SUCCESS": 1,
                "ERROR": 0,
            },
            {
                "STEP": 5,
                "ACTION": "PIPELINE DURATION",
                "PLANNED": "3m 45s",
                "SUCCESS": "",
                "ERROR": "",
            },
        ]
    )

    df_reconciliation_summary_test = pd.DataFrame(
        [
        
            {
                "DESTINO_KEY": "ATP",
                "EXPECTED_ROWS": 2487,
                "ACTUAL_ROWS": 2487,
                "DELTA": 0,
            },
            {
                "DESTINO_KEY": "ENEVA",
                "EXPECTED_ROWS": 2198,
                "ACTUAL_ROWS": 2204,
                "DELTA": 6,
            },
            {
                "DESTINO_KEY": "FLUIDOS_MAR",
                "EXPECTED_ROWS": 312,
                "ACTUAL_ROWS": 309,
                "DELTA": -3,
            },
            {
                "DESTINO_KEY": "FS_BIO_CPT01",
                "EXPECTED_ROWS": 142,
                "ACTUAL_ROWS": 142,
                "DELTA": 0,
            },
            {
                "DESTINO_KEY": "SPTS",
                "EXPECTED_ROWS": 317,
                "ACTUAL_ROWS": 301,
                "DELTA": -16,
            },
        ]
        
    )

    payload = build_summary_payload(
        df_execution_summary=df_execution_summary_test,
        df_reconciliation_summary=df_reconciliation_summary_test,
    )

    send_summary_to_n8n(payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))