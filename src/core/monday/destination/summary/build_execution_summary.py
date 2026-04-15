from __future__ import annotations

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
