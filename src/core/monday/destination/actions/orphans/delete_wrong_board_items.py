from __future__ import annotations

import pandas as pd

from src.core.monday.destination.actions.duplicates.delete_duplicate_items import run_delete_items


def build_df_wrong_board_delete_results(
    df_wrong_board: pd.DataFrame,
    dry_run: bool,
) -> pd.DataFrame:
    return run_delete_items(
        df_input=df_wrong_board,
        reason_label="wrong_board",
        dry_run=dry_run,
    )
