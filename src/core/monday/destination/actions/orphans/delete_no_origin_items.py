from __future__ import annotations

import pandas as pd

from src.core.monday.destination.actions.duplicates.delete_duplicate_items import run_delete_items


def build_df_no_origin_delete_results(
    df_no_origin: pd.DataFrame,
    dry_run: bool,
) -> pd.DataFrame:
    return run_delete_items(
        df_input=df_no_origin,
        reason_label="no_origin",
        dry_run=dry_run,
    )
