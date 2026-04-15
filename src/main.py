from __future__ import annotations

import sys
import time
from typing import Any

from src.config.settings import (
    CREATE_DRY_RUN,
    DELETE_DUPLICATE_DRY_RUN,
    DELETE_NO_ORIGIN_DRY_RUN,
    DELETE_WRONG_BOARD_DRY_RUN,
    LOG_PREFIX,
    MOVE_WRONG_GROUP_DRY_RUN,
    check_required_envs,
)
from src.core.monday.destination.actions.create_monday_items import build_df_create_results
from src.core.monday.destination.actions.duplicates.delete_duplicate_items import run_delete_items
from src.core.monday.destination.actions.duplicates.find_duplicate_items import (
    build_df_duplicates,
    build_df_duplicates_delete,
)
from src.core.monday.destination.actions.orphans.delete_no_origin_items import (
    build_df_no_origin_delete_results,
)
from src.core.monday.destination.actions.orphans.delete_wrong_board_items import (
    build_df_wrong_board_delete_results,
)
from src.core.monday.destination.actions.orphans.find_orphan_items import build_df_orphans
from src.core.monday.destination.actions.orphans.move_wrong_group_items import (
    build_df_wrong_group_move_results,
)
from src.core.monday.destination.payload.build_create_payload import build_df_payload
from src.core.monday.destination.payload.build_missing_ids import (
    build_df_diff_ids,
    dedupe_by_id,
    filter_origem_matched,
    normalize_origem_destino,
)
from src.core.monday.destination.fetch.fetch_destination_audit_items import (
    build_df_destination_audit,
)
from src.core.monday.destination.fetch.fetch_destination_items import build_df_destino
from src.core.monday.destination.summary.build_execution_summary import (
    build_df_actual_by_dest,
    build_df_execution_summary,
    build_df_expected_by_dest,
    build_df_reconcile_by_dest,
)
from src.core.monday.origin.enrich_origin_items import build_df_enriched
from src.core.monday.origin.fetch_origin_items import build_df_origem


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_error(message: str) -> None:
    print(f"{LOG_PREFIX} [ERROR] {message}")


def print_stage(stage_label: str) -> None:
    print("")
    print("============================================================")
    log_info(stage_label)
    print("============================================================")
    print("")


def log_ckpt_start(step_name: str) -> float:
    log_info(f"CKPT START step={step_name}")
    return time.perf_counter()


def log_ckpt_end(step_name: str, start_perf: float, rows: Any | None = None) -> None:
    duration_seconds = time.perf_counter() - start_perf
    if rows is None:
        log_info(f"CKPT END step={step_name} dur_s={duration_seconds:.2f}")
        return
    log_info(f"CKPT END step={step_name} rows={rows} dur_s={duration_seconds:.2f}")


def print_df(df_name: str, df_value: Any) -> None:
    print(f"{df_name}:")
    if df_value is None:
        print("None")
        return
    try:
        print(f"rows={len(df_value)}")
    except Exception:
        pass
    if df_name in {"df_summary", "df_reconcile_by_dest"} and hasattr(df_value, "to_string"):
        print(df_value.to_string(index=False))
        return
    print(df_value)


def main() -> int:
    pipeline_start_ts = time.time()

    try:
        log_info("Iniciando pipeline AL_PAYMENTS")

        print_stage("ETAPA 01 - VALIDAR AMBIENTE")
        stage_perf = log_ckpt_start("check_env")
        check_required_envs()
        log_ckpt_end("check_env", stage_perf)

        print_stage("ETAPA 02 - LER ORIGEM")
        stage_perf = log_ckpt_start("read_origin")
        df_origem = build_df_origem()
        print_df("df_origem", df_origem)
        log_ckpt_end("read_origin", stage_perf, len(df_origem))

        print_stage("ETAPA 03 - LER DESTINO")
        stage_perf = log_ckpt_start("read_destination")
        df_destino = build_df_destino()
        print_df("df_destino", df_destino)
        log_ckpt_end("read_destination", stage_perf, len(df_destino))

        print_stage("ETAPA 04 - NORMALIZAR DADOS")
        stage_perf = log_ckpt_start("normalize_data")
        normalized_data = normalize_origem_destino(df_origem=df_origem, df_destino=df_destino)
        df_origem_norm = normalized_data["df_origem_norm"]
        df_destino_norm = normalized_data["df_destino_norm"]
        # print_df("df_origem_norm", df_origem_norm)
        print_df("df_destino_norm", df_destino_norm)
        log_ckpt_end("normalize_data", stage_perf, len(df_origem_norm) + len(df_destino_norm))

        print_stage("ETAPA 05 - FILTRAR MATCHED")
        stage_perf = log_ckpt_start("filter_matched")
        df_origem_matched = filter_origem_matched(df_origem_norm=df_origem_norm)
        print_df("df_origem_matched", df_origem_matched)
        log_ckpt_end("filter_matched", stage_perf, len(df_origem_matched))

        print_stage("ETAPA 05.1 - EXPECTED POR DESTINO")
        stage_perf = log_ckpt_start("expected_by_dest")
        df_expected_by_dest = build_df_expected_by_dest(df_origem_matched=df_origem_matched)
        print_df("df_expected_by_dest", df_expected_by_dest)
        log_ckpt_end("expected_by_dest", stage_perf, len(df_expected_by_dest))

        print_stage("ETAPA 06 - BUILD DIFF")
        stage_perf = log_ckpt_start("build_diff")
        df_diff_ids = build_df_diff_ids(df_origem_matched=df_origem_matched, df_destino_norm=df_destino_norm)
        print_df("df_diff_ids", df_diff_ids)
        log_ckpt_end("build_diff", stage_perf, len(df_diff_ids))

        print_stage("ETAPA 07 - DEDUPE PRE-ENRICH")
        stage_perf = log_ckpt_start("dedupe_pre_enrich")
        df_diff_ids_dedup = dedupe_by_id(df_diff_ids)
        print_df("df_diff_ids_dedup", df_diff_ids_dedup)
        log_ckpt_end("dedupe_pre_enrich", stage_perf, len(df_diff_ids_dedup))

        print_stage("ETAPA 08 - ENRICH ORIGIN")
        stage_perf = log_ckpt_start("enrich_origin")
        df_enriched = build_df_enriched(df_input=df_diff_ids_dedup)
        print_df("df_enriched", df_enriched)
        log_ckpt_end("enrich_origin", stage_perf, len(df_enriched))

        print_stage("ETAPA 09 - CREATE DESTINO")
        stage_perf = log_ckpt_start("create_destination_items")
        df_payload = build_df_payload(df_enriched=df_enriched)
        print_df("df_payload", df_payload)
        df_create_result = build_df_create_results(df_payload_ok=df_payload, dry_run=CREATE_DRY_RUN)
        print_df("df_create_result", df_create_result)
        log_ckpt_end("create_destination_items", stage_perf, len(df_create_result))

        print_stage("ETAPA 10 - RELOAD AUDITORIA DESTINO")
        stage_perf = log_ckpt_start("reload_destination_audit")
        df_dest_audit = build_df_destination_audit()
        print_df("df_dest_audit", df_dest_audit)
        log_ckpt_end("reload_destination_audit", stage_perf, len(df_dest_audit))

        print_stage("ETAPA 11 - DETECTAR DUPLICADOS")
        stage_perf = log_ckpt_start("detect_duplicates")
        df_duplicates = build_df_duplicates(df_destination_audit=df_dest_audit)
        df_dup_delete = build_df_duplicates_delete(df_duplicates=df_duplicates)
        print_df("df_duplicates", df_duplicates)
        print_df("df_dup_delete", df_dup_delete)
        log_ckpt_end("detect_duplicates", stage_perf, len(df_dup_delete))

        print_stage("ETAPA 12 - DELETE DUPLICADOS")
        stage_perf = log_ckpt_start("delete_duplicates")
        df_delete_duplicate_result = run_delete_items(
            df_input=df_dup_delete,
            reason_label="duplicate",
            dry_run=DELETE_DUPLICATE_DRY_RUN,
        )
        print_df("df_delete_duplicate_result", df_delete_duplicate_result)
        log_ckpt_end("delete_duplicates", stage_perf, len(df_delete_duplicate_result))

        print_stage("ETAPA 13 - DETECTAR WRONGS/ORPHANS")
        stage_perf = log_ckpt_start("detect_orphans")
        df_wrong_board, df_wrong_group, df_no_origin = build_df_orphans(
            df_destination_audit=df_dest_audit,
            df_origem_matched=df_origem_matched,
        )
        print_df("df_wrong_board", df_wrong_board)
        print_df("df_wrong_group", df_wrong_group)
        print_df("df_no_origin", df_no_origin)
        log_ckpt_end("detect_orphans", stage_perf, len(df_wrong_board) + len(df_wrong_group) + len(df_no_origin))

        print_stage("ETAPA 14 - APLICAR LIMPEZA")
        stage_perf = log_ckpt_start("apply_cleanup_actions")
        df_delete_wrong_board_result = build_df_wrong_board_delete_results(
            df_wrong_board=df_wrong_board,
            dry_run=DELETE_WRONG_BOARD_DRY_RUN,
        )
        df_move_wrong_group_result = build_df_wrong_group_move_results(
            df_wrong_group=df_wrong_group,
            dry_run=MOVE_WRONG_GROUP_DRY_RUN,
        )
        df_delete_no_origin_result = build_df_no_origin_delete_results(
            df_no_origin=df_no_origin,
            dry_run=DELETE_NO_ORIGIN_DRY_RUN,
        )
        print_df("df_delete_wrong_board_result", df_delete_wrong_board_result)
        print_df("df_move_wrong_group_result", df_move_wrong_group_result)
        print_df("df_delete_no_origin_result", df_delete_no_origin_result)
        log_ckpt_end(
            "apply_cleanup_actions",
            stage_perf,
            len(df_delete_wrong_board_result) + len(df_move_wrong_group_result) + len(df_delete_no_origin_result),
        )

        print_stage("ETAPA 14.1 - RELOAD AUDITORIA FINAL")
        stage_perf = log_ckpt_start("reload_destination_audit_final")
        df_dest_audit_final = build_df_destination_audit()
        print_df("df_dest_audit_final", df_dest_audit_final)
        log_ckpt_end("reload_destination_audit_final", stage_perf, len(df_dest_audit_final))

        print_stage("ETAPA 15 - RESUMO FINAL")
        stage_perf = log_ckpt_start("build_summary")
        df_summary = build_df_execution_summary(
            pipeline_start_ts=pipeline_start_ts,
            df_create_result=df_create_result,
            df_duplicate_delete=df_dup_delete,
            df_delete_duplicate_result=df_delete_duplicate_result,
            df_wrong_board=df_wrong_board,
            df_delete_wrong_board_result=df_delete_wrong_board_result,
            df_wrong_group=df_wrong_group,
            df_move_wrong_group_result=df_move_wrong_group_result,
            df_no_origin=df_no_origin,
            df_delete_no_origin_result=df_delete_no_origin_result,
            create_dry_run=CREATE_DRY_RUN,
            duplicate_dry_run=DELETE_DUPLICATE_DRY_RUN,
            wrong_board_dry_run=DELETE_WRONG_BOARD_DRY_RUN,
            wrong_group_dry_run=MOVE_WRONG_GROUP_DRY_RUN,
            no_origin_dry_run=DELETE_NO_ORIGIN_DRY_RUN,
        )
        print_df("df_summary", df_summary)
        log_ckpt_end("build_summary", stage_perf, len(df_summary))

        print_stage("ETAPA 15.1 - RECONCILIACAO POR DESTINO")
        stage_perf = log_ckpt_start("reconcile_by_dest")
        df_actual_by_dest = build_df_actual_by_dest(df_dest_audit=df_dest_audit_final)
        df_reconcile_by_dest = build_df_reconcile_by_dest(
            df_expected_by_dest=df_expected_by_dest,
            df_actual_by_dest=df_actual_by_dest,
        )
        # print_df("df_actual_by_dest", df_actual_by_dest)
        print_df("df_reconcile_by_dest", df_reconcile_by_dest)
        log_ckpt_end("reconcile_by_dest", stage_perf, len(df_reconcile_by_dest))

        print_stage("Pipeline AL_PAYMENTS concluido")
        return 0

    except Exception as exc:
        log_error(f"Falha na execucao do pipeline: {exc}")
        raise


if __name__ == "__main__":
    sys.exit(main())
