from __future__ import annotations

from typing import Any, Mapping

import requests

from src.config.settings import (
    LOG_PREFIX,
    N8N_REQUEST_TIMEOUT,
    N8N_SUMMARY_WEBHOOK_URL,
)


def send_summary_to_n8n(payload: Mapping[str, Any]) -> None:
    """
    Envia o payload de resumo do pipeline de pagamentos para o webhook do n8n.

    O payload deve conter as mesmas informacoes exibidas no final do pipeline:
    - execution_summary
    - reconciliation_summary

    Esta funcao nao interpreta nem formata mensagem de WhatsApp.
    Ela apenas envia o conteudo recebido para o webhook configurado.
    """
    if not N8N_SUMMARY_WEBHOOK_URL:
        raise RuntimeError("N8N_SUMMARY_WEBHOOK_URL nao configurado")

    print(f"{LOG_PREFIX} [INFO] Enviando resumo de pagamentos para n8n")

    try:
        response = requests.post(
            N8N_SUMMARY_WEBHOOK_URL,
            json=payload,
            timeout=N8N_REQUEST_TIMEOUT,
        )
        response.raise_for_status()

    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"{LOG_PREFIX} [ERROR] Falha ao enviar resumo de pagamentos para n8n: {exc}"
        ) from exc

    print(
        f"{LOG_PREFIX} [INFO] Resumo de pagamentos enviado para n8n com sucesso "
        f"| status_code={response.status_code}"
    )