# AL-PAYMENTS-SYNC

Pipeline automatizado para organizar os **pagamentos realizados** no Monday.com em uma estrutura de **Access Level**, garantindo que cada lideranca visualize apenas os pagamentos dos seus respectivos centros de custo.

---

## O que ele faz

- Le pagamentos realizados da origem e itens do destino
- Normaliza e cruza dados por `ID`
- Filtra apenas centros de custo mapeados (`matched`)
- Identifica itens faltantes no destino
- Enriquece dados da origem para criacao
- Cria itens faltantes no board correto
- Detecta e remove duplicados
- Corrige itens em board/grupo errado
- Remove itens sem origem
- Gera resumo operacional por etapa
- Gera reconciliacao final por destino (`EXPECTED`, `ACTUAL`, `DELTA`)
- Envia o resumo final para um webhook n8n

---

## Access Level (1 de 4 pipelines)

Este pipeline e o **1/4** do projeto de niveis de acesso no Monday.com.

- Prefixo `AL` = **Access Level**
- O projeto completo e composto por 4 pipelines integrados:
  - **AFs Geradas**
  - **Base RH**
  - **Pagamentos Realizados**
  - **Faturamento**
- Este repositorio cobre o fluxo de **payments realizados sync**

---

## Estrutura (resumida)

```bash
al_payments/
+-- Dockerfile
+-- docker-compose.yml
+-- requirements.txt
+-- src/
    +-- main.py
    +-- config/settings.py
    +-- core/
        +-- webhook/
        |   +-- send_to_n8n.py
        +-- monday/
            +-- execute_monday_query.py
            +-- origin/
            |   +-- fetch_origin_items.py
            |   +-- enrich_origin_items.py
            +-- destination/
                +-- fetch/
                +-- payload/
                +-- actions/
                |   +-- duplicates/
                |   +-- orphans/
                +-- summary/
```

---

## Configuracao

Crie o `.env` a partir do exemplo e preencha as variaveis obrigatorias:

```env
MONDAY_API_TOKEN=seu_token
MONDAY_BASE_URL=https://api.monday.com/v2
N8N_SUMMARY_WEBHOOK_URL=https://seu-webhook-n8n
PIPELINE_SHOW_PROGRESS=true
```

Variavel opcional:

```env
N8N_REQUEST_TIMEOUT=60
```

> Use sempre `CHAVE=valor` sem aspas e sem espaco apos `=`.
> A URL do webhook n8n deve ser tratada como segredo e nao deve ser versionada.

---

## Execucao

### Local

```bash
python -u -m src.main
```

### Docker

```bash
docker compose up --build
```

---

## Airflow (producao)

- `dag_id`: `al_payments_sync`
- cron: `10 9,21 * * 1-6` (seg-sab: 09:10 e 21:10)

Comando da task:

```bash
docker run --rm \
  --env-file /opt/automations/al_payments/.env \
  conterp-al-payments-app:latest
```

> O `.env` utilizado pela task no Airflow precisa conter `N8N_SUMMARY_WEBHOOK_URL`, pois o pipeline valida essa variavel no inicio da execucao.

---

## Saida operacional

O pipeline imprime:

- checkpoints por etapa (`CKPT START/END`)
- DataFrames de controle por etapa
- auditoria de inconsistencias (`wrong board`, `wrong group`, `no origin`)
- resumo final de execucao
- reconciliacao por destino:
  - `DESTINO_KEY`
  - `EXPECTED_ROWS`
  - `ACTUAL_ROWS`
  - `DELTA`
- envio do resumo final para n8n

---

## Observabilidade

Ao final da execucao, o pipeline envia para o webhook n8n o mesmo resumo operacional usado no log final.

O payload contem:

- `pipeline`
- `execution_summary`
- `reconciliation_summary`
- totais separados da execucao
- totais separados da reconciliacao

Exemplo resumido:

```json
{
  "pipeline": "payments",
  "execution_summary": [],
  "reconciliation_summary": [],
  "execution_total_planned": 115,
  "execution_total_success": 115,
  "execution_total_error": 0,
  "pipeline_duration": "5m 49s",
  "reconciliation_total_expected_rows": 11751,
  "reconciliation_total_actual_rows": 11751,
  "reconciliation_total_delta": 0,
  "reconciliation_has_divergence": false
}
```

O pipeline nao monta nem envia mensagem de WhatsApp diretamente.
Ele apenas transmite os dados finais para que o tratamento, roteamento e notificacao sejam feitos no fluxo do n8n.

---

## Seguranca

- Segredos via `.env` (nao versionar)
- Execucao conteinerizada
- Retry/backoff para chamadas de API
- Webhook n8n tratado como segredo
- Recomenda-se rotacao periodica do token da API

---

## Autor

**Joao Carser**  
[github.com/JoaoCarser](https://github.com/JoaoCarser)
