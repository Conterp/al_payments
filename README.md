# 🧾 AL-PAYMENTS-SYNC

Pipeline automatizado para organizar os **pagamentos realizados** no Monday.com em uma estrutura de **Access Level**, garantindo que cada liderança visualize apenas os pagamentos dos seus respectivos centros de custo.

---

## 🚀 O que ele faz

- Lê pagamentos realizados da origem e itens do destino
- Normaliza e cruza dados por `ID`
- Filtra apenas centros de custo mapeados (`matched`)
- Identifica itens faltantes no destino
- Enriquece dados da origem para criação
- Cria itens faltantes no board correto
- Detecta e remove duplicados
- Corrige itens em board/grupo errado
- Remove itens sem origem
- Gera resumo operacional por etapa
- Gera reconciliação final por destino (`EXPECTED`, `ACTUAL`, `DELTA`)

---

## 🧩 Access Level (1 de 4 pipelines)

Este pipeline é o **1/4** do projeto de níveis de acesso no Monday.com.

- Prefixo `AL` = **Access Level**
- O projeto completo é composto por 4 pipelines integrados
- Este repositório cobre o fluxo de **payments realizados sync**

---

## 🧩 Estrutura (resumida)

```bash
al_payments/
+-- Dockerfile
+-- docker-compose.yml
+-- requirements.txt
+-- src/
    +-- main.py
    +-- config/settings.py
    +-- core/monday/
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

## ⚙️ Configuração

Crie o `.env` a partir do exemplo e preencha as variáveis obrigatórias:

```env
MONDAY_API_TOKEN=seu_token
MONDAY_BASE_URL=https://api.monday.com/v2
PIPELINE_SHOW_PROGRESS=true
```

> Use sempre `CHAVE=valor` sem aspas e sem espaço após `=`.

---

## 🧪 Execução

### Local
```bash
python -u -m src.main
```

### Docker
```bash
docker compose up --build
```

---

## 🌬️ Airflow (produção)

- `dag_id`: `al_payments_sync`
- cron: `10 9,21 * * 1-6` (seg-sáb: 09:10 e 21:10)

Comando da task:

```bash
docker run --rm \
  --env-file /opt/automations/al_payments/.env \
  conterp-al-payments-app:latest
```

---

## 📊 Saída operacional

O pipeline imprime:

- checkpoints por etapa (`CKPT START/END`)
- DataFrames de controle por etapa
- auditoria de inconsistências (`wrong board`, `wrong group`, `no origin`)
- resumo final de execução
- reconciliação por destino:
  - `DESTINO_KEY`
  - `EXPECTED_ROWS`
  - `ACTUAL_ROWS`
  - `DELTA`

---

## 🔒 Segurança

- Segredos via `.env` (não versionar)
- Execução conteinerizada
- Retry/backoff para chamadas de API
- Recomenda-se rotação periódica do token da API

---

## 🤝 Autor

**João Carser**  
[github.com/JoaoCarser](https://github.com/JoaoCarser)
