# AL Payments

Pipeline para sincronizacao e saneamento do board de pagamentos no Monday.com.

## Fluxo

1. Le origem (boards Py de pagamentos realizados)
2. Le destino (5 boards de pagamentos)
3. Normaliza dados
4. Filtra somente registros `matched` por centro de custo
5. Calcula diff por `(ID_NORM, BOARD_EXPECTED)`
6. Dedupe pre-enrich por ID
7. Enriquece dados para create
8. Cria itens faltantes no destino
9. Recarrega destino para auditoria
10. Detecta e remove duplicados
11. Detecta wrong board / wrong group / no origin
12. Aplica deletes e moves corretivos
13. Gera resumo final

## Regras de negocio importantes

- O pipeline considera apenas os 5 centros de custo mapeados nos boards de destino.
- Registros fora desse escopo sao ignorados silenciosamente.
- `no_match`, `conflict` e `origem_invalid_id` nao fazem parte do fluxo operacional.
- Dedupe de destino mantem o menor `ID_ITEM_MONDAY_DESTINO` (mais antigo).

## Execucao local

```bash
python -u -m src.main
```

## Docker

```bash
docker compose up --build
```

## Airflow (exemplo)

```bash
docker run --rm \
  --env-file /opt/automations/al_payments/.env \
  conterp-al-payments-app:latest
```
