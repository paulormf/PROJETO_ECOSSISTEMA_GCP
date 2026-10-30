# Runbook operacional

## Fluxo normal

O fluxo incremental é executado pelo Cloud Scheduler a cada 6 horas:

1. Cloud Scheduler inicia o Workflow do ambiente.
2. O Workflow executa o job `ingestion-<ambiente>`.
3. A ingestão lê o `audit.change_log` após o checkpoint do ambiente.
4. Os eventos são gravados no GCS e aplicados na Bronze.
5. O Workflow inicia o Dataform.
6. O Dataform atualiza Silver, Gold, Presentation e assertions.

Os ambientes são independentes:

- `dev`: `orchestrator-dev`
- `hom`: `orchestrator-hom`
- `prod`: `orchestrator-prod`

## Full load

Os jobs `ingestion-<ambiente>-full` são manuais. Eles não fazem parte do
agendamento normal e devem ser usados para carga inicial ou recuperação após
uma indisponibilidade superior à janela de retenção do change log.

## Verificação de uma execução

```powershell
gcloud workflows executions list `
  --workflow=orchestrator-prod `
  --location=us-central1 `
  --project=botbrewpub-ghsurp `
  --limit=5
```

Uma execução saudável deve apresentar `SUCCEEDED`. O Cloud Run e o Dataform
também devem estar concluídos com sucesso.

## Falhas

1. Consultar a execução do Workflow.
2. Identificar se a falha ocorreu no Cloud Run ou no Dataform.
3. Consultar os logs do recurso indicado.
4. Corrigir a causa antes de repetir a execução.
5. Reexecutar o Workflow do mesmo ambiente.

Falha de assertion indica problema de qualidade. Os registros encontrados
ficam no dataset `<ambiente>_assertions` para investigação.

## Retenção

Os objetos da Landing são apagados pelo lifecycle do GCS após 5 dias, conforme
`ops/gcs/lifecycle-5-days.json`.

O `audit.change_log` do PostgreSQL é limpo manualmente. Antes da limpeza,
confirmar que os checkpoints de `dev`, `hom` e `prod` estão atualizados:

```sql
SELECT COUNT(*) AS registros_expirados
FROM audit.change_log
WHERE occurred_at < clock_timestamp() - INTERVAL '5 days';
```

Depois da validação operacional, executar a exclusão manual conforme o
procedimento documentado em `docs/architecture/03-auditoria-change-log.md`.

## Promoção de código

1. Push na `main` executa o deploy de `dev`.
2. Após a validação de `dev`, executar o deploy de `hom` pelo GitHub Actions.
3. Validar o Workflow de `hom`.
4. Executar o deploy de `prod` e validar o Workflow de `prod`.

Deploys de `hom` e `prod` devem promover a mesma imagem imutável validada em
`dev`.
