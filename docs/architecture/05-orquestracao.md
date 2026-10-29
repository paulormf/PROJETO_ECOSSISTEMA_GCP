# Orquestração da plataforma

## Objetivo

Executar a carga horária de forma controlada, idempotente e observável, separando a ingestão Python das transformações Dataform.

## Fluxo de execução

```text
Cloud Scheduler
        |
        v
Workflows
        |
        v
Cloud Run Job - ingestão Python
        |
        +--> PostgreSQL: change log
        +--> GCS: landing Parquet
        +--> BigQuery: Bronze incremental
        |
        v
Dataform Workflow Invocation
        |
        +--> Silver
        +--> Assertions
        |
        v
Registro de logs e métricas
```

## Responsabilidade dos componentes

### Cloud Scheduler

Dispara o workflow uma vez por hora. O agendamento não acessa diretamente o PostgreSQL nem o BigQuery.

### Workflows

Controla a sequência, timeout, retries e tratamento de falhas:

1. Inicia o Cloud Run Job de ingestão.
2. Aguarda a conclusão do job.
3. Interrompe o fluxo se a ingestão falhar.
4. Inicia a execução do Dataform.
5. Registra o resultado final.

### Cloud Run Job

Executa o código Python com `python -m`, sem console scripts ou arquivos `.exe`. O job executa:

1. Leitura do change log PostgreSQL.
2. Escrita dos arquivos Parquet na landing do GCS.
3. Carga idempotente na tabela técnica de eventos Bronze.
4. Aplicação das alterações nas tabelas Bronze.
5. Atualização dos checkpoints.

### Dataform

Executa os modelos Silver e as assertions somente depois da ingestão Bronze concluir com sucesso.

## Segurança

- A senha do PostgreSQL ficará no Secret Manager.
- O Cloud Run usará uma conta de serviço própria.
- O Dataform continuará usando a conta `sa-dataform-dev` no ambiente dev.
- Cada ambiente deverá possuir datasets e permissões separados.
- Nenhum segredo será armazenado no GitHub ou no código-fonte.

## Ambientes

O fluxo usará o mesmo código, com o ambiente informado por configuração:

```text
dev  -> dev_control, dev_bronze, dev_silver, dev_gold
hom  -> hom_control, hom_bronze, hom_silver, hom_gold
prod -> prod_control, prod_bronze, prod_silver, prod_gold
```

## Estratégia de falhas

- Falha na ingestão: o Dataform não é executado.
- Falha no Dataform: o próximo ciclo reprocessa a partir do checkpoint confirmado.
- Retry do workflow: operações de ingestão e merge devem permanecer idempotentes.
- Falha após escrita parcial: o checkpoint só é confirmado após a etapa correspondente concluir.

## Ordem de implementação

1. Containerização do módulo Python.
2. Execução local equivalente ao Cloud Run Job.
3. Conta de serviço e Secret Manager.
4. Cloud Run Job no ambiente dev.
5. Workflow manual no ambiente dev.
6. Invocação do Dataform pelo Workflow.
7. Cloud Scheduler horário.
8. GitHub Actions para build e deploy.
9. Configuração de homologação e produção.
