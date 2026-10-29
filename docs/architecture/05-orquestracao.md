# Orquestracao da plataforma

## Objetivo

Executar a carga horaria de forma controlada, idempotente e observavel,
separando a ingestao Python das transformacoes Dataform.

## Fluxo alvo

```text
Cloud Scheduler
        |
        v
Workflows
        |
        v
Cloud Run Job - ingestao Python
        |
        +--> PostgreSQL: change log
        +--> GCS: Landing Parquet
        +--> BigQuery: Bronze
        |
        v
Dataform Workflow Invocation
        |
        +--> Silver
        +--> Gold
        +--> Assertions
        |
        v
Cloud Logging e metricas
```

## Responsabilidade dos componentes

### Cloud Scheduler

Dispara o Workflow uma vez por hora. O agendamento nao acessa diretamente o
PostgreSQL nem executa SQL de transformacao.

### Workflows

Controla a sequencia, timeout, retries e tratamento de falhas:

1. Inicia o Cloud Run Job de ingestao.
2. Aguarda a conclusao do job.
3. Interrompe o fluxo se a ingestao falhar.
4. Inicia a execucao Dataform do ambiente correspondente.
5. Aguarda Silver, Gold e assertions.
6. Registra o resultado final.

### Cloud Run Job

Executa o Python com `python -m`, sem console scripts ou arquivos `.exe`:

1. Le o change log PostgreSQL.
2. Escreve Parquet na Landing.
3. Carrega os eventos na tabela tecnica Bronze.
4. Aplica os eventos nas tabelas Bronze de origem.
5. Atualiza os checkpoints somente apos sucesso.

O full load e executado separadamente do job incremental, com controle de carga
logica, tentativas e manifest de arquivos.

### Dataform

Executa os modelos Silver, Gold e assertions somente depois que a ingestao
Bronze concluir com sucesso. A configuracao do ambiente direciona as acoes
para os datasets correspondentes.

## Ambientes

O mesmo codigo sera promovido por configuracao:

```text
dev  -> dev_control, dev_bronze, dev_silver, dev_gold, dev_assertions
hom  -> hom_control, hom_bronze, hom_silver, hom_gold, hom_assertions
prod -> prod_control, prod_bronze, prod_silver, prod_gold, prod_assertions
```

O ambiente deve ser definido de forma explicita no Cloud Run Job e na release
configuration do Dataform. Uma execucao nunca deve depender do ambiente default
do container ou do workspace local.

## Seguranca

- A URL do PostgreSQL fica no Secret Manager.
- Cada Cloud Run Job usa uma conta de servico propria por ambiente.
- A conta do Dataform possui apenas as permissoes necessarias no ambiente alvo.
- Buckets e datasets possuem permissoes separadas por ambiente e funcao.
- Nenhum segredo fica no GitHub ou no codigo-fonte.
- Cloud Logging registra execucoes, falhas e identificadores de correlacao.

## Estrategia de falhas

- Falha na ingestao: Dataform nao e executado.
- Falha no Dataform: o checkpoint da ingestao nao e retrocedido; o proximo ciclo
  deve reprocessar de forma idempotente conforme o estado confirmado.
- Retry do Workflow: cargas e merges devem permanecer idempotentes.
- Falha apos escrita parcial: arquivos publicados permanecem no GCS e o estado
  da execucao deve indicar quais foram validados.
- Falha de assertion: a execucao deve ser marcada como falha de qualidade, sem
  promover o resultado para o proximo ambiente.

## Observabilidade minima

Cada execucao deve permitir localizar:

- ambiente;
- nome da etapa;
- `logical_load_id` ou `run_id`;
- checkpoint inicial e final;
- quantidade de eventos e linhas;
- status;
- mensagem de erro;
- links para logs e jobs do BigQuery/Dataform.

## Ordem de implementacao

1. Validar o fluxo local equivalente aos Cloud Run Jobs.
2. Manter Cloud Run Jobs e Secret Manager operacionais no ambiente dev.
3. Finalizar a invocacao do Dataform pelo Workflow.
4. Criar o agendamento horario no Cloud Scheduler.
5. Automatizar build e deploy com GitHub Actions.
6. Promover e validar o ambiente hom.
7. Promover para prod somente apos revisar seguranca, retencao, custos e
   monitoramento.
