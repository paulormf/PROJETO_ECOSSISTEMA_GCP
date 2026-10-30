# Auditoria e change log da origem

## Objetivo

Registrar as alteracoes das tabelas do schema `ecommerce` em uma tabela central
de auditoria, permitindo que o extrator Python leia as mudancas por um
identificador crescente.

## Decisao

A tabela `audit.change_log` fica no PostgreSQL de origem. A funcao
`audit.capture_change()` e associada as 13 tabelas monitoradas e registra cada
`INSERT`, `UPDATE` e `DELETE`.

Os `INSERTs` tambem sao registrados para manter um unico fluxo incremental e
um unico watermark global para o extrator Python.

Embora o contrato atual informe que nao havera exclusoes fisicas, o evento
`DELETE` permanece habilitado como protecao operacional. Um delete inesperado
deve gerar alerta e ser processado de forma idempotente na Bronze.

## Contrato do evento

| Campo | Funcao |
|---|---|
| `change_id` | Watermark global crescente usado pelo extrator |
| `occurred_at` | Instante da alteracao no banco |
| `operation` | Tipo da operacao: `I`, `U` ou `D` |
| `source_schema` | Schema da tabela alterada |
| `source_table` | Tabela alterada |
| `primary_key` | Chave do registro em formato JSONB |
| `old_row` | Estado anterior; nulo para insert |
| `new_row` | Estado posterior; nulo para delete |
| `source_user` | Usuario PostgreSQL que executou a alteracao |
| `transaction_id` | Identificador da transacao de origem |
| `request_id` | Correlacao opcional com a aplicacao |
| `processed_at` | Metadado operacional opcional; nao substitui o checkpoint GCP |

Para `INSERT`, `old_row` sera nulo e `new_row` contera a linha completa. Para
`UPDATE`, ambos conteraao o estado anterior e o posterior. Para `DELETE`,
`old_row` contera a linha removida e `new_row` sera nulo.

## Regras de processamento

1. O extrator le eventos com `change_id > ultimo_change_id` confirmado.
2. Os eventos sao processados em ordem crescente de `change_id`.
3. O lote e escrito na Landing antes de avancar o checkpoint.
4. O lote e carregado na tabela tecnica de eventos Bronze.
5. As alteracoes sao aplicadas nas tabelas Bronze por chave natural.
6. O checkpoint so e atualizado apos todas as etapas concluidas.
7. O processamento deve ser idempotente para permitir retries e reprocessamento.
8. Eventos `DELETE` inesperados devem gerar alerta operacional.

## Retencao

A retencao inicial do `audit.change_log` sera de 5 dias, conforme decidido
para esta fase. A limpeza sera executada manualmente, fora do horario de
ingestao, somente depois de confirmar que os checkpoints de `dev`, `hom` e
`prod` estao atualizados.

Consulta para revisar os eventos elegiveis:

```sql
SELECT COUNT(*) AS registros_expirados
FROM audit.change_log
WHERE occurred_at < clock_timestamp() - INTERVAL '5 days';
```

Com os checkpoints validados, a limpeza pode ser executada manualmente:

```sql
DELETE FROM audit.change_log
WHERE occurred_at < clock_timestamp() - INTERVAL '5 days';
```

Essa janela deve ser revisada antes da producao considerando o maior periodo
esperado de indisponibilidade do pipeline.

## Integridade e desempenho

- `change_id` e a chave primaria e deve permanecer monotonicamente crescente.
- A consulta incremental deve filtrar por `change_id` e limitar o tamanho do
  lote.
- Os indices de origem devem ser medidos e revisados conforme o volume.
- A limpeza de eventos antigos deve ocorrer fora do horario de ingestao e ser
  monitorada.
- Novas tabelas somente entram no fluxo depois de possuir trigger e contrato
  de chave primaria validado.

## Evolucao futura

O change log podera alimentar dimensoes SCD Tipo 2 quando existir requisito de
historico. Nesse caso, `occurred_at`, `change_id`, `old_row` e `new_row` serao
utilizados para reconstruir as versoes das dimensoes.
