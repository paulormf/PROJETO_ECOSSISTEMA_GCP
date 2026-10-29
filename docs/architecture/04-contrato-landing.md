# Contrato da camada Landing

## Objetivo

A Landing recebe a fotografia inicial das tabelas e os eventos do
`audit.change_log` do PostgreSQL de forma bruta, imutavel e auditavel. Os
arquivos serao usados como entrada para a Bronze no BigQuery.

## Buckets por ambiente

| Ambiente | Bucket |
|---|---|
| dev | `gs://botbrewpub-ghsurp-dev-landing` |
| hom | `gs://botbrewpub-ghsurp-hom-landing` |
| prod | `gs://botbrewpub-ghsurp-prod-landing` |

Todos os buckets ficam em `us-central1`.

## Tipos de carga

### Full inicial

O full carrega todos os registros atuais de cada tabela de origem. Ele e
necessario porque o change log nao possui o historico anterior a criacao dos
triggers.

Antes de iniciar o full, o processo deve capturar o maior `change_id` disponivel
no PostgreSQL. Esse valor sera armazenado como `initial_change_id` do controle
logico da carga.

O full sera considerado concluido somente quando:

1. todos os registros da tabela forem extraidos;
2. todos os arquivos forem registrados no manifest;
3. todos os arquivos forem carregados na Bronze;
4. a quantidade de linhas for reconciliada;
5. os eventos posteriores ao `initial_change_id` forem reaplicados.

### Incremental

O incremental carrega eventos `I`, `U` e `D` do change log em ordem crescente
de `change_id`. Cada evento recebe `load_type = "incremental"`.

Depois do full, devem ser processados todos os eventos com:

```text
change_id > initial_change_id
```

Essa regra captura alteracoes ocorridas enquanto a fotografia inicial estava
sendo extraida.

## Formato dos arquivos

- Formato: Apache Parquet (`.parquet`).
- Compressao: Snappy.
- Schema explicito no full e estavel no incremental.
- Arquivos imutaveis depois da publicacao.
- Um arquivo representa um lote de dados de uma execucao.
- Arquivos publicados nunca sao sobrescritos.

## Caminho dos objetos

```text
source=postgresql/
schema=ecommerce/
table=<tabela_origem>/
load_type=<full|incremental>/
ingestion_date=YYYY-MM-DD/
logical_load_id=<uuid>/
attempt_id=<uuid>/
part-00000.parquet
```

O `logical_load_id` identifica uma carga logica. O `attempt_id` identifica uma
tentativa de execucao. Uma nova tentativa pode continuar a carga sem apagar
os arquivos validos produzidos anteriormente.

## Controle do full load

O full sera controlado pelas tabelas BigQuery de controle:

- `full_load_runs`: uma linha por carga logica e tabela;
- `full_load_files`: manifest dos arquivos produzidos e carregados.

O controle deve registrar, no minimo:

- tabela de origem;
- `logical_load_id`;
- `attempt_id`;
- `initial_change_id`;
- snapshot da origem;
- status da carga;
- linhas e arquivos esperados;
- linhas e arquivos extraidos;
- ultima chave primaria processada;
- mensagem de erro;
- timestamps de inicio e conclusao.

Em caso de falha, a retomada deve reutilizar o mesmo `logical_load_id`, criar
um novo `attempt_id` e carregar na Bronze a uniao dos arquivos registrados no
manifest. A Bronze nao deve considerar apenas os arquivos da ultima tentativa.

## Envelope incremental

Cada evento incremental deve conter:

```json
{
  "source_system": "postgresql",
  "load_type": "incremental",
  "change_id": 12345,
  "occurred_at": "2026-10-28T12:00:00Z",
  "operation": "I",
  "source_schema": "ecommerce",
  "source_table": "pedidos",
  "primary_key": {"pedido_id": 1001},
  "old_row": null,
  "new_row": {"pedido_id": 1001},
  "request_id": null,
  "logical_load_id": "00000000-0000-0000-0000-000000000000",
  "attempt_id": "00000000-0000-0000-0000-000000000000",
  "ingested_at": "2026-10-28T12:05:00Z"
}
```

`old_row` e `new_row` serao armazenados como JSON no Parquet incremental para
preservar o contrato variavel das tabelas.

## Dados do full

Os arquivos do full terao as colunas da tabela de origem e metadados tecnicos,
incluindo:

- `source_system`;
- `load_type = "full"`;
- `snapshot_at`;
- `source_schema`;
- `source_table`;
- `logical_load_id`;
- `attempt_id`;
- `ingested_at`.

## Regras de processamento

1. O full deve ser concluido e reconciliado antes da carga incremental oficial.
2. `change_id` e a chave de ordenacao e o watermark global da origem.
3. A combinacao `source_system + change_id` identifica unicamente um evento.
4. Full e incremental devem ser idempotentes na Bronze.
5. O schema Parquet deve ser validado antes do upload.
6. O checkpoint so avanca depois que todos os arquivos e aplicacoes do lote
   forem concluidos.
7. Falhas nao devem apagar arquivos publicados.
8. Arquivos orfaos de tentativas falhas devem ser identificaveis pelo controle
   e nao podem ser carregados fora do manifest validado.

## Estrategia para tabelas volumosas

O full nao deve carregar uma tabela inteira em memoria:

1. Ler dados em streaming usando cursor do PostgreSQL.
2. Usar paginacao por chave primaria com `ORDER BY` deterministico.
3. Nao usar `OFFSET`.
4. Serializar lotes progressivamente em Parquet Snappy.
5. Limitar arquivo por quantidade de registros e tamanho aproximado.
6. Persistir a ultima chave processada no controle do full.
7. Limitar a concorrencia conforme a capacidade do PostgreSQL, rede e GCS.

Para `estoque`, a paginacao deve respeitar a chave composta
`(armazem_id, variante_id)`.

## Dados sensiveis

Landing e Bronze podem conter dados brutos da origem e terao acesso restrito.
Mascaramento e reducao de dados para consumo analitico ocorrerão na Silver.

## Retencao

A retencao inicial da Landing sera de 5 dias. Essa politica sera revisada antes
da entrada em producao conforme requisitos de auditoria, LGPD, recuperacao e
custo.
