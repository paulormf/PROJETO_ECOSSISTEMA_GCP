# Contrato da camada Landing

## Objetivo

A camada Landing recebe tanto a fotografia inicial das tabelas quanto os eventos
do `audit.change_log` do PostgreSQL de forma bruta, imutável e auditável. Os
arquivos serão usados como entrada para a carga da camada Bronze no BigQuery.

## Buckets por ambiente

| Ambiente | Bucket |
|---|---|
| dev | `gs://botbrewpub-ghsurp-dev-landing` |
| hom | `gs://botbrewpub-ghsurp-hom-landing` |
| prod | `gs://botbrewpub-ghsurp-prod-landing` |

Todos os buckets ficam em `us-central1`.

## Tipos de carga

### Full inicial

O full carrega todos os registros atuais de cada tabela de origem. Ele é
necessário porque o `audit.change_log` não contém automaticamente o histórico
anterior à criação dos gatilhos.

Cada registro do full representa uma linha da tabela de origem e recebe
`load_type = "full"`.

### Incremental

O incremental carrega os eventos `I`, `U` e `D` do `audit.change_log`, sempre em
ordem crescente de `change_id`. Cada evento recebe `load_type = "incremental"`.

Após o full, os eventos incrementais devem ser reaplicados de forma idempotente
para capturar alterações ocorridas durante a carga inicial.

## Formato dos arquivos

- Formato principal: Apache Parquet (`.parquet`).
- Compressão: Snappy.
- Schema: explícito por tabela no full e estável por contrato no incremental.
- Um arquivo representa um lote de eventos lido em uma execução.
- Arquivos já gravados não serão sobrescritos.

## Caminho dos objetos

```text
source=postgresql/
schema=ecommerce/
table=<tabela_origem>/
load_type=<full|incremental>/
ingestion_date=YYYY-MM-DD/
run_id=<uuid>/
part-00000.parquet
```

O caminho contém a tabela de origem para facilitar auditoria e processamento
seletivo. O `run_id` torna cada execução identificável e impede colisões entre
execuções.

## Envelope de cada evento

Cada registro incremental terá a seguinte estrutura lógica:

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
  "run_id": "00000000-0000-0000-0000-000000000000",
  "ingested_at": "2026-10-28T12:05:00Z"
}
```

No arquivo Parquet incremental, `old_row` e `new_row` serão armazenados como
colunas JSON, preservando a estrutura variável das tabelas sem forçar um único
schema para todas as entidades.

Para registros do full, a estrutura usará `change_id = null`. As colunas da
tabela de origem serão gravadas com tipos próprios no Parquet, acompanhadas dos
metadados de carga:

```json
{
  "source_system": "postgresql",
  "load_type": "full",
  "change_id": null,
  "snapshot_at": "2026-10-28T12:00:00Z",
  "source_schema": "ecommerce",
  "source_table": "pedidos",
  "primary_key": {"pedido_id": 1001},
  "pedido_id": 1001,
  "run_id": "00000000-0000-0000-0000-000000000000",
  "ingested_at": "2026-10-28T12:05:00Z"
}
```

## Regras de processamento

1. O full deve ser concluído antes de marcar a pipeline como pronta para uso
   incremental.
2. `change_id` é a chave de ordenação e o watermark global da origem.
3. A combinação `source_system + change_id` identifica unicamente um evento
   incremental.
4. O full e o incremental devem ser carregados na Bronze com operações
   idempotentes.
5. O schema Parquet deve ser validado antes do upload e comparado com o contrato
   conhecido da tabela.
6. Após o full, os eventos do change log devem ser reaplicados em ordem para
   capturar alterações ocorridas durante a fotografia inicial.
7. O checkpoint só será atualizado depois que todos os arquivos do lote forem
   gravados com sucesso.
8. Em caso de falha, a execução poderá ser repetida sem sobrescrever arquivos.
9. A camada Landing é bruta; regras de qualidade, deduplicação e tratamento de
   dados sensíveis serão aplicadas nas camadas seguintes.

## Estratégia para tabelas volumosas

O full não poderá carregar uma tabela inteira em memória. A extração deverá
seguir estas regras:

1. Ler os dados em streaming usando cursor do PostgreSQL e lotes configuráveis.
2. Usar paginação por chave primária, com `ORDER BY` determinístico e filtro
   baseado na última chave processada.
3. Não usar `OFFSET`, pois o custo aumenta conforme a posição da página e pode
   causar inconsistências durante alterações concorrentes.
4. Serializar e comprimir os registros progressivamente em arquivos Parquet com
   compressão Snappy.
5. Limitar cada arquivo por quantidade de registros e tamanho aproximado,
   evitando objetos muito grandes.
6. Gravar arquivos com `run_id` único e nunca sobrescrever um arquivo concluído.
7. Persistir o progresso do full por tabela e por última chave processada para
   permitir retomada após falha.
8. Processar tabelas em paralelo somente com limite explícito de concorrência,
   respeitando a capacidade do PostgreSQL, da rede e do Cloud Storage.

### Chaves de paginação conhecidas

| Tabela | Estratégia |
|---|---|
| `eventos_clientes` | `evento_id` crescente |
| `pedidos` | `pedido_id` |
| `itens_pedido` | `item_pedido_id` |
| `pagamentos` | `pagamento_id` |
| `envios` | `envio_id` |
| `clientes` | `cliente_id` |
| `estoque` | chave composta `(armazem_id, variante_id)` |
| Demais tabelas | chave primária identificada no inventário |

Para a tabela `estoque`, a paginação deverá usar a ordenação lexicográfica da
chave composta, nunca apenas uma das colunas.

### Consistência do full

Cada tabela deverá ser lida em uma transação somente leitura com snapshot
consistente. O full será considerado concluído somente quando todas as tabelas
forem extraídas e os arquivos forem disponibilizados na Landing.

Como alterações podem ocorrer durante o full, os eventos do change log serão
reprocessados após a carga inicial. Essa reconciliação garante que a Bronze
termine refletindo o estado mais recente da origem.

O checkpoint incremental existente não será usado para controlar a paginação do
full. O full precisará de controle próprio por tabela, com a última chave
extraída, quantidade de linhas, `run_id`, status e mensagem de erro.

## Dados sensíveis

Os eventos podem conter dados pessoais em `old_row`, `new_row` e,
eventualmente, em campos JSONB. O acesso aos buckets será privado e restrito à
identidade da pipeline e aos operadores autorizados. A aplicação de mascaramento
para consumo analítico ocorrerá na Silver. A Bronze receberá os dados sem
mascaramento, como cópia bruta controlada da origem; por isso, o acesso à
Landing e à Bronze deverá ser privado e restrito.

## Retenção

A retenção definitiva da Landing ainda será definida após validarmos os
requisitos de auditoria, LGPD e custo.
