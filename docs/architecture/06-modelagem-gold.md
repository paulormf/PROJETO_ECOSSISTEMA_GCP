# Contrato de modelagem da camada Gold

## Objetivo

Definir o modelo analitico consumivel da plataforma antes da criacao dos
modelos Dataform da camada Gold.

Os modelos Gold serao derivados exclusivamente da Silver. A Gold nao deve
consultar PostgreSQL, Landing ou Bronze diretamente.

## Principios

- Cada fato possui uma granularidade explicita.
- Cada dimensao possui uma chave substituta deterministica (`*_sk`).
- As chaves naturais continuam preservadas para rastreabilidade controlada.
- A Gold nao publica dados pessoais por padrao.
- Fatos transacionais serao particionados por uma data de negocio.
- Fatos de grande volume receberao clustering alinhado aos filtros analiticos.
- A primeira versao usara dimensoes em estado atual, SCD Tipo 1.
- SCD Tipo 2 sera implementado somente quando houver requisito para consultar
  o estado historico de uma dimensao.

## Dimensoes do primeiro escopo

| Modelo | Granularidade | Chave natural | Chave Gold | Estrategia |
|---|---|---|---|---|
| `dim_clientes` | Um registro atual por cliente | `cliente_id` | `cliente_sk` | SCD Tipo 1 |
| `dim_produtos` | Um registro atual por produto | `produto_id` | `produto_sk` | SCD Tipo 1 |
| `dim_variantes_produtos` | Uma variante atual por produto | `variante_id` | `variante_sk` | SCD Tipo 1 |
| `dim_categorias` | Uma categoria atual | `categoria_id` | `categoria_sk` | SCD Tipo 1 |
| `dim_data` | Um dia do calendario | `data` | `data_sk` | Estatica/controlada |

`dim_marcas` e `dim_armazens` ficam fora do primeiro escopo da Gold, pois nao
sao necessarias para a granularidade inicial da fato. Poderao ser adicionadas
posteriormente sem criar uma nova fato.

## Regras das dimensoes

- Nenhuma dimensao Gold tera nome, e-mail, telefone, CPF, CNPJ, endereco ou
  outro dado pessoal bruto.
- A Gold utilizara chaves substitutas e atributos analiticos nao identificaveis.
- Campos mascarados ou hash somente serao publicados com requisito de negocio
  aprovado e controle de acesso especifico.
- `dim_data` sera gerada no Dataform entre a menor e a maior data valida de
  `silver.pedidos`. Caso a origem esteja vazia, sera usado `1900-01-01` como
  inicio e fim do calendario.
- Registros sem correspondencia receberao uma chave desconhecida reservada,
  por exemplo `data_sk = 0`.

## Fato

| Modelo | Granularidade | Particao | Clustering inicial |
|---|---|---|---|
| `fato_itens_pedido` | Um registro por item do pedido | `DATE(realizado_em)` | `pedido_id`, `variante_sk` |

## Colunas de `fato_itens_pedido`

A fato utilizara `item_pedido_id` como identificador da granularidade. Nao
sera criada uma chave substituta `item_pedido_sk`, pois existe uma unica fonte
e `item_pedido_id` ja identifica unicamente cada item.

### Chaves e relacionamentos

- `item_pedido_id`: chave natural do item na origem;
- `pedido_id`: identificador do pedido, usado como dimensao degenerada;
- `cliente_sk`: referencia para `dim_clientes`;
- `produto_sk`: referencia para `dim_produtos`;
- `variante_sk`: referencia para `dim_variantes_produtos`;
- `categoria_sk`: referencia para `dim_categorias`;
- `data_sk`: referencia para `dim_data`.

### Contexto do pedido

- `realizado_em`;
- `status_pedido`;
- `canal_venda`;
- `moeda`.

### Medidas

- `quantidade`;
- `preco_unitario`;
- `desconto_total`;
- `total_item`.

### Auditoria tecnica

- `source_ingested_at`;
- `source_run_id`.

Medidas do cabecalho do pedido, como `subtotal`, `frete_total` e
`total_pedido`, nao serao copiadas para a fato de itens, pois sua repeticao em
cada item poderia causar dupla contagem em agregacoes.

## Chaves substitutas

As chaves substitutas serao deterministicas e nao dependerao da ordem de
execucao do Dataform. A primeira implementacao usara uma representacao estavel
derivada da chave natural e do dominio:

```sql
TO_HEX(SHA256(CONCAT('cliente|', CAST(cliente_id AS STRING)))) AS cliente_sk
```

As chaves naturais permanecerao nas dimensoes e fatos para auditoria e
reconciliacao com a origem.

## SCD Tipo 2 futuro

A primeira versao sera SCD Tipo 1. Quando houver requisito historico, a
dimensao passara a conter, no minimo:

- chave natural;
- chave substituta da versao;
- `valid_from`;
- `valid_to`;
- `is_current`;
- hash dos atributos controlados;
- identificador do evento do change log.

A fato devera relacionar-se com a versao valida da dimensao no momento do
pedido. Essa evolucao sera implementada separadamente, sem alterar a
granularidade da fato.

## Qualidade e assertions

Cada dimensao devera validar:

- unicidade da chave natural na versao SCD Tipo 1;
- nao nulidade da chave substituta;
- ausencia de duplicidade na granularidade definida.

A fato devera validar:

- unicidade de `item_pedido_id`;
- nao nulidade das chaves obrigatorias;
- integridade referencial logica com as dimensoes;
- ausencia de medidas monetarias negativas quando a regra de negocio nao
  permitir valores negativos;
- coerencia entre quantidade, preco e total do item quando aplicavel.

## Ordem de implementacao

1. Criar `dim_data`.
2. Criar `dim_clientes`, sem dados pessoais brutos.
3. Criar `dim_produtos`.
4. Criar `dim_variantes_produtos`.
5. Criar `dim_categorias`.
6. Criar `fato_itens_pedido` como tabela incremental.
7. Criar assertions Gold.
8. Executar e validar no ambiente `dev`.
9. Promover a mesma revisao para `hom` e, posteriormente, `prod`.
