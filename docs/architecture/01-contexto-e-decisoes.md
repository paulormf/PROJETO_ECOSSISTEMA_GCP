# Contexto e decisoes da plataforma

## Objetivo

Construir uma plataforma de engenharia de dados para producao no Google Cloud,
com origem PostgreSQL, arquitetura medalhao e ambientes separados logicamente
para desenvolvimento, homologacao e producao.

## Escopo atual

- Origem: PostgreSQL 16, banco `appdb`, schema `ecommerce`.
- Tabelas monitoradas: 13.
- Frequencia operacional desejada: horaria.
- Regiao GCP: `us-central1`.
- Projeto GCP: `botbrewpub-ghsurp`.
- Repositorio de codigo: GitHub.
- Infraestrutura como codigo: Terraform, em etapa posterior.
- CI/CD: GitHub Actions, em etapa posterior.
- Datastream: fora do escopo desta fase.
- Requisito de privacidade: dados sujeitos a LGPD.

## Arquitetura adotada

```text
PostgreSQL
    |
    | Change log PostgreSQL lido pelo Python
    v
Python em Cloud Run Jobs
    |
    | Parquet imutavel
    v
Cloud Storage - Landing
    |
    | Carga controlada e idempotente
    v
BigQuery - Bronze
    |
    | Transformacoes e qualidade via Dataform
    v
BigQuery - Silver
    |
    | Modelo analitico via Dataform
    v
BigQuery - Gold
```

O Python sera responsavel por ler o PostgreSQL, extrair o full e os eventos
incrementais, escrever Parquet na Landing e carregar/aplicar os eventos na
Bronze. O Dataform sera responsavel pelas camadas Silver e Gold, assertions e
dependencias entre os modelos.

## Incrementalidade

O mecanismo incremental oficial desta fase e a tabela PostgreSQL
`audit.change_log`, alimentada por triggers nas tabelas monitoradas.

O `change_id` e o watermark global crescente. O fluxo deve:

1. Ler eventos com `change_id` maior que o checkpoint confirmado.
2. Persistir os eventos na Landing.
3. Carregar os eventos na tabela tecnica Bronze.
4. Aplicar `INSERT`, `UPDATE` e `DELETE` nas tabelas Bronze de origem.
5. Atualizar o checkpoint somente depois de todas as etapas concluirem.

Watermarks por tabela podem existir como mecanismo auxiliar, mas nao substituem
o change log para detectar alteracoes posteriores a insercao.

## Camadas

- Landing: arquivos Parquet brutos, imutaveis e auditaveis.
- Bronze: copia operacional da origem e eventos tecnicos de ingestao.
- Silver: limpeza, normalizacao, deduplicacao, regras de qualidade e tratamento
  de dados sensiveis.
- Gold: modelo analitico consumivel, com dimensoes e uma unica fato inicial:
  `fato_itens_pedido`.

## Ambientes

Os ambientes serao separados por datasets dentro do mesmo projeto GCP:

```text
dev  -> dev_control, dev_bronze, dev_silver, dev_gold, dev_assertions
hom  -> hom_control, hom_bronze, hom_silver, hom_gold, hom_assertions
prod -> prod_control, prod_bronze, prod_silver, prod_gold, prod_assertions
```

Os buckets de Landing tambem serao separados por ambiente:

```text
gs://botbrewpub-ghsurp-dev-landing
gs://botbrewpub-ghsurp-hom-landing
gs://botbrewpub-ghsurp-prod-landing
```

O mesmo codigo sera promovido entre ambientes por configuracao, sem copiar
manualmente modelos SQL entre pastas.

## Orquestracao alvo

```text
Cloud Scheduler
    -> Workflows
        -> Cloud Run Job - ingestao Python
            -> Bronze
        -> Dataform - Silver, Gold e assertions
```

GitHub Actions sera utilizado para testes, build da imagem, push para o
Artifact Registry e atualizacao dos jobs/recursos conforme a infraestrutura
for automatizada.

## Seguranca e LGPD

- Segredos ficam no Secret Manager, nunca no Git.
- Cada ambiente possui contas de servico e permissoes proprias.
- A Landing e a Bronze preservam dados brutos, com acesso restrito.
- Dados pessoais nao serao publicados na Gold por padrao.
- O tratamento inicial de dados sensiveis ocorre na Silver.
- A Gold utilizara chaves substitutas e atributos analiticos nao identificaveis.
- Campos mascarados ou hash somente serao publicados com requisito de negocio
  aprovado e controle de acesso especifico.
- Policy tags e data masking do BigQuery complementarao o controle por coluna
  quando houver necessidade de acesso autorizado ou mascarado.
- A retencao inicial da Landing sera de 5 dias.
- A retencao inicial do change log sera de 5 dias.

## Decisoes ainda pendentes

| Tema | Estado | Proxima decisao |
|---|---|---|
| Retencao da Landing | Definida inicialmente | 5 dias, revisar antes da producao |
| Retencao do change log | Definida inicialmente | 5 dias, revisar antes da producao |
| Catalogo LGPD | Parcial | Confirmar proprietario e classificacao das colunas |
| SCD Tipo 2 | Posterior | Implementar somente quando houver requisito historico |
| Orquestracao automatizada | Em implementacao | Finalizar Workflows, Scheduler e Dataform |
| Terraform e GitHub Actions | Posterior | Automatizar infraestrutura e deploy |

## Criterios para producao

Antes da entrada em producao, devemos validar:

1. Full load reconciliado por tabela e por quantidade de arquivos.
2. Incremental idempotente e com checkpoint recuperavel.
3. Falhas e retries sem duplicacao de dados.
4. Silver e Gold com assertions executadas.
5. Permissoes separadas por ambiente e funcao.
6. Retencao, monitoramento, alertas e custos definidos.
