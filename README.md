# AirData - ETL

Módulo do AirData responsável por fazer a Extração, Transformação e Carregamento de Dados.

## Infraestrutura do Projeto

O arquivo `docker-compose.yaml` define a arquitura do container a ser construído em **Docker**.
O container é criado com os seguintes componentes:

- Apache Airflow (todos os componentes)
- Postgres

### Airflow
O **Apache Airflow** é criado em sua versão **3.0.0** e sua configuração é definida em `config/airflow.cfg`.
O armazenamento de metadados do Airflow está sendo feito no **Postgres**, para otimizar o tempo de execução e permitir **DAG Runs** simultâneas.

### Postgres
O **Postgres** é criado em sua versão **13**. 
Automaticamente (via `docker-compose.yaml`) já são criados os Data Bases e Schemas necessários para executar o Airflow e para armazenamento de dados do AirData.
O armazenamento de metadados do Airflow acontece no schema `public`, enquanto os dados do Airdata são salvos no schema `airdata`. O nome do DataBase criado no Postgres é `airflow`.

## Arquitetura da Implementação

### Sistema de Arquivos
O padrão desenvolvido para o sistema de arquivos segue a recomendação da documentação do Airflow, descrito a seguir:

```bash
.
├── config/
│   ├── airflow.cfg
│   └── db.cfg
├── dags/
│   └── VRA/
│       └── vra_extraction.py
├── docker-compose.yaml
├── logs/
├── plugins/
├── README.md
└── requirements.txt
```

Na pasta `config` ficam todos os arquivos de configuração do Módulo ETL. O arquivo `airflow.cfg` trata das configurações utilizadas pelo serviço do Airflow, enquanto `db.cfg` carrega as configs utilizadas em código para conexão com Banco de Dados.

Na pasta `dags` são mantidos os códigos que definem os Directed Acyclic Graph (DAG) que representam os pipelines de dados orquestrados pelo Airflow.
O Padrão para implementação das DAGs é criar uma subpasta para cada base de dados trabalhada. Está ilustrada na árvore do repositório a subpasta `VRA`.

Os logs são armazenados automaticamente na pasta `logs`, enquanto os arquivos de plugins são salvos na pasta `plugins`.

## Deploy

Para reproduzir em ambiente local é necessário ter docker instalado na máquina.

O primeiro passo para criação do ambiente de execução é criar um ambiente virtual com Python instalado.

```bash
uv venv --python 3.11
```

Em seguida, ative o ambiente virtual:
```bash
source .venv/bin/activate
```

Para instalar as dependências:
```bash
uv pip install -r requirements.txt
```

Daí, basta executar o docker-compose (para que o container seja iniciado) da seguinte forma:
```bash
docker compose up
```

Com o ambiente em execução é necessário criar uma conexão com o banco de dados com ID `postgres`.

## Codificação das DAGs

A princípio haverão 3 tipos de DAGs: 
- DAG de Extração: Que tem como objetivo coletar dados de fontes externas e armazenar em ambiente interno (Postgres)
- DAG de Processamento: Que tem como objetivo gerar outras bases de dados mantidas em ambiente interno (Postgres)
- DAG de Parsing (para RDF): Que tem como objetivo preparar os dados para serem inseridos em um RDF storage (Jena)

O padrão de construção de `DAG de Extração` foi definido em 3 passos:
1. Criar a tabela no banco caso não exista
2. Recuperar a última data de atualização (Definir data inicial)
3. Coletar e Armazenar dados da fonte externa a partir da data recuperada no passo 2
