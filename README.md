# API de Análise de Dívida Ativa Municipal

![Linguagem](https://img.shields.io/badge/Python-3.11-blue.svg)
![Framework](https://img.shields.io/badge/Framework-FastAPI-009688.svg)
![Banco de Dados](https://img.shields.io/badge/Banco_de_Dados-PostgreSQL-336791.svg)
![Infraestrutura](https://img.shields.io/badge/Infra-Docker-2496ED.svg)

## Sobre o Projeto

Este projeto consiste em um backend completo para análise de dados da Dívida Ativa de um município. A solução é composta por três componentes principais que funcionam de forma integrada através do Docker:

1.  **ETL (Extract, Transform, Load)**: Um processo que extrai dados de arquivos CSV, os transforma e os carrega em um banco de dados PostgreSQL, estruturado como um Data Warehouse para otimizar as consultas analíticas.
2.  **Banco de Dados**: Uma instância do PostgreSQL que armazena os dados em um esquema dimensional (Star Schema), com tabelas de fatos e dimensões.
3.  **API RESTful**: Uma API de alta performance construída com FastAPI que serve os dados processados, oferecendo endpoints para consultas, buscas filtradas e resumos analíticos.

O projeto foi desenvolvido para ser facilmente executado em qualquer ambiente que possua Docker, garantindo reprodutibilidade e simplicidade na configuração.

## Funcionalidades

-   **Ambiente Containerizado**: Todo o projeto (banco de dados, ETL, API) é orquestrado com `docker-compose`.
-   **Carga de Dados Automatizada**: O serviço de ETL é executado automaticamente na inicialização para popular o banco de dados.
-   **Endpoints de Resumo**: Rotas que fornecem dados agregados sobre a Dívida Ativa, como quantidade e saldo total por tipo de tributo.
-   **Busca Avançada**: Um endpoint de pesquisa poderoso que permite filtrar CDAs (Certidões de Dívida Ativa) por múltiplos critérios como valor, ano, natureza e com ordenação customizável.
-   **Consulta de Detalhes**: Possibilidade de obter informações específicas de um devedor a partir do número da CDA.
-   **Documentação Interativa**: A API conta com documentação automática (Swagger UI e ReDoc) gerada pelo FastAPI.

## Tecnologias Utilizadas

-   **Backend**: Python 3.11, FastAPI
-   **Banco de Dados**: PostgreSQL 13
-   **Comunicação com BD**: SQLAlchemy (Core), Asyncpg
-   **Processamento de Dados (ETL)**: Pandas
-   **Containerização**: Docker, Docker Compose
-   **Validação de Dados**: Pydantic
-   **Servidor ASGI**: Uvicorn

## Como Executar o Projeto

Com o Docker e o Docker Compose instalados, você pode executar todo o ambiente com um único comando.

**1. Clone este repositório:**
```bash
git clone <url-do-seu-repositorio>
cd DT-BACK
```

**2. Execute o Docker Compose:**
```bash
docker-compose up --build
```

**O que este comando faz?**
-   `--build`: Constrói as imagens do Docker para os serviços de ETL e API conforme definido nos arquivos `Dockerfile.etl` e `Dockerfile.api`.
-   Inicia um container com o banco de dados PostgreSQL.
-   Executa o script `sql/01_schema_creation.sql` para criar todas as tabelas no banco de dados.
-   Inicia o container do ETL, que lê os arquivos da pasta `/data`, processa-os e os insere no banco de dados. Este serviço termina após a conclusão da carga.
-   Após o ETL ser finalizado com sucesso, o container da API é iniciado e passa a servir os dados na porta `8000`.

### Acessando a API

-   **URL Base da API**: `http://localhost:8000`
-   **Documentação Interativa (Swagger UI)**: `http://localhost:8000/docs`
-   **Health Check**: `http://localhost:8000/health`

## Endpoints da API

Para testar qualquer um destes endpoints de forma interativa, acesse a documentação do Swagger UI em `http://localhost:8000/docs`.

#### Endpoints de Resumo e Análise

* `GET /resumo/quantidade_cdas`
    * **O que faz:** Retorna a quantidade total de CDAs (Certidões de Dívida Ativa) para cada tipo de tributo.

* `GET /resumo/saldo_cdas`
    * **O que faz:** Retorna o saldo devedor total, agrupado por tipo de tributo.

* `GET /resumo/inscricoes`
    * **O que faz:** Mostra a quantidade de novas inscrições de dívidas por ano, em ordem cronológica.

* `GET /resumo/distribuicao_cdas`
    * **O que faz:** Apresenta a distribuição percentual das CDAs por situação (ex: Em cobrança, Paga, Cancelada) para cada natureza de tributo.

* `GET /resumo/montante_acumulado`
    * **O que faz:** Exibe o percentual acumulado do valor da dívida em faixas de concentração (percentis), permitindo identificar onde se concentram os maiores valores.

#### Endpoints de Busca e Detalhes

* `GET /cda/search`
    * **O que faz:** Realiza uma busca avançada e paginada por CDAs com base em múltiplos filtros.
    * **Parâmetros de busca:**
        * `numCDA`: Busca pelo número exato da CDA.
        * `minSaldo` / `maxSaldo`: Filtra por uma faixa de valor de saldo.
        * `minAno` / `maxAno`: Filtra pelo ano de inscrição da dívida.
        * `natureza`: Busca por parte do nome da natureza do tributo (ex: "IPTU", "TAXA").
        * `sort_by`: Define o campo para ordenação. Opções: 'ano' ou 'valor'.
        * `sort_order`: Define a ordem. Opções: 'asc' ou 'desc'.
        * `limit` e `offset`: Para controlar a paginação dos resultados.

* `GET /cda/{num_cda}/devedor`
    * **O que faz:** Retorna os detalhes do devedor (nome, tipo de pessoa, documento) associado a um número de CDA específico.
