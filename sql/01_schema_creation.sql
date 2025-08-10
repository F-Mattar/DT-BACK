-- Apaga as tabelas antigas na ordem correta, se elas existirem.
DROP TABLE IF EXISTS fact_divida_ativa;
DROP TABLE IF EXISTS dim_tempo;
DROP TABLE IF EXISTS dim_devedor;
DROP TABLE IF EXISTS dim_divida_natureza;
DROP TABLE IF EXISTS dim_cda_situacao;

-- Cria as tabelas de dimensão
CREATE TABLE dim_tempo (
    id_tempo_sk       INTEGER PRIMARY KEY,
    data_completa     DATE, 
    ano               INTEGER NOT NULL,
    trimestre         INTEGER NOT NULL,
    mes               INTEGER NOT NULL,
    dia_da_semana     VARCHAR(20) NOT NULL
);

CREATE TABLE dim_devedor (
    id_devedor_sk     SERIAL PRIMARY KEY,
    id_pessoa_nk      INTEGER UNIQUE,
    nome              VARCHAR(255),
    documento         VARCHAR(20),
    tipo_pessoa       VARCHAR(2)
);

CREATE TABLE dim_divida_natureza (
    id_natureza_sk    SERIAL PRIMARY KEY,
    id_natureza_nk    INTEGER UNIQUE,
    nome_natureza     VARCHAR(100)
);

CREATE TABLE dim_cda_situacao (
    id_situacao_sk    SERIAL PRIMARY KEY,
    cod_situacao_nk   INTEGER UNIQUE,
    nome_situacao     VARCHAR(100)
);

-- Tabela de Fatos
CREATE TABLE fact_divida_ativa (
    id_divida_ativa      BIGSERIAL PRIMARY KEY,
    num_cda_nk           BIGINT NOT NULL,
    id_devedor_sk        INTEGER REFERENCES dim_devedor(id_devedor_sk),
    id_natureza_sk       INTEGER REFERENCES dim_divida_natureza(id_natureza_sk),
    id_situacao_sk       INTEGER REFERENCES dim_cda_situacao(id_situacao_sk),
    id_tempo_cadastro_sk INTEGER REFERENCES dim_tempo(id_tempo_sk),
    ano_inscricao        INTEGER NOT NULL,
    valor_saldo          DECIMAL(18, 2),
    prob_recuperacao     FLOAT,
    cod_fase_cobranca    INTEGER,
    flag_saldo_negativo  BOOLEAN
);