import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text

# =============================================================================
# FASE 1: EXTRAÇÃO (EXTRACT)
# =============================================================================
print("Iniciando o processo de ETL...")
try:
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data')
    
    df_pf = pd.read_csv(os.path.join(data_path, '006.csv'))
    df_pj = pd.read_csv(os.path.join(data_path, '007.csv'))
    df_natureza_raw = pd.read_csv(os.path.join(data_path, '002.csv'))
    df_situacao_raw = pd.read_csv(os.path.join(data_path, '003.csv'))
    df_fatos_raw = pd.read_csv(os.path.join(data_path, '001.csv'))
    df_prob_raw = pd.read_csv(os.path.join(data_path, '004.csv'))
    df_devedor_cda_link = pd.read_csv(os.path.join(data_path, '005.csv'))

    print("FASE DE EXTRAÇÃO: Todos os arquivos foram lidos com sucesso.")

except FileNotFoundError as e:
    print(f"ERRO: Arquivo não encontrado. Verifique se a pasta 'data' existe e contém os 7 arquivos CSV. Detalhes: {e}")
    exit()


# =============================================================================
# FASE 2: TRANSFORMAÇÃO (TRANSFORM)
# =============================================================================
print("\nIniciando a FASE DE TRANSFORMAÇÃO...")

# == 2.1 Transformação da dim_devedor ==
print("Transformando dim_devedor...")
df_devedores_detalhes = pd.concat([df_pf, df_pj], ignore_index=True)
df_devedores_detalhes = df_devedores_detalhes.drop_duplicates(subset=['idpessoa'])
df_dim_devedor = df_devedor_cda_link[['idPessoa']].drop_duplicates().rename(columns={'idPessoa': 'id_pessoa_nk'})
df_dim_devedor = pd.merge(df_dim_devedor, df_devedores_detalhes, left_on='id_pessoa_nk', right_on='idpessoa', how='left')
df_dim_devedor['documento'] = df_dim_devedor['numcpf'].fillna(df_dim_devedor['numCNPJ'])
df_dim_devedor['tipo_pessoa'] = np.where(df_dim_devedor['numcpf'].notna(), 'PF', 'PJ')
df_dim_devedor = df_dim_devedor[['id_pessoa_nk', 'descNome', 'documento', 'tipo_pessoa']].rename(columns={'descNome': 'nome'})
df_dim_devedor['nome'].fillna('Não Identificado', inplace=True)
df_dim_devedor['documento'] = df_dim_devedor['documento'].astype('Int64').astype(str).replace('<NA>', np.nan)

# == 2.2 Transformação da dim_divida_natureza ==
print("Transformando dim_divida_natureza...")
df_dim_natureza = df_natureza_raw[['idNaturezadivida', 'nomnaturezadivida']].rename(columns={'idNaturezadivida': 'id_natureza_nk', 'nomnaturezadivida': 'nome_natureza'})
df_dim_natureza = df_dim_natureza.drop_duplicates(subset=['id_natureza_nk'])

# == 2.3 Transformação da dim_cda_situacao ==
print("Transformando dim_cda_situacao...")
df_dim_situacao = df_situacao_raw[['codSituacaoCDA', 'nomSituacaoCDA']].rename(columns={'codSituacaoCDA': 'cod_situacao_nk', 'nomSituacaoCDA': 'nome_situacao'})
df_dim_situacao = df_dim_situacao.drop_duplicates(subset=['cod_situacao_nk'])

# == 2.4 Geração da dim_tempo ==
print("Gerando dim_tempo...")
datas_situacao = pd.to_datetime(df_fatos_raw['DatSituacao'], errors='coerce')
datas_cadastro = pd.to_datetime(df_fatos_raw['datCadastramento'], errors='coerce')
min_data = min(datas_situacao.min(), datas_cadastro.min())
max_data = max(datas_situacao.max(), datas_cadastro.max())
date_range = pd.date_range(start=min_data, end=max_data, freq='D')
df_dim_tempo = pd.DataFrame(date_range, columns=['data_completa'])
df_dim_tempo['id_tempo_sk'] = df_dim_tempo['data_completa'].dt.strftime('%Y%m%d').astype(int)
df_dim_tempo['ano'] = df_dim_tempo['data_completa'].dt.year
df_dim_tempo['mes'] = df_dim_tempo['data_completa'].dt.month
df_dim_tempo['trimestre'] = df_dim_tempo['data_completa'].dt.quarter
df_dim_tempo['dia_da_semana'] = df_dim_tempo['data_completa'].dt.day_name()
registros_especiais = pd.DataFrame([{'id_tempo_sk': -1, 'data_completa': pd.NaT, 'ano': -1, 'mes': -1, 'trimestre': -1, 'dia_da_semana': 'Desconhecido'}])
df_dim_tempo = pd.concat([registros_especiais, df_dim_tempo], ignore_index=True)
df_dim_tempo = df_dim_tempo[['id_tempo_sk', 'data_completa', 'ano', 'trimestre', 'mes', 'dia_da_semana']]

# == 2.5 Transformação da fact_divida_ativa ==
print("Transformando a tabela de fatos (fact_divida_ativa)...")
df_dim_devedor.reset_index(drop=True, inplace=True)
df_dim_devedor['id_devedor_sk'] = df_dim_devedor.index + 1
df_dim_natureza.reset_index(drop=True, inplace=True)
df_dim_natureza['id_natureza_sk'] = df_dim_natureza.index + 1
df_dim_situacao.reset_index(drop=True, inplace=True)
df_dim_situacao['id_situacao_sk'] = df_dim_situacao.index + 1
df_fact_divida = pd.merge(df_fatos_raw, df_prob_raw, on='numCDA', how='left')
df_fact_divida = pd.merge(df_fact_divida, df_devedor_cda_link[['numCDA', 'idPessoa']], on='numCDA', how='left')
df_fact_divida = pd.merge(df_fact_divida, df_dim_devedor[['id_pessoa_nk', 'id_devedor_sk']], left_on='idPessoa', right_on='id_pessoa_nk', how='left')
df_fact_divida = pd.merge(df_fact_divida, df_dim_natureza[['id_natureza_nk', 'id_natureza_sk']], left_on='idNaturezaDivida', right_on='id_natureza_nk', how='left')
df_fact_divida = pd.merge(df_fact_divida, df_dim_situacao[['cod_situacao_nk', 'id_situacao_sk']], left_on='codSituacaoCDA', right_on='cod_situacao_nk', how='left')
df_fact_divida['id_tempo_cadastro_sk'] = pd.to_datetime(df_fact_divida['datCadastramento'], errors='coerce').dt.strftime('%Y%m%d')
df_fact_divida['id_tempo_cadastro_sk'] = df_fact_divida['id_tempo_cadastro_sk'].fillna(-1).astype(int)
df_fact_divida['flag_saldo_negativo'] = df_fact_divida['ValSaldo'] < 0
colunas_finais_fatos = {'numCDA': 'num_cda_nk', 'id_devedor_sk': 'id_devedor_sk', 'id_natureza_sk': 'id_natureza_sk', 'id_situacao_sk': 'id_situacao_sk', 'id_tempo_cadastro_sk': 'id_tempo_cadastro_sk', 'anoInscricao': 'ano_inscricao', 'ValSaldo': 'valor_saldo', 'probRecuperacao': 'prob_recuperacao', 'codFaseCobranca': 'cod_fase_cobranca', 'flag_saldo_negativo': 'flag_saldo_negativo'}
df_fact_divida_final = df_fact_divida[list(colunas_finais_fatos.keys())].rename(columns=colunas_finais_fatos)
print("FASE DE TRANSFORMAÇÃO: Concluída.")

# =============================================================================
# FASE 3: CARGA (LOAD)
# =============================================================================
print("\nIniciando a FASE DE CARGA...")
try:
    # Lê a string de conexão da variável de ambiente, com um valor padrão para desenvolvimento local
    connection_string = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/divida_ativa_dw")
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        print("Limpando as tabelas do Data Warehouse...")
        connection.execute(text("TRUNCATE TABLE fact_divida_ativa, dim_devedor, dim_divida_natureza, dim_cda_situacao, dim_tempo RESTART IDENTITY CASCADE;"))
        connection.commit()
        print("Carregando dim_devedor...")
        df_dim_devedor.to_sql('dim_devedor', engine, if_exists='append', index=False)
        print("Carregando dim_divida_natureza...")
        df_dim_natureza.to_sql('dim_divida_natureza', engine, if_exists='append', index=False)
        print("Carregando dim_cda_situacao...")
        df_dim_situacao.to_sql('dim_cda_situacao', engine, if_exists='append', index=False)
        print("Carregando dim_tempo...")
        df_dim_tempo.to_sql('dim_tempo', engine, if_exists='append', index=False)
        print("Carregando fact_divida_ativa...")
        df_fact_divida_final.to_sql('fact_divida_ativa', engine, if_exists='append', index=False)
    print("\nFASE DE CARGA: Concluída com sucesso!")
    print("\nProcesso de ETL finalizado.")
except Exception as e:
    print(f"\nOcorreu um erro durante a FASE DE CARGA: {e}")
