from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from collections import defaultdict
from .schemas import CDASearchFilters, DevedorDetails, MontanteAcumuladoItem
from .db.session import get_db_session

app = FastAPI(
    title="API de Dívida Ativa",
    description="API para consulta e análise de dados da Dívida Ativa Municipal, conforme o desafio técnico.",
    version="1.0.0"
)

@app.get("/")
def read_root():
    return {"message": "API de Divida Ativa"}

@app.get("/health")
def health_check():
    return {"status": "ok"}


# --- Endpoints ---

@app.get("/resumo/quantidade_cdas")
async def get_quantidade_cdas(db: AsyncSession = Depends(get_db_session)):
    """
    Retorna a quantidade total de CDAs para cada tipo de tributo.
    """
    query = text("""
        SELECT 
            n.nome_natureza AS name, 
            COUNT(f.id_divida_ativa) AS "Quantidade"
        FROM fact_divida_ativa f
        JOIN dim_divida_natureza n ON f.id_natureza_sk = n.id_natureza_sk
        GROUP BY n.nome_natureza
        ORDER BY "Quantidade" DESC;
    """)
    result = await db.execute(query)
    data = result.mappings().all()
    return [dict(row) for row in data]

@app.get("/resumo/saldo_cdas")
async def get_saldo_cdas(db: AsyncSession = Depends(get_db_session)):
    """
    Retorna o saldo total das CDAs agrupado por tipo de tributo.
    """
    query = text("""
        SELECT 
            n.nome_natureza AS name, 
            SUM(f.valor_saldo) AS "Saldo"
        FROM fact_divida_ativa f
        JOIN dim_divida_natureza n ON f.id_natureza_sk = n.id_natureza_sk
        GROUP BY n.nome_natureza
        ORDER BY "Saldo" DESC;
    """)
    result = await db.execute(query)
    data = result.mappings().all()
    return [dict(row) for row in data]

@app.get("/resumo/inscricoes")
async def get_inscricoes_por_ano(db: AsyncSession = Depends(get_db_session)):
    """
    Retorna a quantidade de inscrições de CDAs por ano.
    """
    query = text("""
        SELECT 
            ano_inscricao AS ano, 
            COUNT(id_divida_ativa) AS "Quantidade"
        FROM fact_divida_ativa
        GROUP BY ano_inscricao
        ORDER BY ano_inscricao ASC;
    """)
    result = await db.execute(query)
    data = result.mappings().all()
    return [dict(row) for row in data]

@app.get("/resumo/distribuicao_cdas")
async def get_distribuicao_cdas(db: AsyncSession = Depends(get_db_session)):
    """
    Retorna a distribuição percentual das CDAs por situação para cada tipo de tributo.
    """
    query = text("""
        SELECT 
            n.nome_natureza,
            s.nome_situacao,
            COUNT(f.id_divida_ativa) AS quantidade
        FROM fact_divida_ativa f
        JOIN dim_divida_natureza n ON f.id_natureza_sk = n.id_natureza_sk
        JOIN dim_cda_situacao s ON f.id_situacao_sk = s.id_situacao_sk
        GROUP BY n.nome_natureza, s.nome_situacao;
    """)
    result = await db.execute(query)
    data = result.mappings().all()
    resumo_por_natureza = defaultdict(lambda: {"total": 0})
    for row in data:
        natureza = row['nome_natureza']
        situacao = row['nome_situacao']
        quantidade = row['quantidade']
        resumo_por_natureza[natureza][situacao] = quantidade
        resumo_por_natureza[natureza]["total"] += quantidade
    output = []
    for natureza, valores in resumo_por_natureza.items():
        total = valores["total"]
        if total > 0:
            item = {
                "name": natureza,
                "Em cobranca": round((valores.get("Cobrança", 0) / total) * 100, 2),
                "Cancelada": round((valores.get("Cancelada", 0) / total) * 100, 2),
                "Quitada": round((valores.get("Paga", 0) / total) * 100, 2)
            }
            output.append(item)
    return output

@app.get("/resumo/montante_acumulado", response_model=list[MontanteAcumuladoItem])
async def get_montante_acumulado(db: AsyncSession = Depends(get_db_session)):
    """
    Exibe o percentual acumulado do montante da dívida por tipo de tributo em faixas de percentil.
    """
    query = text("""
        WITH dividas_categorizadas AS (
            -- PASSO 1: Normalizar os nomes dos tributos em categorias limpas primeiro.
            SELECT
                CASE
                    WHEN n.nome_natureza ILIKE 'IPTU%' THEN 'IPTU'
                    WHEN n.nome_natureza ILIKE 'ISS%' THEN 'ISS'
                    WHEN n.nome_natureza ILIKE '%MULTA%' THEN 'Multas'
                    WHEN n.nome_natureza ILIKE 'ITBI%' THEN 'ITBI'
                    WHEN n.nome_natureza ILIKE '%TAXA%' AND n.nome_natureza NOT ILIKE 'IPTU%' THEN 'Taxas'
                    ELSE 'Outros'
                END as categoria,
                f.valor_saldo
            FROM fact_divida_ativa f
            JOIN dim_divida_natureza n ON f.id_natureza_sk = n.id_natureza_sk
            WHERE f.valor_saldo > 0
        ),
        ranked_dividas AS (
            -- PASSO 2: Agora, calcular o percentil sobre as categorias limpas.
            SELECT
                categoria,
                valor_saldo,
                NTILE(100) OVER (PARTITION BY categoria ORDER BY valor_saldo) AS percentil
            FROM dividas_categorizadas
        ),
        soma_por_percentil AS (
            -- PASSO 3: Agregar os valores dentro de cada percentil.
            SELECT
                categoria,
                percentil,
                SUM(valor_saldo) as total_saldo_percentil
            FROM ranked_dividas
            GROUP BY categoria, percentil
        ),
        soma_acumulada AS (
            -- PASSO 4: Calcular a soma acumulada sobre os dados agregados.
            SELECT
                categoria,
                percentil,
                SUM(total_saldo_percentil) OVER (PARTITION BY categoria ORDER BY percentil) as saldo_acumulado,
                SUM(total_saldo_percentil) OVER (PARTITION BY categoria) as saldo_total_categoria
            FROM soma_por_percentil
        )
        -- PASSO 5: Calcular o percentual final e pivotar o resultado.
        SELECT
            percentil AS "Percentil",
            COALESCE(MAX(CASE WHEN categoria = 'IPTU' THEN (saldo_acumulado / saldo_total_categoria) * 100 END), 0) AS "IPTU",
            COALESCE(MAX(CASE WHEN categoria = 'ISS' THEN (saldo_acumulado / saldo_total_categoria) * 100 END), 0) AS "ISS",
            COALESCE(MAX(CASE WHEN categoria = 'Taxas' THEN (saldo_acumulado / saldo_total_categoria) * 100 END), 0) AS "Taxas",
            COALESCE(MAX(CASE WHEN categoria = 'Multas' THEN (saldo_acumulado / saldo_total_categoria) * 100 END), 0) AS "Multas",
            COALESCE(MAX(CASE WHEN categoria = 'ITBI' THEN (saldo_acumulado / saldo_total_categoria) * 100 END), 0) AS "ITBI"
        FROM soma_acumulada
        WHERE percentil IN (1, 5, 10, 25, 50, 75, 90, 95, 100)
        GROUP BY percentil
        ORDER BY percentil;
    """)
    result = await db.execute(query)
    data = result.mappings().all()
    return [dict(row) for row in data]

@app.get("/cda/search")
async def search_cdas(db: AsyncSession = Depends(get_db_session), filters: CDASearchFilters = Depends()):
    query_base = """
        SELECT 
            f.num_cda_nk,
            f.valor_saldo,
            (SELECT EXTRACT(YEAR FROM CURRENT_DATE)) - f.ano_inscricao as qtde_anos_idade_cda,
            s.cod_situacao_nk as agrupamento_situacao,
            n.nome_natureza as natureza,
            f.prob_recuperacao as score
        FROM fact_divida_ativa f
        JOIN dim_cda_situacao s ON f.id_situacao_sk = s.id_situacao_sk
        JOIN dim_divida_natureza n ON f.id_natureza_sk = n.id_natureza_sk
    """
    where_clauses = []
    params = {}
    if filters.numCDA:
        where_clauses.append("f.num_cda_nk = :numCDA")
        params["numCDA"] = int(filters.numCDA)
    if filters.minSaldo is not None:
        where_clauses.append("f.valor_saldo >= :minSaldo")
        params["minSaldo"] = filters.minSaldo
    if filters.maxSaldo is not None:
        where_clauses.append("f.valor_saldo <= :maxSaldo")
        params["maxSaldo"] = filters.maxSaldo
    if filters.minAno is not None:
        where_clauses.append("f.ano_inscricao >= :minAno")
        params["minAno"] = filters.minAno
    if filters.maxAno is not None:
        where_clauses.append("f.ano_inscricao <= :maxAno")
        params["maxAno"] = filters.maxAno
    if filters.natureza:
        where_clauses.append("n.nome_natureza ILIKE :natureza")
        params["natureza"] = f"%{filters.natureza}%"
    if where_clauses:
        query_base += " WHERE " + " AND ".join(where_clauses)
    order_by_map = {'ano': 'f.ano_inscricao', 'valor': 'f.valor_saldo'}
    order_column = order_by_map.get(filters.sort_by, 'f.ano_inscricao')
    query_base += f" ORDER BY {order_column} {filters.sort_order.upper()}"
    query_base += " LIMIT :limit OFFSET :offset"
    params["limit"] = filters.limit
    params["offset"] = filters.offset
    result = await db.execute(text(query_base), params)
    data = result.mappings().all()
    return [dict(row) for row in data]

@app.get("/cda/{num_cda}/devedor", response_model=DevedorDetails)
async def get_devedor_por_cda(num_cda: int, db: AsyncSession = Depends(get_db_session)):
    query = text("""
        SELECT
            d.nome AS name,
            d.tipo_pessoa,
            d.documento
        FROM fact_divida_ativa f
        JOIN dim_devedor d ON f.id_devedor_sk = d.id_devedor_sk
        WHERE f.num_cda_nk = :num_cda
    """)
    result = await db.execute(query, {"num_cda": num_cda})
    devedor = result.mappings().first()
    if not devedor:
        raise HTTPException(status_code=404, detail="CDA não encontrada ou devedor não associado.")
    return devedor
