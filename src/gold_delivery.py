import os
from logging import Logger

import duckdb

from src.utils import Config, track_execution

@track_execution(job_name="delivery")
def run_delivery(config: Config, logger: Logger, context: dict = None) -> None:
    """Cria as tabelas no DuckDB apontando para os dados transformados na camada silver e salva a tabela final na camada gold."""
    con = create_connection(config)
    create_views(config, con)
    build_dimensions(con)
    build_facts(con)
    export_gold_to_parquet(con, config)
    close_connection(con)

def create_connection(config: Config) -> duckdb.DuckDBPyConnection:
    """Cria uma conexão com o DuckDB, utilizando um arquivo local para persistência."""

    db_name = os.path.join(config.storage.gold, f"{config.name}.db")
    con = duckdb.connect(database=db_name, read_only=False)
    return con

def close_connection(con: duckdb.DuckDBPyConnection):
    """Fecha a conexão com o DuckDB."""
    if con:
        con.close()

def create_views(config: Config, con: duckdb.DuckDBPyConnection):
    """Cria views temporárias apontando para os Parquets da Silver."""

    silver_path = config.storage.silver
    con.execute(f"""
            CREATE OR REPLACE VIEW stg_emendas AS 
            SELECT * 
            FROM read_parquet(
                '{silver_path}/EmendasParlamentares.parquet');
            
            CREATE OR REPLACE VIEW stg_convenios AS 
            SELECT * 
            FROM read_parquet(
                '{silver_path}/EmendasParlamentares_Convenios.parquet');

            CREATE OR REPLACE VIEW stg_favorecidos AS 
            SELECT * 
            FROM read_parquet(
                '{silver_path}/EmendasParlamentares_PorFavorecido.parquet');
            
        """)

def build_dimensions(con: duckdb.DuckDBPyConnection):
    """Cria as tabelas de dimensão (Star Schema)."""
    
    # Dimensão Autor
    con.execute("""
        CREATE OR REPLACE TABLE dim_autor AS
        SELECT DISTINCT
            "Código do Autor da Emenda" AS autor_id,
            "Nome do Autor da Emenda" AS nome_autor,
            "Tipo de Emenda" AS tipo_emenda
        FROM stg_emendas;
    """)

    # Dimensão Localidade
    con.execute("""
        CREATE OR REPLACE TABLE dim_localidade AS
        SELECT DISTINCT
            "Código Município IBGE" AS municipio_id,
            "Município" AS nome_municipio,
            "UF" AS sigla_uf,
            "Região" AS nome_regiao
        FROM stg_emendas;
    """)

def build_facts(con: duckdb.DuckDBPyConnection):
    """Cria as tabelas fato com métricas financeiras."""
    
    # Fato Execução (Foco em Orçamento)
    con.execute("""
        CREATE OR REPLACE TABLE fato_execucao_orcamentaria AS
        SELECT 
            "Código da Emenda" AS emenda_id,
            "Código do Autor da Emenda" AS autor_id,
            "Código Município IBGE" AS municipio_id,
            "Ano da Emenda" AS ano_referencia,
            "Valor Empenhado" AS valor_empenhado,
            "Valor Pago" AS valor_pago
        FROM stg_emendas;
    """)

def export_gold_to_parquet(con: duckdb.DuckDBPyConnection, config: Config):
    """Opcional: Exporta as tabelas da Gold de volta para Parquet (para PowerBI/Tableau)."""
    output_path = config.storage.gold
    tables = ['dim_autor', 'dim_localidade', 'fato_execucao_orcamentaria']
    for table in tables:
        path = f"{output_path}/{table}.parquet"
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")