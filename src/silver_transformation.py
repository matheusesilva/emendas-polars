import os
import re
from datetime import datetime

import polars as pl

from src.utils import Config, track_execution

@track_execution(job_name="transformation")
def run_transformations(config: Config, logger, context: dict = None):
    """Lê da Bronze, aplica transformações Polars e salva na Silver."""
    expected_files = config.expected_files.values()

    for table_name in expected_files:
        bronze_file = os.path.join(config.storage.bronze, f"{table_name}.parquet")
        silver_file = os.path.join(config.storage.silver, f"{table_name}.parquet")

        if os.path.exists(bronze_file):
            df = pl.scan_parquet(bronze_file)
            
            # Chain transformations using pipe
            df = (
                df.pipe(_format_strings, config, table_name)
                  .pipe(_clean_invalid_values, config, table_name)
                  .pipe(_format_currency, config, table_name)
                  .pipe(_format_numbers, config, table_name)
                  .pipe(_format_dates, config, table_name)
                  .with_columns(pl.lit(datetime.now().isoformat()).alias("timestamp_transform").cast(pl.Datetime))
            )
            
            df.sink_parquet(silver_file)
        else:
            logger.warning(f"Arquivo Bronze não encontrado para {table_name}")

def _format_currency(df: pl.LazyFrame, config: Config, table_name: str) -> pl.LazyFrame:
    cols = config.get_cols_format(table_name, "NUM_BRL")
    if not cols: return df
    
    return df.with_columns([
        pl.col(c)
        .str.replace_all(r"[^0-9,]", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
        .alias(c) 
        for c in cols
    ])

def _clean_invalid_values(df: pl.LazyFrame, config: Config, table_name: str) -> pl.LazyFrame:
    invalid = config.get_invalid_values(table_name)
    if not invalid:
        return df
    
    # Apply to all columns found in the schema
    all_cols = config.get_cols_format(table_name, "")
    return df.with_columns([
        pl.when(pl.col(c).str.to_lowercase().is_in(invalid))
        .then(None)
        .otherwise(pl.col(c))
        .alias(c)
        for c in all_cols
    ])

def _format_strings(df: pl.LazyFrame, config: "Config", table_name: str) -> pl.LazyFrame:
    """Mesma estrutura do Spark, adaptada para Polars."""
    
    cols_initcap = config.get_cols_format(table_name, "STR_INITC")
    cols_sentcap = config.get_cols_format(table_name, "STR_SENTC")
    all_affected = list(set(cols_initcap + cols_sentcap))

    # --- Funções Internas (Mantendo a lógica Spark) ---

    def to_initcap(df: pl.LazyFrame) -> pl.LazyFrame:
        if cols_initcap:
            return df.with_columns([pl.col(c).str.to_titlecase().alias(c) for c in cols_initcap])
        return df

    def to_sentcap(df: pl.LazyFrame) -> pl.LazyFrame:
        if cols_sentcap:
            return df.with_columns([
                (pl.col(c).str.slice(0, 1).str.to_uppercase() + 
                 pl.col(c).str.slice(1).str.to_lowercase()).alias(c)
                for c in cols_sentcap
            ])
        return df
    
    def handle_prepositions(df: pl.LazyFrame) -> pl.LazyFrame:
        preps = ["a","o","as","os","em","de","do","da","dos","das","para",
                 "por","com","e", "sem", "sobre", "entre", "até", "desde", 
                 "contra", "à", "às", "ao", "aos", "pela", "pelo", "pelos", "pelas"]
        
        # Regex nativo (sem lambda aqui para evitar o erro)
        exprs = []
        for col in all_affected:
            col_expr = pl.col(col)
            for p in preps:
                col_expr = col_expr.str.replace_all(rf"(?i)\b{p}\b", p.lower())
            exprs.append(col_expr.alias(col))
        return df.with_columns(exprs)
    
    def handle_states(df: pl.LazyFrame) -> pl.LazyFrame:
        ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", 
               "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
        ufs_pattern = "|".join(ufs)
        pattern = rf"(?i)(?:\s*[-/]\s*|\()({ufs_pattern})\b\s*\)?"
        return df.with_columns([
            pl.col(c).str.replace_all(pattern, " ($1)").alias(c)
            for c in all_affected
        ])
    
    def handle_initialisms(df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Esta é a parte que dava erro. Usamos map_elements que é o equivalente 
        ao 'transform' de arrays do Spark para lógica Python.
        """
        pattern = re.compile(r"\(([^)]{1,5})\)")
        
        def upper_sigla(text):
            if text is None: return None
            return pattern.sub(lambda m: f"({m.group(1).upper()})", text)

        return df.with_columns([
            pl.col(c).map_elements(upper_sigla, return_dtype=pl.String).alias(c)
            for c in all_affected
        ])

    # --- Execução em Cadeia (Pipe = Transform) ---
    return (
        df.pipe(to_initcap)
          .pipe(to_sentcap)
          .pipe(handle_prepositions)
          .pipe(handle_states)
          .pipe(handle_initialisms)
    )

def _format_dates(df: pl.LazyFrame, config: Config, table_name: str) -> pl.LazyFrame:
    cols = config.get_cols_format(table_name, "DAT")
    if not cols:
        return df

    exprs = []
    for col in cols:
        raw_fmt = config.get_col(table_name, col)["format"].split("_")[1]
        p_fmt = raw_fmt.replace("dd", "%d").replace("MM", "%m").replace("yyyy", "%Y")
        exprs.append(pl.col(col).str.to_date(format=p_fmt, strict=False).alias(col))
    
    return df.with_columns(exprs)

def _format_numbers(df: pl.LazyFrame, config: Config, table_name: str) -> pl.LazyFrame:
    cols = config.get_cols_format(table_name, "NUM_INT")
    if not cols:
        return df

    return df.with_columns([
        pl.col(c).str.replace_all(r'[^\d]', "")
        .cast(pl.Int32, strict=False)
        .alias(c)
        for c in cols
    ])