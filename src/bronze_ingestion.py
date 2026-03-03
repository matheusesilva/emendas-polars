import os
import shutil
import zipfile
import tempfile
from logging import Logger
from datetime import datetime

import requests
import polars as pl

from src.utils import Config, get_current_date, track_execution

def extract_zip_to_tmp(zip_path: str) -> str:
    """Extrai o conteúdo do zip para uma pasta temporária e retorna o caminho."""
    print(f"Extraindo arquivo zip: {zip_path}")
    tmp_dir = tempfile.mkdtemp(prefix="emendas_tmp_")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(tmp_dir)
    return tmp_dir

@track_execution(job_name="get_zip")
def get_zip(config: Config, logger: Logger, context: dict = None) -> str:
    """Baixa o arquivo zip da URL definida no config e salva na pasta raw particionada por data."""
    
    url = config.url
    file_name = config.file_name
    save_path = config.storage.raw
    zip_path = os.path.join(save_path, file_name)

    os.makedirs(save_path, exist_ok=True)
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        context['downloaded_file'] = file_name
        context['size_mb'] = round(os.path.getsize(zip_path) / (1024*1024), 2)
    else:
        raise Exception(f"Erro ao baixar arquivo: {url}")
    return zip_path

def _ingest_file(config: Config, file_path: str, table_name: str) -> None:
    """Lê um arquivo CSV específico, aplica o schema, adiciona colunas de metadata e salva na camada Bronze."""     
    bronze_path = config.storage.bronze
    content_conf = getattr(config.content, table_name)
    sep = getattr(content_conf, 'separator', ';')
    encoding = getattr(content_conf, 'encoding', 'iso-8859-1')
    schema = config.build_polars_schema(table_name)

    df = pl.read_csv(
        file_path, 
        separator=sep, 
        encoding=encoding, 
        schema=schema, 
        infer_schema_length=0,
        rechunk=True
        )
    
    df = df.lazy().with_columns([
        pl.lit(table_name).alias("arquivo_fonte"),
        pl.lit(datetime.now().isoformat()).alias("timestamp_ingestao").cast(pl.Datetime),
        pl.lit(config.processing_date).alias("reference_date").cast(pl.Date)
    ])

    source_path = os.path.join(bronze_path,table_name + ".parquet")

    if os.path.exists(source_path): # sink_parquet não sobrepõe arquivos
        os.remove(source_path)

    df.sink_parquet(source_path)

@track_execution(job_name="ingestion")
def run_ingestion(config: Config, logger: Logger, zip_path: str, context: dict = None) -> None:
    """Extrai o zip, faz loop nos arquivos, verifica se são esperados, lê e salva cada CSV em parquet particionado por ingestion_date na bronze."""
    
    tmp_dir = extract_zip_to_tmp(zip_path)
    expected_files = config.expected_files

    for file_name in os.listdir(tmp_dir):
        if file_name in expected_files:
            file_path = os.path.join(tmp_dir, file_name)
            table_name = expected_files[file_name]
            _ingest_file(config, file_path, table_name)
        else:
            context['unexpected_files'] = context.get('unexpected_files', []) + [file_name]

    shutil.rmtree(tmp_dir)
    return 
