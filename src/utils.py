import uuid
import json
import time
import logging
import functools
from pathlib import Path
from datetime import datetime
from typing import Dict, List
from types import SimpleNamespace

import yaml
import polars as pl

def get_current_date() -> str:
    """Retorna a data atual no formato YYYY-MM-DD"""
    return datetime.now().strftime('%Y-%m-%d')

class Config: 
    """Classe para representar a configuração do pipeline, carregada do YAML."""
    def __init__(self, args=None):
        self._args = self._check_args(args)
        self._config = None
        self._processing_date = get_current_date()
        self._load_yaml()
        self._replace_with_args()
        self._to_dot_notation()

    def __getattr__(self, name):
            """Permite acesso direto aos atributos do config.yaml via config.url, config.storage, etc."""
            return getattr(self._config, name)

    @property
    def processing_date(self):
        """Retorna a data de processamento, que pode ser definida via CLI ou carregada do YAML."""
        return self._processing_date
    

    @property
    def expected_files(self) -> dict:
        """Retorna um dicionário {file_name: table_name} para os arquivos esperados na ingestão."""
        files = {}
        for key, content in self.content.__dict__.items():
            files[content.file_name] = key
        return files

    def _load_yaml(self) -> bool:
        """Carrega a configuração do arquivo YAML e converte para um namespace."""
        file = self._args.config if self._args and self._args.config else 'config.yaml'
        try:
            with open(file, 'r') as f:
                self._config = yaml.safe_load(f)
            return True
        except Exception as e:
            raise ValueError(f"Erro ao carregar o arquivo YAML: {e}")
    
    def _check_args(self, args):
        """Valida os argumentos fornecidos via CLI, se existirem."""
        if args:
            if args.log_level and args.log_level.upper() not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
                raise ValueError(f"Nível de log inválido: {args.log_level}. Use um dos seguintes: DEBUG, INFO, WARNING, ERROR, CRITICAL.")
            if args.date:
                try:
                    datetime.strptime(args.date, '%Y-%m-%d')
                except ValueError:
                    raise ValueError(f"Data inválida: {args.date}. Use o formato YYYY-MM-DD.")
            if not Path(args.config).is_file():
                raise FileNotFoundError(f"Arquivo de configuração YAML não encontrado: {args.config}")
        return args

    def _replace_with_args(self) -> bool:
        """Substitui valores do YAML por argumentos fornecidos via CLI, se existirem."""
        if self._args:
            if self._args.log_level:
                print(f"Configuração de log_level sobrescrita por argumento CLI: {self._args.log_level}")
                self._config['logging']['level'] = self._args.log_level.upper()
            if self._args.date:
                print(f"Data de processamento sobrescrita por argumento CLI: {self._args.date}")
                self._processing_date = self._args.date
            return True
        return False

    def _to_dot_notation(self):
        """Converte o dicionário de configuração para um namespace para acesso via dot notation."""
        def dict_to_namespace(d):
            if isinstance(d, dict):
                return SimpleNamespace(**{k: dict_to_namespace(v) for k, v in d.items()})
            elif isinstance(d, list):
                return [dict_to_namespace(i) for i in d]
            return d
        self._config = dict_to_namespace(self._config)

    def get_invalid_values(self, table_name: str) -> List[str]:
        """Retorna a lista de valores inválidos para uma tabela específica, conforme definido no YAML."""
        content = getattr(self._config, 'content', None)
        if content and hasattr(content, table_name):
            table = getattr(content, table_name)
            return getattr(table, 'invalid_values', [])
        return []
            
    def get_cols_format(self, table_name: str, format_substring: str) -> List[str]:
        """Retorna uma lista de nomes de colunas que contêm a substring informada no campo 'format'."""
        content = getattr(self._config, 'content', None)
        column_list = []
        
        if content and hasattr(content, table_name):
            table = getattr(content, table_name)
            schema = getattr(table, 'schema', [])
            
            for col in schema:
                col_name = getattr(col, 'name', None)
                col_format = getattr(col, 'format', None)
                
                # Verifica se ambos existem e se a substring está contida no formato
                if col_name and col_format:
                    if format_substring.upper() in col_format.upper():
                        column_list.append(col_name)
                        
        return column_list

    def get_col(self, table_name: str, col_name: str) -> Dict[str, str]:
        """Retorna o dicionário completo de configuração de uma coluna específica."""
        content = getattr(self._config, 'content', None)
        
        if content and hasattr(content, table_name):
            table = getattr(content, table_name)
            schema = getattr(table, 'schema', [])
            
            for col in schema:
                if getattr(col, 'name', None) == col_name:
                    return {
                        "name": getattr(col, 'name', ''),
                        "type": getattr(col, 'type', ''),
                        "format": getattr(col, 'format', '')
                    }
        return {}
    
    def build_polars_schema(self, table_name: str) -> dict:
        """Constrói um schema do Polars a partir da definição de schema no YAML."""
        import polars as pl
        type_mapper = {
            "string": pl.Utf8,
            "integer": pl.Int64,
            "long": pl.Int64,
            "double": pl.Float64,
            "float": pl.Float64,
            "date": pl.Date,
        }
        content = getattr(self._config, 'content', None)
        if not content or not hasattr(content, table_name):
            raise ValueError(f"Tabela '{table_name}' não encontrada no arquivo de configuração.")
        table = getattr(content, table_name)
        schema = getattr(table, 'schema', [])
        if not schema:
            raise ValueError(f"Schema não encontrado para a tabela '{table_name}' no arquivo de configuração.")
        polars_schema = {}
        for col in schema:
            col_type = getattr(col, 'type', 'string').lower()
            pl_type = type_mapper.get(col_type, pl.Utf8)
            polars_schema[getattr(col, 'name')] = pl_type
        return polars_schema

def track_execution(job_name=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = next((arg for arg in args if hasattr(arg, 'info')), None)
            job_id = str(uuid.uuid4())
            start_time = time.time()
            execution_context = {} 
            kwargs['context'] = execution_context   
            payload = {
                "job_id": job_id,
                "job_name": job_name or func.__name__,
                "status": "started",
                "start_at": datetime.now().isoformat()
            }
            try:
                if logger: 
                    logger.info(f"Iniciando {payload['job_name']}", extra=payload)
                result = func(*args, **kwargs)
                payload.update({
                    "status": "success",
                    "duration": round(time.time() - start_time, 4),
                    "end_at": datetime.now().isoformat(),
                    **execution_context
                })
                if logger: 
                    logger.info(f"Sucesso em {payload['job_name']}", extra=payload)
                return result
            except Exception as e:
                payload.update({
                    "status": "error",
                    "duration": round(time.time() - start_time, 4),
                    "exception": str(e),
                    **execution_context
                })
                if logger: logger.error(f"Erro em {payload['job_name']}", extra=payload)
                raise
        return wrapper
    return decorator

class JSONFormatter(logging.Formatter):
    def format(self, record) -> str:
        log_record = {
            "time": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        standard_attrs = logging.LogRecord(None, None, None, None, None, None, None).__dict__.keys()
        for key, value in record.__dict__.items():
            if key not in standard_attrs:
                log_record[key] = value
        return json.dumps(log_record, ensure_ascii=False)

def get_logger(config: Config) -> logging.Logger:
    """Configura e retorna um logger com formato JSON padronizado"""
    name = getattr(config, 'name', 'ETL_Pipeline')
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        level = getattr(logging, config.logging.level.upper(), logging.INFO)
        handler = logging.StreamHandler()
        file_handler = logging.FileHandler(f"{config.storage.logs}/{name}.jsonl")
        handler.setFormatter(JSONFormatter())
        file_handler.setFormatter(JSONFormatter())
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.addHandler(file_handler)
    return logger