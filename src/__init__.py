import polars as pl
from src.bronze_ingestion import get_zip, run_ingestion
from src.silver_transformation import run_transformations
from src.gold_delivery import run_delivery
from src.utils import Config, get_logger

class ETLPipeline:
    def __init__(self, args=None):
        self.args = args
        self.config = None
        self.logger = None

    def _set_config(self):
        self.config = Config(self.args)

    def _set_logger(self):
        self.logger = get_logger(self.config)

    def run(self):
        """Executa o pipeline completo: Ingestão -> Transformação -> Entrega"""
        self._set_config()
        self._set_logger()

        # --- GET RAW DATA ---
        zip_path = get_zip(self.config, self.logger)

        # --- INGESTÃO ---
        run_ingestion(self.config, self.logger, zip_path)

        # --- TRANSFORMAÇÃO ---
        run_transformations(self.config, self.logger)

        # --- ENTREGA ---
        run_delivery(self.config, self.logger)