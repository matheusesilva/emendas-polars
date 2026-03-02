import argparse
from src import ETLPipeline

if __name__ == "__main__":

    # Configurações de argumentos para permitir flexibilidade na execução do pipeline
    parser = argparse.ArgumentParser(description="Executa o pipeline ETL")
    parser.add_argument('--log-level', type=str, help='Nível de logging (ex: INFO, DEBUG, WARNING)', default='INFO')
    parser.add_argument('--config', type=str, help='Nome do arquivo de configuração YAML', default='config.yaml')
    parser.add_argument('--date', type=str, help='Data para filtrar os dados (formato YYYY-MM-DD) para processamento')
    args = parser.parse_args()

    pipeline = ETLPipeline(args=args)
    pipeline.run()