import os
import pandas as pd
from pymongo import MongoClient
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO)

def conectar_mongodb():
    """Conecta ao MongoDB usando a URI fornecida e retorna a coleção."""
    uri = "mongodb+srv://blequizit:aYKbkBUB0FPI39UX@dadospessoas.k6sjq.mongodb.net/"
    
    try:
        # Conectando ao MongoDB usando a URI
        client = MongoClient(uri)
        # Conectar ao banco de dados 'DadosPessoas1' e à coleção 'DadosPessoas01'
        db = client['DadosPessoas1']  # Nome do banco de dados
        collection = db['DadosPessoas01']  # Nome da coleção
        logging.info("Conexão com o MongoDB estabelecida com sucesso.")
        return collection
    except Exception as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

def verificar_csv(diretorio):
    """Verifica se há arquivos CSV no diretório e retorna a lista de arquivos encontrados."""
    arquivos_csv = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
    
    if arquivos_csv:
        logging.info(f"Arquivos CSV encontrados: {arquivos_csv}")
        return arquivos_csv
    else:
        logging.warning("Nenhum arquivo CSV encontrado no diretório.")
        return []

def inserir_dados_no_mongodb(arquivo_csv):
    """Insere dados de um arquivo CSV no MongoDB."""
    try:
        df = pd.read_csv(arquivo_csv)
        collection = conectar_mongodb()
        
        if collection:
            dados = df.to_dict(orient='records')
            collection.insert_many(dados)
            logging.info(f"Dados inseridos no MongoDB a partir de {arquivo_csv}.")
        else:
            logging.error("Coleção não encontrada. Dados não foram inseridos.")
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado: {arquivo_csv}")
    except pd.errors.EmptyDataError:
        logging.error(f"O arquivo CSV está vazio: {arquivo_csv}")
    except Exception as e:
        logging.error(f"Erro ao processar {arquivo_csv}: {e}")

def processar_csv(diretorio):
    """Verifica e insere dados de arquivos CSV no MongoDB."""
    arquivos_csv = verificar_csv(diretorio)
    
    for arquivo_csv in arquivos_csv:
        caminho_arquivo = os.path.join(diretorio, arquivo_csv)
        inserir_dados_no_mongodb(caminho_arquivo)

# Defina a variável de ambiente antes de executar o script
diretorio = '/caminho/para/o/diretorio'  # Substitua pelo caminho correto do diretório
processar_csv(diretorio)
