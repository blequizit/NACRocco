import os
import pandas as pd
from pymongo import MongoClient, errors
import logging
from datetime import datetime

# Configuração do logging
logging.basicConfig(level=logging.INFO)

def log_info(message):
    """Log de informações."""
    logging.info(message)

def log_error(message):
    """Log de erros."""
    logging.error(message)

def log_warning(message):
    """Log de avisos."""
    logging.warning(message)

def validar_dados(dado):
    """Valida os dados do registro e retorna uma lista de erros."""
    erros = []

    # Verifica se o id é um número inteiro
    if not isinstance(dado['id'], int):
        erros.append(f"ID deve ser um número inteiro, encontrado: {dado['id']}")

    # Verifica se o nome não está vazio
    if not dado['nome']:
        erros.append("Nome está faltando.")

    # Verifica se o email não está vazio
    if not dado['email']:
        erros.append("Email está faltando.")

    # Verifica se o telefone não está vazio
    if not dado['telefone']:
        erros.append("Telefone está faltando.")

    # Verifica se a data de nascimento não está vazia e é válida
    if not dado['data_nascimento']:
        erros.append("Data de nascimento está faltando.")
    else:
        try:
            datetime.strptime(dado['data_nascimento'], '%Y-%m-%d')  # Verifica o formato da data
        except ValueError:
            erros.append(f"Data de nascimento inválida: {dado['data_nascimento']}")

    # Verifica se o sexo não está vazio
    if not dado['sexo']:
        erros.append("Sexo está faltando.")

    # Verifica se a cidade não está vazia
    if not dado['cidade']:
        erros.append("Cidade está faltando.")

    # Verifica se o estado não está vazio
    if not dado['estado']:
        erros.append("Estado está faltando.")

    return erros

def conectar_mongodb(uri):
    """Conecta ao MongoDB usando a URI fornecida e retorna a coleção."""
    log_info("Tentando conectar ao MongoDB...")
    try:
        client = MongoClient(uri)
        db = client['DadosPessoas1']  # Nome do banco de dados
        collection = db['DadosPessoas01']  # Nome da coleção
        log_info("Conexão com o MongoDB estabelecida com sucesso.")
        return collection
    except errors.ConnectionError:
        log_error("Erro de conexão ao MongoDB.")
        return None
    except Exception as e:
        log_error(f"Erro ao conectar ao MongoDB: {e}")
        return None

def verificar_csv(diretorio):
    """Verifica se há arquivos CSV no diretório e retorna a lista de arquivos encontrados."""
    log_info(f"Verificando arquivos CSV no diretório: {diretorio}")
    arquivos_csv = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
    
    if arquivos_csv:
        log_info(f"Arquivos CSV encontrados: {arquivos_csv}")
        return arquivos_csv
    else:
        log_warning("Nenhum arquivo CSV encontrado no diretório.")
        return []

def inserir_dados_no_mongodb(arquivo_csv, collection):
    """Insere dados de um arquivo CSV no MongoDB."""
    log_info(f"Iniciando a inserção de dados do arquivo: {arquivo_csv}")
    try:
        df = pd.read_csv(arquivo_csv)
        dados = df.to_dict(orient='records')

        log_info(f"{len(dados)} registros lidos do arquivo CSV.")

        if collection is not None:  # Verificação correta da coleção
            registros_inseridos = 0
            for dado in dados:
                # Valida os dados
                erros = validar_dados(dado)
                if erros:
                    for erro in erros:
                        log_warning(f"Erro no registro {dado['id']}: {erro}")
                    continue  # Ignora o registro se houver erros

                # Verificação de duplicidade antes de inserir
                if not collection.find_one({'id': dado['id']}):  # Se não existe, insere
                    collection.insert_one(dado)
                    registros_inseridos += 1
                else:
                    log_warning(f"Registro duplicado encontrado e ignorado: {dado}")

            log_info(f"{registros_inseridos} registros inseridos no MongoDB a partir de {arquivo_csv}.")
        else:
            log_error("Coleção não encontrada. Dados não foram inseridos.")
    except FileNotFoundError:
        log_error(f"Arquivo não encontrado: {arquivo_csv}")
    except pd.errors.EmptyDataError:
        log_error(f"O arquivo CSV está vazio: {arquivo_csv}")
    except Exception as e:
        log_error(f"Erro ao processar {arquivo_csv}: {e}")

def processar_csv(diretorio, uri):
    """Verifica e insere dados de arquivos CSV no MongoDB."""
    log_info(f"Iniciando o processamento de arquivos CSV no diretório: {diretorio}")
    arquivos_csv = verificar_csv(diretorio)
    collection = conectar_mongodb(uri)

    for arquivo_csv in arquivos_csv:
        caminho_arquivo = os.path.join(diretorio, arquivo_csv)
        inserir_dados_no_mongodb(caminho_arquivo, collection)
    
    log_info("Processamento de arquivos CSV concluído.")

if __name__ == "__main__":
    uri = "mongodb+srv://blequizit:aYKbkBUB0FPI39UX@dadospessoas.k6sjq.mongodb.net/"
    diretorio = r'C:\Users\Novo Pc de testes\Desktop\NAC_THIAGO'  # Substitua pelo caminho correto do diretório
    log_info("Iniciando o script...")
    processar_csv(diretorio, uri)
    log_info("Script concluído.")
