import os
import pandas as pd
from pymongo import MongoClient, errors
import logging
from datetime import datetime

# Função para criar a pasta de logs e o arquivo de log com a data atual
def configurar_log(diretorio):
    """Cria a pasta de logs e define o arquivo de log com a data atual."""
    # Define o nome do arquivo de log com a data e hora no formato anomesdiahoraminutosegundo
    data_atual = datetime.now().strftime('%Y%m%d%H%M%S')
    nome_arquivo_log = f"log{data_atual}.txt"

    # Caminho da pasta de logs
    caminho_logs = os.path.join(diretorio, 'logs')
    
    # Verifica se a pasta de logs existe, caso não exista, cria
    if not os.path.exists(caminho_logs):
        os.makedirs(caminho_logs)

    # Caminho completo do arquivo de log
    caminho_arquivo_log = os.path.join(caminho_logs, nome_arquivo_log)
    
    # Configuração do logging para escrever no arquivo de log
    logging.basicConfig(
        filename=caminho_arquivo_log,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log_info("Arquivo de log configurado.")

# Funções de log personalizadas
def log_info(message):
    """Log de informações."""
    logging.info(message)

def log_error(message):
    """Log de erros."""
    logging.error(message)

def log_warning(message):
    """Log de avisos."""
    logging.warning(message)

# Função para validar os dados
def validar_dados(dado):
    """Valida os dados do registro e retorna uma lista de erros."""
    erros = []

    if not isinstance(dado['id'], int):
        erros.append(f"ID deve ser um número inteiro, encontrado: {dado['id']}")
    if not dado['nome']:
        erros.append("Nome está faltando.")
    if not dado['email']:
        erros.append("Email está faltando.")
    if not dado['telefone']:
        erros.append("Telefone está faltando.")
    if not dado['data_nascimento']:
        erros.append("Data de nascimento está faltando.")
    else:
        try:
            datetime.strptime(dado['data_nascimento'], '%Y-%m-%d')
        except ValueError:
            erros.append(f"Data de nascimento inválida: {dado['data_nascimento']}")
    if not dado['sexo']:
        erros.append("Sexo está faltando.")
    if not dado['cidade']:
        erros.append("Cidade está faltando.")
    if not dado['estado']:
        erros.append("Estado está faltando.")

    return erros

# Função para conectar ao MongoDB
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

# Função para verificar CSVs no diretório
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

# Função para inserir dados no MongoDB
def inserir_dados_no_mongodb(arquivo_csv, collection):
    """Insere dados de um arquivo CSV no MongoDB."""
    log_info(f"Iniciando a inserção de dados do arquivo: {arquivo_csv}")
    try:
        df = pd.read_csv(arquivo_csv)
        dados = df.to_dict(orient='records')

        log_info(f"{len(dados)} registros lidos do arquivo CSV.")

        if collection is not None:
            registros_inseridos = 0
            for dado in dados:
                erros = validar_dados(dado)
                if erros:
                    for erro in erros:
                        log_warning(f"Erro no registro {dado['id']}: {erro}")
                    continue

                if not collection.find_one({'id': dado['id']}):
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

# Função para processar os arquivos CSV
def processar_csv(diretorio, uri):
    """Verifica e insere dados de arquivos CSV no MongoDB."""
    log_info(f"Iniciando o processamento de arquivos CSV no diretório: {diretorio}")
    arquivos_csv = verificar_csv(diretorio)
    collection = conectar_mongodb(uri)

    for arquivo_csv in arquivos_csv:
        caminho_arquivo = os.path.join(diretorio, arquivo_csv)
        inserir_dados_no_mongodb(caminho_arquivo, collection)
    
    log_info("Processamento de arquivos CSV concluído.")

# Execução do script principal
if __name__ == "__main__":
    uri = "mongodb+srv://blequizit:aYKbkBUB0FPI39UX@dadospessoas.k6sjq.mongodb.net/"
    diretorio = r'C:\Users\Novo Pc de testes\Desktop\NAC_THIAGO'  # Substitua pelo caminho correto do diretório
    configurar_log(diretorio)  # Configura o log antes de iniciar o processamento
    log_info("Iniciando o script...")
    processar_csv(diretorio, uri)
    log_info("Script concluído.")
