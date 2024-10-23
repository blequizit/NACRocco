import os
import pandas as pd
from pymongo import MongoClient

# Função para conectar ao MongoDB Atlas com autenticação via URI
def conectar_mongodb():
    # URI fornecida pelo MongoDB Atlas
    uri = "mongodb+srv://blequizit:aYKbkBUB0FPI39UX@dadospessoas.k6sjq.mongodb.net/"
    
    # Conectando ao MongoDB usando a URI
    client = MongoClient(uri)
    
    # Conectar ao banco de dados 'DadosPessoas1' e à coleção 'DadosPessoas01'
    db = client['DadosPessoas1']  # Nome do banco de dados
    collection = db['DadosPessoas01']  # Nome da coleção
    return collection

# Função que verifica se há CSV no diretório e insere no MongoDB
def verificar_e_inserir_csv(diretorio):
    # Verificar se há arquivos CSV no diretório
    arquivos_csv = [f for f in os.listdir(diretorio) if f.endswith('.csv')]
    
    if arquivos_csv:
        # Pegar o primeiro arquivo CSV encontrado
        arquivo_csv = arquivos_csv[0]
        caminho_arquivo = os.path.join(diretorio, arquivo_csv)
        
        # Carregar o arquivo CSV
        print(f"Carregando o arquivo: {arquivo_csv}")
        df = pd.read_csv(caminho_arquivo)
        
        # Conectar ao MongoDB
        collection = conectar_mongodb()

        # Converter o DataFrame para um formato adequado ao MongoDB (lista de dicionários)
        dados = df.to_dict(orient='records')

        # Inserir os dados no MongoDB
        collection.insert_many(dados)
        print(f"Dados inseridos no MongoDB a partir de {arquivo_csv}.")
    else:
        print("Nenhum arquivo CSV encontrado no diretório.")

# Definir o diretório onde o arquivo CSV está localizado
diretorio = r'C:\Users\Novo Pc de testes\Desktop\NAC_THIAGO'  # Substitua pelo caminho correto do diretório

# Executar a função automaticamente
verificar_e_inserir_csv(diretorio)
