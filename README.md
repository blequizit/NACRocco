# SKYTECH Data Pipeline

### Descrição
Este projeto foi desenvolvido como parte da disciplina **ARCHITECTURE ANALYTICS & NOSQL** do curso de **Data Science**. O objetivo é criar um pipeline de dados para uma empresa de médio porte que deseja modernizar a maneira de armazenar, processar e visualizar informações sobre clientes. 

Para isso, o projeto implementa um fluxo automatizado de inserção de dados com **Apache Airflow** para orquestração de tarefas, **MongoDB** para armazenamento de dados, e suporte para visualização em **Power BI**. Com essa solução, é possível transformar dados brutos em relatórios dinâmicos, otimizando o processo de tomada de decisão da empresa.

---

## Índice
- [Sobre o Projeto](#sobre-o-projeto)
- [Estrutura dos Dados](#estrutura-dos-dados)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Instalação e Execução](#instalação-e-execução)
- [Funcionalidades](#funcionalidades)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Referências](#referências)
- [Autores](#autores)

---

## Sobre o Projeto
Neste projeto, atuamos como analistas de dados para criar uma estrutura de coleta e análise de informações de clientes. Antes, esses dados eram armazenados em planilhas e documentos, dificultando a extração de insights. Agora, com o uso de tecnologias como **MongoDB** (NoSQL), **Apache Airflow**, e **Power BI** (em implementação), a empresa poderá processar e visualizar dados de forma mais eficiente.

## Estrutura dos Dados
Abaixo estão os campos armazenados no banco de dados:
- `id`: Identificador único do cliente.
- `nome`: Nome do cliente.
- `email`: Email do cliente.
- `telefone`: Telefone do cliente.
- `data_nascimento`: Data de nascimento do cliente (YYYY-MM-DD).
- `sexo`: Sexo do cliente.
- `cidade`: Cidade de residência do cliente.
- `estado`: Estado de residência do cliente.

### Exemplo de Arquivo CSV
```csv
id,nome,email,telefone,data_nascimento,sexo,cidade,estado
1,Alice Silva,alice.silva@email.com,(11) 98765-4321,1990-05-12,Feminino,São Paulo,SP
2,Bob Oliveira,bob.oliveira@email.com,(21) 97654-3210,1985-08-23,Masculino,Rio de Janeiro,RJ
...
```

## Tecnologias Utilizadas
- **Apache Airflow** - Orquestração de tarefas para gerenciar o pipeline de dados.
- **MongoDB** - Banco de dados NoSQL para armazenamento flexível dos dados dos clientes.
- **Python** - Linguagem principal para o desenvolvimento dos scripts de pipeline e processamento de dados.
- **Docker** - Ferramenta de contêinerização para criar e gerenciar ambientes isolados.
- **Power BI** - Ferramenta de visualização de dados (em desenvolvimento).

## Instalação e Execução
Para configurar e executar o projeto, siga o passo a passo disponível no arquivo `Script Para Funcionar.txt`, que contém todas as instruções de instalação. Certifique-se de ter o **Docker Desktop** instalado na máquina para gerenciar os contêineres corretamente.

1. Clone o repositório:
   ```bash
   git clone https://github.com/blequizit/NACRocco.git

## Funcionalidades
- **Pipeline Automatizado**: Pipeline desenvolvido em Apache Airflow para orquestrar a carga de dados.
- **Armazenamento em MongoDB**: Banco de dados NoSQL para armazenamento flexível e escalável dos dados dos clientes.
- **Geração de Logs**: A DAG gera um arquivo de log para acompanhar o processo e identificar erros.
- **Tratamento de Erros**: Mecanismos de tratamento de erros implementados na DAG, garantindo uma execução robusta.
- **Visualização em Power BI**: Em desenvolvimento, o Power BI permitirá uma visualização e análise dinâmica dos dados armazenados.

## Estrutura do Repositório
```plaintext
├── README.md                     # Documentação do repositório
├── dados/                        # Pasta para adicionar o arquivo CSV com dados dos clientes
│   └── dados.csv                 # Arquivo CSV com os dados de exemplo
├── dags/                         # Pasta contendo o script .py da DAG para inserção de dados
│   └── DAG_Script.py             # Script DAG de processamento dos dados
├── Script.py                     # Script inicial de processamento
├── arquitetura-nac.pdf           # Desenho da arquitetura do projeto
├── docker-compose.yml            # Arquivo para configuração do contêiner Docker
└── Script Para Funcionar.txt     # Passo a passo para execução do projeto
```

## Referências
As principais referências utilizadas para o desenvolvimento deste projeto incluem:
- Aulas da disciplina **ARCHITECTURE ANALYTICS & NOSQL**, ministrada pelo professor **Thiago Matos Rocco**.
- Documentação oficial do [Apache Airflow](https://airflow.apache.org/).
- Documentação oficial do [MongoDB](https://www.mongodb.com/).

## Futuras Implementações
**Integração Completa com Power BI**: Conectar o MongoDB ao Power BI para visualização completa e relatórios dinâmicos dos dados dos clientes.

## Autores
Projeto desenvolvido pelo grupo **SKYTECH**:
- **Anna Paula Martins** - RM558152
- **Eduardo Ferreira** - RM555610 (Representante)
- **Guilherme Panfiete** - RM557758
- **Victória Nogueira** - RM558629
