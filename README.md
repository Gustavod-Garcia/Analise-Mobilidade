# Análise de Mobilidade Urbana - SPTrans

Este projeto analisa dados públicos de GTFS da SPTrans, carrega-os em um banco de dados PostgreSQL e apresenta as descobertas em um dashboard interativo feito com Streamlit.

## Objetivo

Desenvolver conhecimento de Data Science investigando as principais linhas de onibus e caminhos percorridos na capital paulista.  

## Como Executar o Projeto

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/seu-repositorio.git](https://github.com/seu-usuario/seu-repositorio.git)
    cd seu-repositorio
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    python3.12 -m venv venv
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configuração do Banco de Dados:**
    * Certifique-se de ter o PostgreSQL rodando.
    * Crie um banco de dados (ex: `CREATE DATABASE mobility_sptrans;`).
    * Crie um arquivo `.env` na raiz do projeto (veja `.env.example` se tiver um) com suas credenciais do banco.

5.  **Baixe os Dados:**
    (Rode o script de download ou acesse o link para dowload manual)
    ```bash
    python src/download_sptrans.py
    ```

6.  **Carregue os Dados (ETL):**
    (Primeiro, execute o schema para criar as tabelas)
    ```bash
    psql -d mobility_sptrans -f sql/schema.sql
    ```
    (Agora, rode o script de ETL)
    ```bash
    python src/etl.py
    ```

7.  **Instale o Streamlit:**
    ```bash
    pip install streamlit
    ```

7.  **Execute o Dashboard:**
    ```bash
    streamlit run dashboard.py
    ```
