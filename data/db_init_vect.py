# criar_schema_vetorial.py

import psycopg2
from psycopg2 import sql

# =================================================================
# 1. CONFIGURAÇÕES E NOMES DE TABELAS (AJUSTE AQUI)
# =================================================================
DB_CONFIG = {
    "host": "localhost",  # Altere para o host correto
    "port": 5432,         
    "dbname": "legal",    
    "user": "admin",      
    "password": "admin"   
}

# Tabela Vetorial de Destino
TABELA_VETORIAL = "DIM_VETORES_LLM"
TABELA_FATO = "FATO_JULGADOS_STJ"

# Dimensão do vetor (Depende do modelo de embedding. 
# Usamos 768 como um exemplo comum, como para o Cohere v3 ou modelos BERT.)
DIMENSAO_VETOR = 768 

# =================================================================
# 2. FUNÇÕES DE CRIAÇÃO DO SCHEMA VETORIAL
# =================================================================

def ativar_extensao_pgvector(cursor):
    """ Ativa a extensão pgvector no banco de dados. """
    try:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgvector;")
        print("Extensão 'pgvector' verificada/ativada com sucesso.")
    except Exception as e:
        print(f"ERRO ao ativar a extensão pgvector. Certifique-se de que o pacote 'postgresql-16-pgvector' ou similar está instalado e o Postgis está configurado. Erro: {e}")
        # Recomenda-se parar a execução se a extensão falhar.

def criar_tabela_vetorial(cursor):
    """ Cria a tabela para armazenar os embeddings (vetores). """
    try:
        # A coluna 'embedding' armazena o vetor real
        create_table_query = sql.SQL(f"""
            CREATE TABLE IF NOT EXISTS {TABELA_VETORIAL} (
                ID_VETOR SERIAL PRIMARY KEY,
                ID_JULGADO_FK BIGINT REFERENCES {TABELA_FATO} (ID_JULGADO),
                TEXTO_FONTE TEXT NOT NULL,           -- Texto completo que foi vetorizado (Ementa + Decisão)
                TIPO_FONTE VARCHAR(50) NOT NULL,     -- Ex: 'ACORDAO_COMPLETO'
                EMBEDDING VECTOR({DIMENSAO_VETOR})   -- O vetor gerado
            );
        """)
        cursor.execute(create_table_query)
        print(f"Tabela '{TABELA_VETORIAL}' criada com sucesso (Dimensão: {DIMENSAO_VETOR}).")

        # Cria um índice eficiente para buscas vetoriais (ANN - Approximate Nearest Neighbor)
        # O tipo IVFFlat é comum para começar; use HNSW para desempenho superior em alta escala.
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_embedding_ivfflat 
            ON {TABELA_VETORIAL} 
            USING IVFFLAT (embedding vector_l2_ops)
            WITH (lists = 100);
        """)
        print("Índice IVFFLAT criado para buscas vetoriais.")

    except Exception as e:
        print(f"ERRO ao criar a tabela vetorial ou o índice: {e}")


def executar_criacao_schema_vetorial():
    """ Executa a sequência de criação de extensão e tabelas vetoriais. """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        ativar_extensao_pgvector(cursor)
        criar_tabela_vetorial(cursor)

        conn.commit()
        cursor.close()
        conn.close()
        print("\nConfiguração Vetorial do Data Warehouse concluída.")
        
    except Exception as e:
        print(f"Erro geral no processo de criação do schema vetorial: {e}")
        if conn:
            conn.rollback()

# =================================================================
# 3. EXECUÇÃO
# =================================================================
if __name__ == "__main__":
    # NOTA: Assumimos que o banco de dados 'legal' já foi criado pelo 'criar_schema_dw.py'
    executar_criacao_schema_vetorial()