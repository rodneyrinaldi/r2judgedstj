# -*- coding: utf-8 -*-
# data/db_insert.py - Rotina de Inserção de Dados em Lote (Batch Insert)
# Espera que o lote de dados já esteja tratado e padronizado pelo script de ETL.

import psycopg2
from psycopg2 import sql, extras
import sys
import os 
from typing import List, Dict, Any

# --- Configurações do banco de dados ---
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "legal",
    "user": "admin",
    "password": "admin"
}

# Nome da tabela de Staging (área de carregamento)
TABLE_NAME = "judged"

def inserir_dados_lote(lote_de_dados: List[Dict[str, Any]]):
    """
    Insere uma lista de registros tratados usando psycopg2.extras.execute_values 
    para alta performance (Batch Insert) na tabela de staging 'judged'.
    
    PREMISSA: O lote_de_dados recebido pelo ETL (process_data.py) já deve ter suas
    chaves padronizadas para os nomes das colunas do banco de dados, incluindo 
    a chave primária de origem como 'id_origem'.
    
    Recebe: lote_de_dados (list de dicts)
    """
    if not lote_de_dados:
        return

    conn = None
    try:
        # 1. Obtém os nomes das colunas (chaves do dicionário) do primeiro registro.
        # Estas chaves devem corresponder EXATAMENTE aos nomes das colunas no DB.
        colunas_db = list(lote_de_dados[0].keys())
        
        # 2. Cria a lista de tuplas de valores (garantindo a ordem)
        valores_tuplas = []
        for dados_tratados in lote_de_dados:
            # Extrai os valores na ordem das chaves, garantindo a compatibilidade com a ordem das colunas_db.
            tupla_valores = tuple(dados_tratados[c] for c in colunas_db)
            valores_tuplas.append(tupla_valores)

        # Conecta ao banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Monta o comando SQL para inserção usando execute_values
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, colunas_db))
        )

        # 3. Executa o comando de inserção em lote (transacional)
        extras.execute_values(cursor, insert_query, valores_tuplas, page_size=1000)
        
        conn.commit()
        cursor.close()

    except Exception as e:
        if conn:
            conn.rollback() # Garante que todo o lote seja desfeito em caso de erro
        
        # Imprime o erro no stderr e re-raise para notificar o script principal 
        # (ETL) sobre a falha de persistência.
        print(f"\nERRO FATAL DE DB: Falha ao inserir lote ({len(lote_de_dados)} registros): {e}", file=sys.stderr)
        raise # Essencial para que o script principal possa tratar o arquivo JSON como falho.
        
    finally:
        if conn:
            conn.close()

# --- Códigos Auxiliares que podem ser usados em outros sistemas ---

def testar_conexao():
    """
    Função auxiliar para testar a conectividade com o banco de dados.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com PostgreSQL estabelecida com sucesso.")
        return True
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}", file=sys.stderr)
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    testar_conexao()
