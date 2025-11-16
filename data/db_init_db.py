# -*- coding: utf-8 -*-
# db_init_staging.py - Criação da Tabela de Staging (judged)

import psycopg2
from psycopg2 import sql
import sys

# =================================================================
# 1. CONFIGURAÇÕES E LAYOUT DA TABELA DE STAGING
# =================================================================
DB_CONFIG = {
    "host": "localhost", # Altere para o host correto
    "port": 5434,
    "dbname": "legal", 
    "user": "admin",
    "password": "admin"
}

# Nome da Tabela de Origem/Staging
TABELA_ORIGEM = "judged" 

# Layout da Tabela de Staging (judged)
# id_dw é a chave substituta (surrogate key) para o DW.
LAYOUT_ORIGEM = [
    {"campo": "id_dw", "tipo": "BIGSERIAL PRIMARY KEY"}, 
    {"campo": "id_origem", "tipo": "VARCHAR(50)"}, 
    
    # Numéricos/Strings (Tipos nativos para Staging)
    {"campo": "numeroProcesso", "tipo": "VARCHAR(50)"},
    {"campo": "numeroRegistro", "tipo": "VARCHAR(50)"},

    # Strings (VARCHARs com tamanho sugerido)
    {"campo": "Obs", "tipo": "VARCHAR(60)"}, 
    {"campo": "dataDecisao", "tipo": "VARCHAR(50)"}, 
    {"campo": "dataPublicacao", "tipo": "VARCHAR(300)"}, 
    {"campo": "ministroRelator", "tipo": "VARCHAR(90)"}, 
    {"campo": "nomeOrgaoJulgador", "tipo": "VARCHAR(50)"}, 
    {"campo": "tipoDeDecisao", "tipo": "VARCHAR(50)"}, 
    {"campo": "siglaClasse", "tipo": "VARCHAR(150)"}, 
    
    # Campos de Texto Longo (TEXT)
    {"campo": "descricaoClasse", "tipo": "TEXT"},
    {"campo": "ementa", "tipo": "TEXT"},
    {"campo": "decisao", "tipo": "TEXT"},
    {"campo": "jurisprudenciaCitada", "tipo": "TEXT"},
    {"campo": "informacoesComplementares", "tipo": "TEXT"},
    {"campo": "notas", "tipo": "TEXT"},
    {"campo": "termosAuxiliares", "tipo": "TEXT"},
    {"campo": "teseJuridica", "tipo": "TEXT"},
    {"campo": "referenciasLegislativas", "tipo": "TEXT"},
    {"campo": "acordaosSimilares", "tipo": "TEXT"},
    {"campo": "tema", "tipo": "TEXT"}, 
]

# =================================================================
# 2. FUNÇÕES DE CRIAÇÃO DO SCHEMA (REDUZIDAS)
# =================================================================

def verificar_criar_banco():
    """ Verifica se o banco de dados existe e o cria, se necessário. """
    try:
        temp_config = DB_CONFIG.copy()
        # Conecta ao banco 'postgres' padrão para criar o banco de dados principal
        temp_config["dbname"] = "postgres" 
        
        conn = psycopg2.connect(**temp_config)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG["dbname"],)
        )
        if not cursor.fetchone():
            print(f"Banco de dados '{DB_CONFIG['dbname']}' não encontrado. Criando...")
            # Usa sql.Identifier para segurança no nome do DB
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_CONFIG["dbname"])))
        else:
            print(f"Banco de dados '{DB_CONFIG['dbname']}' já existe.")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao verificar/criar o banco de dados: {e}")
        return False

def criar_tabela(cursor, nome_tabela, layout):
    """ Função auxiliar para criar qualquer tabela com base em um layout. """
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        )
        """,
        (nome_tabela,)
    )
    
    if not cursor.fetchone()[0]:
        print(f"Tabela '{nome_tabela}' não encontrada. Criando...")
        
        create_table_query = sql.SQL("CREATE TABLE {} (").format(sql.Identifier(nome_tabela))
        # Mapeia o layout para comandos de coluna (Nome Tipo)
        columns = [
            sql.SQL("{} {}").format(sql.Identifier(col["campo"]), sql.SQL(col["tipo"]))
            for col in layout
        ]
        create_table_query += sql.SQL(", ").join(columns)
        create_table_query += sql.SQL(");")

        cursor.execute(create_table_query)
        print(f"Tabela '{nome_tabela}' criada com sucesso.")
    else:
        print(f"Tabela '{nome_tabela}' já existe.")

def criar_tabela_staging():
    """ Cria a tabela de origem (judged). """
    conn = None
    if not verificar_criar_banco():
        print("Não foi possível continuar sem um banco de dados válido.")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Tabela de Origem (Staging)
        print("\n--- 1. Criando Tabela de Origem (Staging: judged) ---")
        criar_tabela(cursor, TABELA_ORIGEM, LAYOUT_ORIGEM)
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao criar a tabela de Staging: {e}")
        if conn:
            conn.rollback()


# =================================================================
# 3. EXECUÇÃO
# =================================================================
if __name__ == "__main__":
    print("Iniciando a criação da tabela de Staging...")
    criar_tabela_staging()
    print("Processo de criação da tabela de Staging concluído.")