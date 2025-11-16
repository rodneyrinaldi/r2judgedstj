# -*- coding: utf-8 -*-
# db_init_dw_schemas.py - Criação das Tabelas FATO e DIMENSIONAIS

import psycopg2
from psycopg2 import sql
import sys

# =================================================================
# 1. CONFIGURAÇÕES E NOMES DE TABELAS DW
# =================================================================
DB_CONFIG = {
    "host": "localhost", # Altere para o host correto
    "port": 5434,
    "dbname": "legal", 
    "user": "admin",
    "password": "admin"
}

# Nomes das Tabelas DW
TABELA_FATO = "fato_julgados_stj"
TABELA_DIM_REF = "dim_referencias_legais"
TABELA_DIM_ASSUNTOS = "dim_assuntos_stj"


# =================================================================
# 2. FUNÇÕES DE CRIAÇÃO DO SCHEMA
# =================================================================

def verificar_criar_banco():
    """ 
    Verifica se o banco de dados existe e o cria, se necessário. 
    (Função duplicada para garantir a independência, mas idealmente
    deve ser fatorada em um módulo comum.)
    """
    try:
        temp_config = DB_CONFIG.copy()
        temp_config["dbname"] = "postgres" 
        
        conn = psycopg2.connect(**temp_config)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (DB_CONFIG["dbname"],)
        )
        if not cursor.fetchone():
            print(f"Banco de dados '{DB_CONFIG['dbname']}' não encontrado. Criando...")
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

        try:
            cursor.execute(create_table_query)
            print(f"Tabela '{nome_tabela}' criada com sucesso.")
        except psycopg2.errors.UndefinedTable as e:
            # Captura erro de chave estrangeira se a tabela referenciada não existir
            print(f"\nERRO: Falha ao criar '{nome_tabela}'. Verifique se as tabelas referenciadas (como '{TABELA_FATO}') existem. Detalhes: {e}")
            sys.exit(1)
    else:
        print(f"Tabela '{nome_tabela}' já existe.")

def criar_tabelas_dw():
    """ Cria as tabelas FATO e DIMENSIONAIS. """
    conn = None
    if not verificar_criar_banco():
        print("Não foi possível continuar sem um banco de dados válido.")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. Tabela FATO (Principal - Tratada)
        print("\n--- 1. Criando Tabela FATO para BI/LLM ---")
        # Layout da FATO, usando tipos tratados (DATE, TEXT) e referenciando a chave primária
        LAYOUT_FATO = [
            {"campo": "id_julgado", "tipo": "INTEGER PRIMARY KEY"}, # Chave primária da FATO
            {"campo": "dt_decisao", "tipo": "DATE"}, 
            {"campo": "dt_publicacao", "tipo": "DATE"}, 
            {"campo": "classe_sigla", "tipo": "VARCHAR(150)"}, 
            {"campo": "orgao_julgador", "tipo": "VARCHAR(50)"}, 
            {"campo": "ministro_relator", "tipo": "VARCHAR(90)"}, 
            {"campo": "resultado_binario", "tipo": "BOOLEAN"}, 
            {"campo": "tema_repetitivo", "tipo": "TEXT"}, 
            {"campo": "ementa_limpa", "tipo": "TEXT"},
            {"campo": "decsiao_teor_limpo", "tipo": "TEXT"},
            {"campo": "tese_juridica_limpa", "tipo": "TEXT"}, 
            {"campo": "acordaos_similares_limpo", "tipo": "TEXT"},
            {"campo": "jurisprudencia_citada_limpa", "tipo": "TEXT"},
            {"campo": "teor_bruto_json", "tipo": "JSONB"}
        ]
        criar_tabela(cursor, TABELA_FATO, LAYOUT_FATO)
        
        # 2. Tabela DIMENSIONAL (Referências Legais) - Depende de TABELA_FATO
        print("\n--- 2. Criando Tabela DIMENSIONAL de Referências Legais ---")
        LAYOUT_DIM_REF = [
            {"campo": "id_ref_legal", "tipo": "SERIAL PRIMARY KEY"},
            {"campo": "id_julgado_fk", "tipo": f"INTEGER REFERENCES {TABELA_FATO} (id_julgado)"}, 
            {"campo": "tipo_norma", "tipo": "VARCHAR(50)"},
            {"campo": "norma_nome", "tipo": "TEXT"},
            {"campo": "artigo_dispositivo", "tipo": "TEXT"}
        ]
        criar_tabela(cursor, TABELA_DIM_REF, LAYOUT_DIM_REF)

        # 3. Tabela DIMENSIONAL (Assuntos/Teses/Termos Auxiliares) - Depende de TABELA_FATO
        print("\n--- 3. Criando Tabela DIMENSIONAL de Assuntos/Teses ---")
        LAYOUT_DIM_ASSUNTOS = [
            {"campo": "id_assunto", "tipo": "SERIAL PRIMARY KEY"},
            {"campo": "id_julgado_fk", "tipo": f"INTEGER REFERENCES {TABELA_FATO} (id_julgado)"}, 
            {"campo": "tipo_assunto", "tipo": "VARCHAR(50)"},
            {"campo": "termo", "tipo": "TEXT"}
        ]
        criar_tabela(cursor, TABELA_DIM_ASSUNTOS, LAYOUT_DIM_ASSUNTOS)

        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao verificar/criar as tabelas do DW: {e}")
        if conn:
            conn.rollback()


# =================================================================
# 3. EXECUÇÃO
# =================================================================
if __name__ == "__main__":
    print("Iniciando a criação da estrutura das tabelas FATO e DIMENSIONAIS...")
    criar_tabelas_dw()
    print("Processo de criação da estrutura DW concluído.")