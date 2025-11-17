# -*- coding: utf-8 -*-
# db_init_dw_schemas_corrected.py - Criação das Tabelas FATO e DIMENSIONAIS com correção da chave única.

import psycopg2
from psycopg2 import sql
import sys

# =================================================================
# 1. CONFIGURAÇÕES E NOMES DE TABELAS DW
# =================================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "legal", 
    "user": "admin",
    "password": "admin"
}

# Nomes das Tabelas DW
TABELA_FATO = "fato_julgados_stj"
TABELA_DIM_REF = "dim_referencias_legais"
TABELA_DIM_ASSUNTOS = "dim_assuntos_stj"

# Lista de tabelas DW na ORDEM REVERSA para DROP (Dimensões primeiro, Fato por último)
TABELAS_DW = [TABELA_DIM_ASSUNTOS, TABELA_DIM_REF, TABELA_FATO]


# =================================================================
# 2. FUNÇÕES DE CRIAÇÃO/EXCLUSÃO DO SCHEMA
# =================================================================

def verificar_criar_banco():
    """ Verifica se o banco de dados existe e o cria, se necessário. """
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

def dropar_tabelas(cursor, tabelas_a_dropar):
    """ Exclui tabelas usando DROP TABLE IF EXISTS. """
    print("\n--- 0. Excluindo Tabelas Existentes (Drop) ---")
    for nome_tabela in tabelas_a_dropar:
        print(f"Verificando e excluindo tabela: '{nome_tabela}'...")
        try:
            # Usando CASCADE para forçar a remoção de dependências (chaves estrangeiras)
            drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(nome_tabela))
            cursor.execute(drop_query)
            print(f"  > Tabela '{nome_tabela}' excluída (se existia).")
        except Exception as e:
            print(f"  > Erro ao tentar excluir a tabela '{nome_tabela}': {e}")
            
def criar_tabela(cursor, nome_tabela, layout):
    """ Função auxiliar para criar qualquer tabela com base em um layout. """
    print(f"Criando tabela: '{nome_tabela}'...")
    
    # 1. Constrói o CREATE TABLE sem as restrições UNIQUE compostas
    create_table_query = sql.SQL("CREATE TABLE {} (").format(sql.Identifier(nome_tabela))
    columns = [
        sql.SQL("{} {}").format(sql.Identifier(col["campo"]), sql.SQL(col["tipo"]))
        for col in layout
    ]
    create_table_query += sql.SQL(", ").join(columns)
    create_table_query += sql.SQL(");")

    try:
        cursor.execute(create_table_query)
        print(f"  > Tabela '{nome_tabela}' criada com sucesso.")
    except Exception as e:
        print(f"\nERRO: Falha ao criar '{nome_tabela}'. Detalhes: {e}")
        raise

def adicionar_restricoes(cursor):
    """ Adiciona as restrições de unicidade compostas (UNIQUE INDEX) necessárias para o UPSERT (ON CONFLICT). """
    print("\n--- 4. Adicionando Restrições de Unicidade Composta (UNIQUE CONSTRAINTS) ---")
    
    restricoes = [
        # DIM_REFERENCIAS_LEGAIS: Chave de Conflito usada no ETL
        (TABELA_DIM_REF, "UQ_DIM_REF", ["id_julgado_fk", "tipo_norma", "norma_nome", "artigo_dispositivo"]),
        
        # DIM_ASSUNTOS_STJ: Chave de Conflito usada no ETL
        (TABELA_DIM_ASSUNTOS, "UQ_DIM_ASSUNTOS", ["id_julgado_fk", "tipo_assunto", "termo"]),
    ]
    
    for tabela, nome_restricao, colunas in restricoes:
        colunas_str = sql.SQL(", ").join(map(sql.Identifier, colunas))
        
        alter_query = sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});").format(
            sql.Identifier(tabela),
            sql.Identifier(nome_restricao),
            colunas_str
        )
        try:
            cursor.execute(alter_query)
            print(f"  > Restrição UNIQUE '{nome_restricao}' adicionada à tabela '{tabela}'.")
        except Exception as e:
            print(f"  > ERRO ao adicionar restrição '{nome_restricao}' em '{tabela}': {e}")
            raise


def criar_tabelas_dw():
    """ Exclui e Cria as tabelas FATO e DIMENSIONAIS. """
    conn = None
    if not verificar_criar_banco():
        print("Não foi possível continuar sem um banco de dados válido.")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 0. EXCLUSÃO PRÉVIA
        dropar_tabelas(cursor, TABELAS_DW)
        
        # 1. Tabela FATO (Principal - COM PRIMARY KEY e UNIQUE na chave natural)
        print("\n--- 1. Recriando Tabela FATO ---")
        LAYOUT_FATO = [
            # ID_JULGADO é a Surrogate Key (PK)
            {"campo": "id_julgado", "tipo": "SERIAL PRIMARY KEY"}, 
            # id_origem_natural é a Business Key/Chave Natural (UNIQUE) - OBRIGATÓRIA para UPSERT
            {"campo": "id_origem_natural", "tipo": "VARCHAR(50) UNIQUE NOT NULL"}, 
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
        print("\n--- 2. Recriando Tabela DIMENSIONAL de Referências Legais ---")
        LAYOUT_DIM_REF = [
            {"campo": "id_ref_legal", "tipo": "SERIAL PRIMARY KEY"},
            # FOREIGN KEY: Referencia a chave substituta da FATO (id_julgado)
            {"campo": "id_julgado_fk", "tipo": f"INTEGER REFERENCES {TABELA_FATO} (id_julgado) NOT NULL"}, 
            {"campo": "tipo_norma", "tipo": "VARCHAR(50) NOT NULL"},
            {"campo": "norma_nome", "tipo": "TEXT NOT NULL"},
            {"campo": "artigo_dispositivo", "tipo": "TEXT NOT NULL"}
            # A restrição UNIQUE será adicionada na seção 4
        ]
        criar_tabela(cursor, TABELA_DIM_REF, LAYOUT_DIM_REF)

        # 3. Tabela DIMENSIONAL (Assuntos/Teses/Termos Auxiliares) - Depende de TABELA_FATO
        print("\n--- 3. Recriando Tabela DIMENSIONAL de Assuntos/Teses ---")
        LAYOUT_DIM_ASSUNTOS = [
            {"campo": "id_assunto", "tipo": "SERIAL PRIMARY KEY"},
            # FOREIGN KEY: Referencia a chave substituta da FATO (id_julgado)
            {"campo": "id_julgado_fk", "tipo": f"INTEGER REFERENCES {TABELA_FATO} (id_julgado) NOT NULL"}, 
            {"campo": "tipo_assunto", "tipo": "VARCHAR(50) NOT NULL"},
            {"campo": "termo", "tipo": "TEXT NOT NULL"}
            # A restrição UNIQUE será adicionada na seção 4
        ]
        criar_tabela(cursor, TABELA_DIM_ASSUNTOS, LAYOUT_DIM_ASSUNTOS)

        # 4. ADICIONA AS RESTRIÇÕES UNIQUE (UPSERT)
        adicionar_restricoes(cursor)

        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro fatal ao executar a criação das tabelas do DW: {e}")
        if conn:
            conn.rollback()


# =================================================================
# 3. EXECUÇÃO
# =================================================================
if __name__ == "__main__":
    print("Iniciando a exclusão e recriação da estrutura das tabelas FATO e DIMENSIONAIS...")
    criar_tabelas_dw()
    print("Processo de inicialização da estrutura DW concluído.")