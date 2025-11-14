# -*- coding: utf-8 -*-
# db_init.py - Programa de Criação e Otimização da Estrutura do Data Warehouse

import psycopg2
from psycopg2 import sql

# =================================================================
# 1. CONFIGURAÇÕES E NOMES DE TABELAS
# =================================================================
DB_CONFIG = {
    "host": "localhost", # Altere para o host correto
    "port": 5432,
    "dbname": "legal", 
    "user": "admin",
    "password": "admin"
}

# Nomes das Tabelas (Padronizados para minúsculas)
TABELA_ORIGEM = "judged" 
TABELA_FATO = "fato_julgados_stj"
TABELA_DIM_REF = "dim_referencias_legais"
TABELA_DIM_ASSUNTOS = "dim_assuntos_stj"

# O layout está baseado na ESPECIFICAÇÃO DE DIMENSIONAMENTO fornecida.
# A tabela de origem (Staging) usa os tipos de dados sugeridos no relatório original
# para garantir que todos os dados brutos sejam carregados.
# id_dw é a chave substituta (surrogate key) para o DW.
LAYOUT_ORIGEM = [
    {"campo": "id_dw", "tipo": "BIGSERIAL PRIMARY KEY"}, 
    {"campo": "id_origem", "tipo": "VARCHAR(50)"}, # ID original do registro.
    
    # Numéricos (Tipos nativos com base nos valores máximos)
    {"campo": "numeroProcesso", "tipo": "VARCHAR(50)"},
    {"campo": "numeroRegistro", "tipo": "VARCHAR(50)"},

    # Strings (VARCHARs com tamanho sugerido)
    {"campo": "Obs", "tipo": "VARCHAR(60)"}, # Adicionado conforme especificação
    {"campo": "dataDecisao", "tipo": "VARCHAR(50)"}, # Alterado de DATE para VARCHAR(50) (Recomendado para Staging)
    {"campo": "dataPublicacao", "tipo": "VARCHAR(300)"}, # Alterado de DATE para VARCHAR(300) (Recomendado para Staging)
    {"campo": "ministroRelator", "tipo": "VARCHAR(90)"}, # Ajustado para VARCHAR(90)
    {"campo": "nomeOrgaoJulgador", "tipo": "VARCHAR(50)"}, # Ajustado para VARCHAR(50)
    {"campo": "tipoDeDecisao", "tipo": "VARCHAR(50)"}, # Ajustado para VARCHAR(50)
    {"campo": "siglaClasse", "tipo": "VARCHAR(150)"}, # Ajustado para VARCHAR(150)
    
    # Campos de Texto Longo (TEXT) conforme sugestão
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
    {"campo": "tema", "tipo": "TEXT"}, # Alterado de VARCHAR(400) para TEXT
]

# =================================================================
# 2. FUNÇÕES DE CRIAÇÃO DO SCHEMA
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
    except Exception as e:
        print(f"Erro ao verificar/criar o banco de dados: {e}")

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

def criar_tabelas_dw():
    """ Cria a tabela de origem (judged) e as tabelas FATO e DIMENSIONAIS. """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Tabela de Origem (Staging) - AGORA COMPATÍVEL COM A ESPECIFICAÇÃO
        print("\n--- 1. Criando Tabela de Origem (Staging) ---")
        criar_tabela(cursor, TABELA_ORIGEM, LAYOUT_ORIGEM)
        
        # 2. Tabela FATO (Principal - Tratada)
        print("\n--- 2. Criando Tabela FATO para BI/LLM ---")
        # Layout da FATO, usando tipos tratados (DATE, TEXT) e referenciando a chave primária
        LAYOUT_FATO = [
            {"campo": "id_julgado", "tipo": "INTEGER PRIMARY KEY"}, # Chave primária da FATO (ref id_origem)
            {"campo": "dt_decisao", "tipo": "DATE"}, # Armazena a data tratada
            {"campo": "dt_publicacao", "tipo": "DATE"}, # Armazena a data tratada
            {"campo": "classe_sigla", "tipo": "VARCHAR(150)"}, # Ajustado
            {"campo": "orgao_julgador", "tipo": "VARCHAR(50)"}, # Ajustado
            {"campo": "ministro_relator", "tipo": "VARCHAR(90)"}, # Ajustado
            {"campo": "resultado_binario", "tipo": "BOOLEAN"}, 
            {"campo": "tema_repetitivo", "tipo": "TEXT"}, # Ajustado para TEXT
            {"campo": "ementa_limpa", "tipo": "TEXT"},
            {"campo": "decsiao_teor_limpo", "tipo": "TEXT"},
            {"campo": "tese_juridica_limpa", "tipo": "TEXT"}, # Campo 'tese' na FATO
            {"campo": "acordaos_similares_limpo", "tipo": "TEXT"},
            {"campo": "jurisprudencia_citada_limpa", "tipo": "TEXT"},
            {"campo": "teor_bruto_json", "tipo": "JSONB"} # Para guardar o JSON original (opcional, mas útil)
        ]
        criar_tabela(cursor, TABELA_FATO, LAYOUT_FATO)
        
        # 3. Tabela DIMENSIONAL (Referências Legais)
        print("\n--- 3. Criando Tabela DIMENSIONAL de Referências Legais ---")
        # id_julgado_fk usa INTEGER para referenciar id_julgado da FATO (INTEGER)
        LAYOUT_DIM_REF = [
            {"campo": "id_ref_legal", "tipo": "SERIAL PRIMARY KEY"},
            {"campo": "id_julgado_fk", "tipo": f"INTEGER REFERENCES {TABELA_FATO} (id_julgado)"}, 
            {"campo": "tipo_norma", "tipo": "VARCHAR(50)"},
            {"campo": "norma_nome", "tipo": "TEXT"},
            {"campo": "artigo_dispositivo", "tipo": "TEXT"}
        ]
        criar_tabela(cursor, TABELA_DIM_REF, LAYOUT_DIM_REF)

        # 4. Tabela DIMENSIONAL (Assuntos/Teses/Termos Auxiliares)
        print("\n--- 4. Criando Tabela DIMENSIONAL de Assuntos/Teses ---")
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
    print("Iniciando a criação e verificação da estrutura do Data Warehouse...")
    verificar_criar_banco()
    criar_tabelas_dw()
    print("Processo de criação de estrutura concluído.")
