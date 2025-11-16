# -*- coding: utf-8 -*-
# etl_vetorial_processo_atualizado.py - Processo ETL para Geração e Carregamento de Embeddings
# Adaptado para a nova infraestrutura de BANCO DUPLO (SQL Fonte e Vetorial Destino)

import psycopg2
import json
import random
import time
import os
import sys
import logging
from typing import List, Dict, Any

# =================================================================
# 1. CONFIGURAÇÕES E VARIÁVEIS GLOBAIS (AJUSTADAS PARA A NOVA INFRA)
# =================================================================

# 🛑 CONFIGURAÇÃO DO BANCO DE DADOS SQL/FATO (Fonte de Dados)
DB_CONFIG_SQL_SOURCE = {
    "host": "localhost",
    "port": 5434,         # Porta externa mapeada do judged_db (SQL)
    "dbname": "legal",    
    "user": "admin",
    "password": "admin"
}

# 🛑 CONFIGURAÇÃO DO BANCO DE DADOS VETORIAL (Destino dos Embeddings)
DB_CONFIG_VECTOR_TARGET = {
    "host": "localhost",
    "port": 5433,         # Porta externa mapeada do judged_llm_db (Vetorial)
    "dbname": "vector_storage", # Nome do DB interno no serviço judged_llm_db
    "user": "admin",
    "password": "admin"
}

# Tabela Fonte de Texto (no judged_db)
TABELA_FONTE = "FATO_JULGADOS_STJ"
# Tabela Destino de Vetores (no judged_llm_db)
TABELA_VETORIAL = "DIM_VETORES_LLM"

# Deve ser a mesma dimensão configurada em 'criar_schema_vetorial.py'
DIMENSAO_VETOR = 768 

# Configurações de Log
PASTA_BASE = r"D:\Sincronizado\tecnologia\data\stj-postgres-llm" 
# PASTA_BASE = os.path.join(os.getcwd(), "dw_vetorial_logs") 
LOG_FILE_NAME = "dw_vetorial_status.log"
LOG_FILE = "" 
logger = logging.getLogger(__name__)

def _setup_environment(base_path: str):
    """ 
    Configura a PASTA_BASE global, cria a estrutura de pastas e configura o logger
    para salvar o log de status DENTRO da pasta base.
    """
    global PASTA_BASE, LOG_FILE
    
    PASTA_BASE = base_path
    
    if not os.path.exists(PASTA_BASE):
        try:
            os.makedirs(PASTA_BASE)
        except OSError as e:
            print(f"Erro Crítico: Não foi possível criar a pasta base '{PASTA_BASE}': {e}")
            sys.exit(1)
            
    LOG_FILE = os.path.join(PASTA_BASE, LOG_FILE_NAME)

    while logger.handlers:
        logger.handlers.pop()
    
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    
    file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info("-" * 80)
    logger.info("Ambiente ETL Vetorial configurado.")
    logger.info(f"Fonte SQL: {DB_CONFIG_SQL_SOURCE['host']}:{DB_CONFIG_SQL_SOURCE['port']}/{DB_CONFIG_SQL_SOURCE['dbname']}")
    logger.info(f"Destino Vetorial: {DB_CONFIG_VECTOR_TARGET['host']}:{DB_CONFIG_VECTOR_TARGET['port']}/{DB_CONFIG_VECTOR_TARGET['dbname']}")
    logger.info("-" * 80)


# =================================================================
# 2. FUNÇÕES DE TRANSFORMAÇÃO (Embedding) - SEM MUDANÇAS
# =================================================================
def gerar_embedding(texto: str) -> List[float]:
    """ Função simulada para geração de vetor. """
    if not texto:
        return [0.0] * DIMENSAO_VETOR
    
    # --- SIMULADOR DE EMBEDDING ---
    random.seed(hash(texto) % 1000) 
    vector = [random.uniform(-0.1, 0.1) for _ in range(DIMENSAO_VETOR)]
    time.sleep(0.05) 
    # --- FIM SIMULADOR ---
    
    return vector

def criar_registro_vetorial(registro_fato: Dict[str, Any]) -> Dict[str, Any] | None:
    """ Concatena o texto, gera o embedding e prepara o registro para a tabela vetorial. """
    
    id_julgado = registro_fato.get("id_julgado")
    
    texto_embedding = (
        f"Ementa: {registro_fato.get('ementa_limpa', '')}\n\n"
        f"Decisão: {registro_fato.get('decsiao_teor_limpo', '')}"
    )
    
    if len(texto_embedding.strip()) < 50:
        logger.warning(f"ID {id_julgado}: Texto muito curto, ignorado.")
        return None
        
    try:
        embedding_vector = gerar_embedding(texto_embedding)
        
        return {
            "ID_JULGADO_FK": id_julgado,
            "TEXTO_FONTE": texto_embedding,
            "TIPO_FONTE": "ACORDAO_COMPLETO",
            # Converte o vetor para o formato de string que o psycopg2 entende
            "EMBEDDING": f"[{','.join(map(str, embedding_vector))}]" 
        }
    except Exception as e:
        logger.error(f"Erro fatal ao gerar embedding para ID {id_julgado}: {e}")
        return None

# =================================================================
# 3. FUNÇÕES DE CARREGAMENTO (L) - SEM MUDANÇAS
# =================================================================

def inserir_em_lote(cursor, dados: List[Dict[str, Any]]):
    """ 
    Insere os registros vetoriais na tabela DIM_VETORES_LLM com UPSERT.
    """
    if not dados: return

    colunas = list(dados[0].keys())
    colunas_sql = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    valores = [[item.get(coluna) for coluna in colunas] for item in dados]
    
    # IMPORTANTE: A tabela DIM_VETORES_LLM deve ter uma PRIMARY KEY em ID_JULGADO_FK
    # para que o ON CONFLICT funcione corretamente. Verifique o script de criação!
    comando_sql = f"""
        INSERT INTO {TABELA_VETORIAL} ({colunas_sql}) 
        VALUES ({placeholders}) 
        ON CONFLICT (ID_JULGADO_FK) DO UPDATE 
        SET EMBEDDING = EXCLUDED.EMBEDDING, TEXTO_FONTE = EXCLUDED.TEXTO_FONTE;
    """
    cursor.executemany(comando_sql, valores)

# =================================================================
# 4. EXECUÇÃO ETL (E, T, L) - ADAPTADA PARA DUAS CONEXÕES
# =================================================================

def executar_etl_vetorial(base_path: str):
    """ Coordena o processo ETL Vetorial, conectando-se à Fonte e ao Destino separadamente. """
    
    # 1. Configura ambiente de log
    _setup_environment(base_path)
    
    conn_sql = None
    conn_vector = None
    
    try:
        # 1.1 Conexão com o Banco de Dados SQL (Fonte)
        conn_sql = psycopg2.connect(**DB_CONFIG_SQL_SOURCE)
        cursor_sql = conn_sql.cursor()
        
        # 1.2 Conexão com o Banco de Dados Vetorial (Destino)
        conn_vector = psycopg2.connect(**DB_CONFIG_VECTOR_TARGET)
        conn_vector.autocommit = False
        cursor_vector = conn_vector.cursor()

        # 2. EXTRAÇÃO (E) - Otimizada com LEFT JOIN (usando o cursor_sql)
        # Seleciona registros da tabela FATO (SQL) que AINDA NÃO possuem um vetor 
        # correspondente na tabela vetorial (Vetorial)
        
        # 🛑 IMPORTANTE: Como os bancos são separados, esta query não pode ser um LEFT JOIN!
        # A nova estratégia é ler todos os IDs da fonte e todos os IDs do destino, e subtrair no Python.
        
        logger.info("Iniciando Extração de IDs da Fonte SQL e Destino Vetorial...")
        
        # a) Extrai todos os IDs da Fonte (SQL)
        cursor_sql.execute(f"SELECT id_julgado, ementa_limpa, decsiao_teor_limpo FROM {TABELA_FONTE} ORDER BY id_julgado ASC;")
        dados_fonte = cursor_sql.fetchall()
        
        # b) Extrai todos os IDs que JÁ foram vetorizados (Vetorial)
        cursor_vector.execute(f"SELECT ID_JULGADO_FK FROM {TABELA_VETORIAL};")
        ids_vetorizados = {row[0] for row in cursor_vector.fetchall()}

        # c) Filtra os dados da fonte para obter apenas os NOVOS IDs
        registros_a_processar = []
        for id_julgado, ementa, decisao in dados_fonte:
            if id_julgado not in ids_vetorizados:
                registros_a_processar.append(dict(zip(['id_julgado', 'ementa_limpa', 'decsiao_teor_limpo'], [id_julgado, ementa, decisao])))
        
        total_a_processar = len(registros_a_processar)
        logger.info(f"Total de registros na Fonte: {len(dados_fonte)}")
        logger.info(f"Total de registros já vetorizados: {len(ids_vetorizados)}")
        logger.info(f"Total de NOVOS registros a processar: {total_a_processar}")

        # 3. TRANSFORMAÇÃO (T) em Lote e CARREGAMENTO (L)
        lote_vetorial = []
        registros_processados = 0
        TAMANHO_LOTE = 50
        
        for registro_dict in registros_a_processar:
            registros_processados += 1
            
            registro_vetorial = criar_registro_vetorial(registro_dict)
            
            if registro_vetorial:
                lote_vetorial.append(registro_vetorial)

            if len(lote_vetorial) >= TAMANHO_LOTE:
                inserir_em_lote(cursor_vector, lote_vetorial)
                conn_vector.commit()
                logger.info(f"Lote commitado. Processados: {registros_processados}/{total_a_processar}")
                lote_vetorial = []
        
        # Insere o lote restante
        if lote_vetorial:
            inserir_em_lote(cursor_vector, lote_vetorial)
        
        conn_vector.commit()
        
        logger.info("\n--- ETL VETORIAL CONCLUÍDO ---")
        logger.info(f"Total de novos registros processados e vetorizados: {registros_processados}")

    except (Exception, psycopg2.Error) as error:
        logger.critical(f"ERRO CRÍTICO DURANTE O ETL Vetorial: {error}")
        if conn_vector: conn_vector.rollback()
            
    finally:
        if conn_sql:
            conn_sql.close()
        if conn_vector:
            conn_vector.close()
        logger.info("Conexões com os bancos de dados fechadas.")

# =================================================================
# 5. EXECUÇÃO
# =================================================================
if __name__ == '__main__':
    executar_etl_vetorial(PASTA_BASE)