# -*- coding: utf-8 -*-
# etl_vetorial_processo_atualizado.py - Processo ETL para Geração e Carregamento de Embeddings
# Otimizado com rotina de LOG e consulta com LEFT JOIN para dados incrementais.

import psycopg2
import json
import random
import time
import os
import sys
import logging
from typing import List, Dict, Any

# =================================================================
# 1. CONFIGURAÇÕES, LOGGING E VARIÁVEIS GLOBAIS
# =================================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "legal",
    "user": "admin",
    "password": "admin"
}

# Tabela Fonte de Texto (Tabela Fato)
TABELA_FONTE = "FATO_JULGADOS_STJ"
# Tabela Destino de Vetores
TABELA_VETORIAL = "DIM_VETORES_LLM"

# Deve ser a mesma dimensão configurada em 'criar_schema_vetorial.py'
DIMENSAO_VETOR = 768 

# Configurações de Log
PASTA_BASE = "" # Será definida em tempo de execução
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
    
    # 1. Cria a pasta base se ela não existir
    if not os.path.exists(PASTA_BASE):
        try:
            os.makedirs(PASTA_BASE)
        except OSError as e:
            # Em caso de erro crítico, imprime e encerra
            print(f"Erro Crítico: Não foi possível criar a pasta base '{PASTA_BASE}': {e}")
            sys.exit(1)
            
    # 2. Define o caminho completo do log
    LOG_FILE = os.path.join(PASTA_BASE, LOG_FILE_NAME)

    # 3. Configura o Logger para usar o caminho do log
    while logger.handlers:
        logger.handlers.pop()
    
    logger.setLevel(logging.INFO)
    
    # Adiciona StreamHandler para logs informativos no console
    logger.addHandler(logging.StreamHandler(sys.stdout))
    
    # Adiciona FileHandler para logs de erro e status (dentro da PASTA_BASE)
    file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info("-" * 80)
    logger.info(f"Ambiente ETL Vetorial configurado. Logs de status em: {LOG_FILE}")
    logger.info(f"Tabela de destino: {TABELA_VETORIAL}")
    logger.info("-" * 80)


# =================================================================
# 2. FUNÇÕES DE TRANSFORMAÇÃO (Embedding)
# =================================================================

def gerar_embedding(texto: str) -> List[float]:
    """
    *** FUNÇÃO CHAVE: SUBSTITUA ISTO PELA CHAMADA REAL À API DE EMBEDDING ***
    
    Esta função deve ser substituída pela integração com uma API de LLM (ex: Gemini API, OpenAI).
    
    """
    if not texto:
        return [0.0] * DIMENSAO_VETOR
    
    # --- SIMULADOR DE EMBEDDING ---
    # Na produção, este bloco será substituído pela API real.
    random.seed(hash(texto) % 1000) # Garante que o mesmo texto gere o mesmo vetor (simulado)
    vector = [random.uniform(-0.1, 0.1) for _ in range(DIMENSAO_VETOR)]
    # Simula o tempo de latência da API
    time.sleep(0.05) 
    # --- FIM SIMULADOR ---
    
    return vector

def criar_registro_vetorial(registro_fato: Dict[str, Any]) -> Dict[str, Any] | None:
    """ Concatena o texto, gera o embedding e prepara o registro para a tabela vetorial. """
    
    id_julgado = registro_fato.get("id_julgado")
    
    # Concatena a ementa e a decisão limpas para criar a fonte de busca vetorial
    texto_embedding = (
        f"Ementa: {registro_fato.get('ementa_limpa', '')}\n\n"
        f"Decisão: {registro_fato.get('decsiao_teor_limpo', '')}"
    )
    
    if len(texto_embedding.strip()) < 50: # Ignora registros com texto muito curto
        logger.warning(f"ID {id_julgado}: Texto muito curto, ignorado.")
        return None
        
    try:
        # AQUI OCORRE A CHAMADA MAIS LENTA (API)
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
# 3. FUNÇÕES DE CARREGAMENTO (L)
# =================================================================

def inserir_em_lote(cursor, dados: List[Dict[str, Any]]):
    """ 
    Insere os registros vetoriais na tabela DIM_VETORES_LLM com UPSERT.
    Garante que se o ID existir, o vetor e o texto fonte são atualizados.
    """
    if not dados: return

    colunas = list(dados[0].keys())
    colunas_sql = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    valores = [[item.get(coluna) for coluna in colunas] for item in dados]
    
    # Usamos ON CONFLICT (UPSERT) para garantir que não haja duplicatas
    # e que o reprocessamento atualize os dados existentes.
    comando_sql = f"""
        INSERT INTO {TABELA_VETORIAL} ({colunas_sql}) 
        VALUES ({placeholders}) 
        ON CONFLICT (ID_JULGADO_FK) DO UPDATE 
        SET EMBEDDING = EXCLUDED.EMBEDDING, TEXTO_FONTE = EXCLUDED.TEXTO_FONTE;
    """
    cursor.executemany(comando_sql, valores)

def executar_etl_vetorial(base_path: str):
    """ Coordena o processo ETL Vetorial. """
    
    # 1. Configura ambiente de log
    _setup_environment(base_path)
    
    conn = None
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        # 2. EXTRAÇÃO (E) - Otimizada com LEFT JOIN
        # A consulta seleciona registros da tabela FATO que AINDA NÃO possuem um vetor
        # correspondente na tabela vetorial (ou seja, DIM_VETORES_LLM.ID_JULGADO_FK é NULL).
        query = f"""
            SELECT T1.id_julgado, T1.ementa_limpa, T1.decsiao_teor_limpo
            FROM {TABELA_FONTE} AS T1
            LEFT JOIN {TABELA_VETORIAL} AS T2 ON T1.id_julgado = T2.id_julgado_fk
            WHERE T2.id_julgado_fk IS NULL
            ORDER BY T1.id_julgado ASC;
        """
        cursor.execute(query)
        
        lote_vetorial = []
        registros_processados = 0
        total_a_processar = cursor.rowcount
        
        logger.info(f"Iniciando a geração de embeddings. Total de novos registros a processar: {total_a_processar}")

        # 3. TRANSFORMAÇÃO (T) em Lote
        for registro_fato in cursor.fetchall():
            registros_processados += 1
            
            # Converte a tupla em dicionário para facilitar o acesso
            registro_dict = dict(zip(['id_julgado', 'ementa_limpa', 'decsiao_teor_limpo'], registro_fato))
            
            # Geração do Vetor (Chamada de API simulada/real)
            registro_vetorial = criar_registro_vetorial(registro_dict)
            
            if registro_vetorial:
                lote_vetorial.append(registro_vetorial)

            # 4. CARREGAMENTO (L) - Inserção em lote
            # O lote de 50 é um bom equilíbrio para chamadas de API lentas.
            if len(lote_vetorial) >= 50:
                inserir_em_lote(cursor, lote_vetorial)
                conn.commit()
                logger.info(f"Lote commitado. Processados: {registros_processados}/{total_a_processar}")
                lote_vetorial = []
        
        # Insere o lote restante
        if lote_vetorial:
            inserir_em_lote(cursor, lote_vetorial)
        
        conn.commit()
        
        logger.info("\n--- ETL VETORIAL CONCLUÍDO ---")
        logger.info(f"Total de registros processados e vetorizados: {registros_processados}")

    except (Exception, psycopg2.Error) as error:
        logger.critical(f"ERRO CRÍTICO DURANTE O ETL Vetorial: {error}")
        if conn: conn.rollback()
            
    finally:
        if conn:
            # Garante que o cursor e a conexão sejam fechados
            cursor.close()
            conn.close()
            logger.info("Conexão com o banco de dados fechada.")

# =================================================================
# 4. EXECUÇÃO
# =================================================================
if __name__ == '__main__':
    
    pasta_base_input = input("Digite o caminho da PASTA BASE para LOGS de DW Vetorial (padrão: ./dw_logs): ").strip()
    if not pasta_base_input:
        pasta_base_input = os.path.join(os.getcwd(), "dw_logs")

    if os.path.isdir(pasta_base_input) or os.path.exists(pasta_base_input) or not os.path.exists(pasta_base_input):
        executar_etl_vetorial(pasta_base_input)
    else:
        print(f"O caminho fornecido não é válido: {pasta_base_input}")