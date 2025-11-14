# etl_vetorial_processo.py

import psycopg2
import json
from typing import List, Dict, Any
import random
import time

# =================================================================
# 1. CONFIGURAÇÕES E NOMES DE TABELAS (AJUSTE AQUI)
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

# =================================================================
# 2. FUNÇÕES DE TRANSFORMAÇÃO (Embedding)
# =================================================================

def gerar_embedding(texto: str) -> List[float]:
    """
    *** FUNÇÃO CHAVE: SUBSTITUA ISTO PELA CHAMADA REAL À API DE EMBEDDING ***
    
    Exemplo de chamada com a SDK do Google GenAI:
    
    from google import genai
    client = genai.Client(api_key="SUA_CHAVE")
    
    response = client.models.embed_content(
        model="text-embedding-004",  # Ou outro modelo de sua escolha
        content=[texto]
    )
    return response['embedding'][0]
    
    """
    if not texto:
        return [0.0] * DIMENSAO_VETOR
    
    # --- SIMULADOR DE EMBEDDING ---
    # Na produção, este bloco será substituído pela API real.
    random.seed(hash(texto) % 1000) # Garante que o mesmo texto gere o mesmo vetor (simulado)
    vector = [random.uniform(-0.1, 0.1) for _ in range(DIMENSAO_VETOR)]
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
        print(f"Erro ao gerar embedding para ID {id_julgado}: {e}")
        return None

# =================================================================
# 3. FUNÇÕES DE CARREGAMENTO (L)
# =================================================================

def inserir_em_lote(cursor, dados: List[Dict[str, Any]]):
    """ Insere os registros vetoriais na tabela DIM_VETORES_LLM. """
    if not dados: return

    colunas = list(dados[0].keys())
    colunas_sql = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    valores = [[item.get(coluna) for coluna in colunas] for item in dados]
    
    # Usamos ON CONFLICT para evitar duplicatas se o script for rodado novamente.
    comando_sql = f"""
        INSERT INTO {TABELA_VETORIAL} ({colunas_sql}) 
        VALUES ({placeholders}) 
        ON CONFLICT (ID_JULGADO_FK) DO UPDATE 
        SET EMBEDDING = EXCLUDED.EMBEDDING, TEXTO_FONTE = EXCLUDED.TEXTO_FONTE;
    """
    cursor.executemany(comando_sql, valores)

def executar_etl_vetorial():
    """ Coordena o processo ETL Vetorial. """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        # Seleciona as colunas tratadas da Tabela FATO
        query = f"""
            SELECT id_julgado, ementa_limpa, decsiao_teor_limpo 
            FROM {TABELA_FONTE}
            -- Exclui IDs que já estão na tabela vetorial para processar apenas novos dados (opcional)
            WHERE id_julgado NOT IN (SELECT id_julgado_fk FROM {TABELA_VETORIAL})
            ORDER BY id_julgado ASC;
        """
        cursor.execute(query)
        
        lote_vetorial = []
        registros_processados = 0
        
        print("Iniciando geração de embeddings (Embeddings simulados se não houver API real).")

        # Itera sobre os dados da Tabela FATO
        for registro_fato in cursor.fetchall():
            registros_processados += 1
            
            # Converte a tupla em dicionário para facilitar o acesso
            registro_dict = dict(zip(['id_julgado', 'ementa_limpa', 'decsiao_teor_limpo'], registro_fato))
            
            # Geração do Vetor (Chamada de API simulada/real)
            registro_vetorial = criar_registro_vetorial(registro_dict)
            
            if registro_vetorial:
                lote_vetorial.append(registro_vetorial)

            # Carregamento em lote a cada 50 registros (para economizar chamadas de commit e API)
            if len(lote_vetorial) >= 50:
                inserir_em_lote(cursor, lote_vetorial)
                conn.commit()
                print(f"Processados e inseridos {registros_processados} vetores...")
                lote_vetorial = []
        
        # Insere o lote restante
        if lote_vetorial:
            inserir_em_lote(cursor, lote_vetorial)
        
        conn.commit()
        print(f"\n--- ETL VETORIAL CONCLUÍDO ---")
        print(f"Total de registros processados: {registros_processados}")

    except (Exception, psycopg2.Error) as error:
        print(f"Erro durante o ETL Vetorial: {error}")
        if conn: conn.rollback()
            
    finally:
        if conn:
            cursor.close()
            conn.close()

# =================================================================
# 4. EXECUÇÃO
# =================================================================
if __name__ == '__main__':
    executar_etl_vetorial()

    