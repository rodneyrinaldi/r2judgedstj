# -*- coding: utf-8 -*-
# dw_processo_completo.py - Processo ETL para modelagem de Fato e Dimensões (STJ)
# Implementa UPSERT nas tabelas de Fato e Bridge para evitar duplicação.
import psycopg2
import json
import re
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set

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

# Nomes das Tabelas
TABELA_ORIGEM = "judged" 
TABELA_FATO = "FATO_JULGADOS_STJ"
TABELA_DIM_REF = "DIM_REFERENCIAS_LEGAIS" # Atua como Tabela Bridge
TABELA_DIM_ASSUNTOS = "DIM_ASSUNTOS_STJ" # Atua como Tabela Bridge

# Configurações de Log
# *** VALOR PADRÃO USADO DIRETAMENTE: NÃO SERÁ MAIS SOLICITADO AO USUÁRIO ***
PASTA_BASE = r"D:\Sincronizado\tecnologia\data\stj-datalake" 
LOG_FILE_NAME = "dw_etl_status.log"
LOG_FILE = "" 
logger = logging.getLogger(__name__)

def _setup_environment(base_path: str):
    """ 
    Configura a PASTA_BASE global, cria a estrutura de pastas e configura o logger
    para salvar o log de status DENTRO da pasta base.
    """
    global PASTA_BASE, LOG_FILE
    
    # Redefine PASTA_BASE com o valor passado (que será o valor global inicial)
    PASTA_BASE = base_path
    
    # 1. Cria a pasta base se ela não existir
    if not os.path.exists(PASTA_BASE):
        try:
            os.makedirs(PASTA_BASE)
        except OSError as e:
            # Em caso de erro na criação (permissão, caminho inválido), sai do programa
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
    logger.info(f"Ambiente DW ETL configurado. Logs de status em: {LOG_FILE}")
    logger.info(f"Tabelas de destino: Fato='{TABELA_FATO}', Dimensão 1='{TABELA_DIM_REF}', Dimensão 2='{TABELA_DIM_ASSUNTOS}'")
    logger.info("-" * 80)


# =================================================================
# 2. FUNÇÕES DE TRANSFORMAÇÃO (T) - Mantidas do original
# =================================================================

def limpar_texto(texto: Any) -> str:
    """ Remove ruídos de strings (quebras de linha, espaços múltiplos). """
    if texto is None: return ""
    if isinstance(texto, list):
        texto = " ".join([str(item) for item in texto if item is not None])
    
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def extrair_data(data_string: str) -> datetime | None:
    """ Converte strings de data em formatos variados para objeto date. """
    if not data_string: return None
        
    if re.match(r'^\d{8}$', data_string):
        try: return datetime.strptime(data_string, '%Y%m%d').date()
        except ValueError: pass
            
    match_dj = re.search(r'DATA:(\d{2}/\d{2}/\d{4})', data_string)
    if match_dj:
        try: return datetime.strptime(match_dj.group(1), '%d/%m/%Y').date()
        except ValueError: pass
            
    try: return datetime.strptime(data_string, '%Y-%m-%d').date() # Tentativa de formato SQL
    except ValueError: pass
            
    return None

def extrair_resultado_binario(decisao_texto: str, ementa_texto: str) -> bool | None:
    """ Inferência de resultado binário (Provido/Negado). """
    texto = (limpar_texto(decisao_texto) + " " + limpar_texto(ementa_texto)).upper()
    
    padroes_favoraveis = [r'DAR PROVIMENTO', r'DEU PROVIMENTO', r'ACOLHER OS EMBARGOS', r'CONHECER.*E DAR PROVIMENTO', r'JULGAR PROCEDENTE']
    padroes_desfavoraveis = [r'NEGAR PROVIMENTO', r'NEGOU PROVIMENTO', r'REJEITAR OS EMBARGOS', r'NÃO CONHECER DO RECURSO', r'INDEFERIR O PEDIDO', r'JULGAR IMPROCEDENTE']
    
    if any(re.search(p, texto) for p in padroes_favoraveis): return True
    if any(re.search(p, texto) for p in padroes_desfavoraveis): return False
        
    return None

def extrair_referencias_legais(refs_brutas: str, id_julgado: int) -> List[Dict[str, Any]]:
    """ Extrai Leis, Súmulas e Temas do campo 'referenciasLegislativas'. """
    referencias_estruturadas = []
    
    try: refs = json.loads(refs_brutas)
    except (json.JSONDecodeError, TypeError): refs = []
        
    if not isinstance(refs, list): refs = []

    for ref_dict in refs:
        norma_bruta = limpar_texto(ref_dict.get('referencia', ''))
        
        # Lógica de extração... (simplificada para o contexto)
        match_tipo = re.search(r'LEG:FED (\w+):(\*+|[A-Z0-9]+)', norma_bruta)
        if match_tipo:
            tipo, nome_norma = match_tipo.groups()
            match_disp = re.search(r'ART:(\w+) (?:INC:(\w+))? (?:PAR:(\w+))?', norma_bruta)
            dispositivo = ""
            if match_disp:
                art, inc, par = match_disp.groups()
                dispositivo += f"ART:{art}"
                if par: dispositivo += f" PAR:{par}"
                if inc: dispositivo += f" INC:{inc}"

            referencias_estruturadas.append({
                "ID_JULGADO_FK": id_julgado, "TIPO_NORMA": tipo, "NORMA_NOME": nome_norma, "ARTIGO_DISPOSITIVO": dispositivo,
            })
        
        elif 'SUM:' in norma_bruta:
            match_sumula = re.search(r'SUM:(\d+)', norma_bruta)
            if match_sumula:
                sumula_num = match_sumula.group(1)
                referencias_estruturadas.append({
                    "ID_JULGADO_FK": id_julgado, "TIPO_NORMA": "SUMULA", "NORMA_NOME": f"SUMULA {sumula_num}", "ARTIGO_DISPOSITIVO": "",
                })
            
    return referencias_estruturadas

def extrair_assuntos_e_teses(campos: Dict[str, str], id_julgado: int) -> List[Dict[str, Any]]:
    """ Extrai Teses Jurídicas e Termos Auxiliares para DIM_ASSUNTOS_STJ. """
    assuntos = []
    
    tema = limpar_texto(campos.get('tema', ''))
    if tema:
        assuntos.append({"ID_JULGADO_FK": id_julgado, "TIPO_ASSUNTO": "TEMA_REPETITIVO", "TERMO": tema})
        
    tese = limpar_texto(campos.get('teseJuridica', ''))
    if tese:
        assuntos.append({"ID_JULGADO_FK": id_julgado, "TIPO_ASSUNTO": "TESE_JURIDICA", "TERMO": tese})
        
    termos_aux = limpar_texto(campos.get('termosAuxiliares', '')).split(';')
    for termo in [t.strip() for t in termos_aux if t.strip()]:
        assuntos.append({"ID_JULGADO_FK": id_julgado, "TIPO_ASSUNTO": "TERMO_AUXILIAR", "TERMO": termo})

    return assuntos

def tratar_registro_etl(registro: Dict[str, Any], nomes_colunas: List[str]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """ Aplica todas as transformações ETL em um único registro. """
    
    # Assume-se que 'id' na origem (Staging) corresponde a 'ID_JULGADO' no DW.
    try: id_julgado = int(registro.get("id")) 
    except (ValueError, TypeError): 
        try: id_julgado = int(registro.get("id_origem"))
        except (ValueError, TypeError):
            logger.error(f"Registro sem ID numérico válido. Descartado: {registro}")
            return None, [], []

    # 1. TRATAMENTO PARA A TABELA FATO
    registro_fato = {
        "ID_JULGADO": id_julgado,
        "DT_DECISAO": extrair_data(registro.get("dataDecisao", "")),
        "DT_PUBLICACAO": extrair_data(registro.get("dataPublicacao", "")),
        "CLASSE_SIGLA": limpar_texto(registro.get("siglaClasse")),
        "ORGAO_JULGADOR": limpar_texto(registro.get("nomeOrgaoJulgador")),
        "MINISTRO_RELATOR": limpar_texto(registro.get("ministroRelator")),
        
        "RESULTADO_BINARIO": extrair_resultado_binario(registro.get("decisao", ""), registro.get("ementa", "")),
        "TEMA_REPETITIVO": limpar_texto(registro.get("tema")),
        "EMENTA_LIMPA": limpar_texto(registro.get("ementa")),
        "DECSIAO_TEOR_LIMPO": limpar_texto(registro.get("decisao")),
        "ACORDAOS_SIMILARES_LIMPO": limpar_texto(registro.get("acordaosSimilares")),
        "JURISPRUDENCIA_CITADA_LIMPA": limpar_texto(registro.get("jurisprudenciaCitada")),
        # Mantém o JSON bruto para rastreabilidade (SCD Tipo 0)
        "TEOR_BRUTO_JSON": json.dumps({k: registro[k] for k in nomes_colunas if k in registro}, default=str), 
    }
    
    # 2. TRATAMENTO PARA DIM_REFERENCIAS_LEGAIS (Bridge)
    referencias_legais = extrair_referencias_legais(registro.get("referenciasLegislativas", "") or "", id_julgado)

    # 3. TRATAMENTO PARA DIM_ASSUNTOS_STJ (Bridge)
    campos_assuntos = {
        'tema': registro.get('tema'),
        'teseJuridica': registro.get('teseJuridica'),
        'termosAuxiliares': registro.get('termosAuxiliares')
    }
    assuntos_segmentados = extrair_assuntos_e_teses(campos_assuntos, id_julgado)

    return registro_fato, referencias_legais, assuntos_segmentados

# =================================================================
# 3. CARREGAMENTO (L) e PROCESSO PRINCIPAL - COM UPSERT ADICIONAL
# =================================================================

def inserir_em_lote(cursor, tabela: str, dados: List[Dict[str, Any]]):
    """ 
    Função auxiliar para inserção eficiente de múltiplos registros com lógica UPSERT (ON CONFLICT). 
    A lógica ON CONFLICT é adaptada para cada tabela para garantir a unicidade.
    """
    if not dados: return

    colunas = list(dados[0].keys())
    colunas_sql = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    valores = [[item.get(coluna) for coluna in colunas] for item in dados]
    
    set_updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in colunas])
    
    if tabela == TABELA_FATO:
        # Chave de Conflito: ID_JULGADO
        chave_conflito = "ID_JULGADO"
        # Não atualiza ID_JULGADO e TEOR_BRUTO_JSON (chave primária e rastreabilidade)
        set_updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in colunas if col not in [chave_conflito, 'TEOR_BRUTO_JSON']]) 
        
    elif tabela == TABELA_DIM_REF:
        # Chave de Conflito Composta: Garante que a mesma referência não é duplicada para o mesmo julgado
        # Assume-se que esta combinação é UNIQUE no DDL da tabela.
        chave_conflito = "ID_JULGADO_FK, TIPO_NORMA, NORMA_NOME, ARTIGO_DISPOSITIVO"
        
    elif tabela == TABELA_DIM_ASSUNTOS:
        # Chave de Conflito Composta: Garante que o mesmo assunto/termo não é duplicado para o mesmo julgado
        # Assume-se que esta combinação é UNIQUE no DDL da tabela.
        chave_conflito = "ID_JULGADO_FK, TIPO_ASSUNTO, TERMO"
        
    else:
        # Para outras tabelas, insere puro (APPEND)
        comando_sql = f"INSERT INTO {tabela} ({colunas_sql}) VALUES ({placeholders});"
        cursor.executemany(comando_sql, valores)
        return

    # Comando SQL com UPSERT (ON CONFLICT)
    comando_sql = (
        f"INSERT INTO {tabela} ({colunas_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT ({chave_conflito}) DO UPDATE SET {set_updates};"
    )
    
    cursor.executemany(comando_sql, valores)


def executar_etl_stj(base_path: str):
    """ Coordena o processo de Extração, Transformação e Carregamento (ETL). """
    
    # 1. Configura ambiente de log
    _setup_environment(base_path)
    
    conn = None
    registros_lidos = 0
    registros_inseridos_fato = 0
    
    try:
        # 2. CONEXÃO E EXTRAÇÃO (E)
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {TABELA_ORIGEM} LIMIT 0;")
        nomes_colunas = [desc[0] for desc in cursor.description]

        logger.info(f"Iniciando Extração de dados da tabela Staging: {TABELA_ORIGEM}")
        cursor.execute(f"SELECT * FROM {TABELA_ORIGEM};")
        
        lote_fato, lote_dim_ref, lote_dim_assuntos = [], [], []

        for linha_bruta in cursor:
            registros_lidos += 1
            registro_dict = dict(zip(nomes_colunas, linha_bruta))

            # 3. TRANSFORMAÇÃO (T)
            registro_fato, referencias_legais, assuntos_segmentados = tratar_registro_etl(registro_dict, nomes_colunas)
            
            if registro_fato:
                lote_fato.append(registro_fato)
                lote_dim_ref.extend(referencias_legais)
                lote_dim_assuntos.extend(assuntos_segmentados)

            # 4. CARREGAMENTO (L) - Inserção em lote
            if len(lote_fato) >= 1000:
                inserir_em_lote(cursor, TABELA_FATO, lote_fato)
                inserir_em_lote(cursor, TABELA_DIM_REF, lote_dim_ref)
                inserir_em_lote(cursor, TABELA_DIM_ASSUNTOS, lote_dim_assuntos)
                
                conn.commit()
                registros_inseridos_fato += len(lote_fato)
                logger.info(f"Lote commitado. Registros lidos: {registros_lidos}. Fato inserido/atualizado: {registros_inseridos_fato}")
                
                lote_fato, lote_dim_ref, lote_dim_assuntos = [], [], []
        
        # Insere os lotes restantes
        if lote_fato:
            inserir_em_lote(cursor, TABELA_FATO, lote_fato)
            inserir_em_lote(cursor, TABELA_DIM_REF, lote_dim_ref)
            inserir_em_lote(cursor, TABELA_DIM_ASSUNTOS, lote_dim_assuntos)
            registros_inseridos_fato += len(lote_fato)
        
        conn.commit()
        
        logger.info("\n--- ETL PARA DATA WAREHOUSE CONCLUÍDO ---")
        logger.info(f"Total de registros lidos da Staging: {registros_lidos}")
        logger.info(f"Total de registros de Fato inseridos/atualizados: {registros_inseridos_fato}")

    except (Exception, psycopg2.Error) as error:
        logger.critical(f"ERRO CRÍTICO DURANTE O ETL: {error}")
        if conn: conn.rollback()
            
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Conexão com o banco de dados fechada.")

# =================================================================
# 4. EXECUÇÃO
# =================================================================
if __name__ == '__main__':
    # *** A chamada de input foi removida. O script usa o valor da PASTA_BASE definido acima. ***
    # Se a pasta não existir ou houver um erro de permissão, o _setup_environment irá tratar.
    executar_etl_stj(PASTA_BASE)