# -*- coding: utf-8 -*-
# load-datalake.py - Processo ETL para modelagem de Fato e Dimensões (STJ)
# Versão robusta para alto volume: Usa Cursors Separados e de Servidor com 'withhold=True'.

import psycopg2
import json
import re
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple

# =================================================================
# 1. CONFIGURAÇÕES, LOGGING E VARIÁVEIS GLOBAIS
# =================================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5434,         # Porta mapeada para o judged_db (SQL Source)
    "dbname": "legal",
    "user": "admin",
    "password": "admin"
}

# Nomes das Tabelas
TABELA_ORIGEM = "judged" 
TABELA_FATO = "FATO_JULGADOS_STJ"
TABELA_DIM_REF = "DIM_REFERENCIAS_LEGAIS"
TABELA_DIM_ASSUNTOS = "DIM_ASSUNTOS_STJ"

# Configurações de Log
PASTA_BASE = r"D:\Sincronizado\tecnologia\data\stj-datalake" 
LOG_FILE_NAME = "dw_etl_status.log"
logger = logging.getLogger(__name__)

def _setup_environment(base_path: str):
    """ Configura o logger e a estrutura de pastas. """
    global PASTA_BASE
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
    logger.info(f"Ambiente DW ETL configurado. Fonte SQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    logger.info("-" * 80)


# =================================================================
# 2. FUNÇÕES DE TRANSFORMAÇÃO (T)
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
            
    try: return datetime.strptime(data_string, '%Y-%m-%d').date()
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
    
    try: id_julgado = int(registro.get("id")) 
    except (ValueError, TypeError): 
        try: id_julgado = int(registro.get("id_origem"))
        except (ValueError, TypeError):
            return None, [], []

    registro_fato = {
        "ID_JULGADO": id_julgado,
        # 🟢 CORREÇÃO: Mapeamento de ID_ORIGEM_NATURAL para resolver a restrição NOT NULL
        # Usando o ID_JULGADO como fallback se o campo não existir ou for nulo.
        "ID_ORIGEM_NATURAL": limpar_texto(registro.get("id_origem_natural", str(id_julgado))),
        
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
        "TEOR_BRUTO_JSON": json.dumps({k: registro[k] for k in nomes_colunas if k in registro}, default=str), 
    }
    
    referencias_legais = extrair_referencias_legais(registro.get("referenciasLegislativas", "") or "", id_julgado)
    
    campos_assuntos = {
        'tema': registro.get('tema'),
        'teseJuridica': registro.get('teseJuridica'),
        'termosAuxiliares': registro.get('termosAuxiliares')
    }
    assuntos_segmentados = extrair_assuntos_e_teses(campos_assuntos, id_julgado)

    return registro_fato, referencias_legais, assuntos_segmentados

# =================================================================
# 3. CARREGAMENTO (L) e PROCESSO PRINCIPAL - COM CURSORS DUPLOS
# =================================================================

def inserir_em_lote(cursor, tabela: str, dados: List[Dict[str, Any]]):
    """ 
    Função auxiliar para inserção eficiente de múltiplos registros com lógica UPSERT (ON CONFLICT). 
    Recebe um cursor SIMPLES (não nomeado).
    """
    if not dados: return

    colunas = list(dados[0].keys())
    colunas_sql = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    valores = [[item.get(coluna) for coluna in colunas] for item in dados]
    
    set_updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in colunas])
    
    chave_conflito = None

    if tabela == TABELA_FATO:
        chave_conflito = "ID_JULGADO"
        set_updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in colunas if col not in [chave_conflito, 'TEOR_BRUTO_JSON']]) 
        
    elif tabela == TABELA_DIM_REF:
        chave_conflito = "ID_JULGADO_FK, TIPO_NORMA, NORMA_NOME, ARTIGO_DISPOSITIVO"
        
    elif tabela == TABELA_DIM_ASSUNTOS:
        chave_conflito = "ID_JULGADO_FK, TIPO_ASSUNTO, TERMO"
        
    
    if chave_conflito:
        # Comando SQL com UPSERT (ON CONFLICT)
        comando_sql = (
            f"INSERT INTO {tabela} ({colunas_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT ({chave_conflito}) DO UPDATE SET {set_updates};"
        )
    else:
        # Insere sem UPSERT
        comando_sql = f"INSERT INTO {tabela} ({colunas_sql}) VALUES ({placeholders});"
        
    cursor.executemany(comando_sql, valores)


def _obter_contagem_total(conn) -> int:
    """ Obtém o total de registros na tabela de origem para cálculo do progresso. """
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {TABELA_ORIGEM};")
            # Verifica o resultado antes de acessar o índice
            resultado = cursor.fetchone()
            return resultado[0] if resultado and resultado[0] is not None else 0
    except Exception as e:
        logger.warning(f"Não foi possível obter a contagem total para progresso: {e}. Retornando 0.")
        return 0

def _exibir_progresso_console(lidos: int, total: int, inseridos_fato: int, lote_size: int):
    """ Exibe o progresso na mesma linha do console usando \r. """
    porcentagem = (lidos / total) * 100 if total > 0 else 0
    
    progress_str = (
        f"🚀 Progresso: {porcentagem:.2f}% | Lidos: {lidos:,} / {total:,} "
        f"| Inseridos (Fato): {inseridos_fato:,} | Tam. Lote: {lote_size}..."
    )
        
    sys.stdout.write(f"\r{progress_str}")
    sys.stdout.flush()


def executar_etl_stj(base_path: str):
    """ Coordena o processo de Extração, Transformação e Carregamento (ETL). """
    
    _setup_environment(base_path)
    
    conn = None
    read_cursor = None
    write_cursor = None
    registros_lidos = 0
    registros_inseridos_fato = 0
    TAMANHO_LOTE = 1000
    total_registros = 0 
    
    try:
        # 2. CONEXÃO E EXTRAÇÃO (E)
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        
        total_registros = _obter_contagem_total(conn)
        
        # Criação de DOIS cursors separados
        write_cursor = conn.cursor() 
        read_cursor = conn.cursor(name="etl_stj_cursor", withhold=True)

        # Obtém os nomes das colunas
        temp_cursor = conn.cursor()
        temp_cursor.execute(f"SELECT * FROM {TABELA_ORIGEM} LIMIT 0;")
        nomes_colunas = [desc[0] for desc in temp_cursor.description]
        temp_cursor.close()

        logger.info(f"Iniciando Extração de dados da tabela Staging: {TABELA_ORIGEM} (Total: {total_registros:,})")
        
        # O SELECT longo usa o cursor nomeado (read_cursor)
        read_cursor.execute(f"SELECT * FROM {TABELA_ORIGEM};")
        
        lote_fato, lote_dim_ref, lote_dim_assuntos = [], [], []

        for linha_bruta in read_cursor: # Itera usando o cursor de leitura
            registros_lidos += 1
            
            # 🟢 CORREÇÃO CRÍTICA: Verifica se a linha lida é None antes de tentar desempacotar
            if linha_bruta is None:
                logger.warning(f"Registro Nulo encontrado na iteração {registros_lidos}. Pulando.")
                continue

            registro_dict = dict(zip(nomes_colunas, linha_bruta))
            
            _exibir_progresso_console(registros_lidos, total_registros, registros_inseridos_fato, len(lote_fato))

            # 3. TRANSFORMAÇÃO (T)
            registro_fato, referencias_legais, assuntos_segmentados = tratar_registro_etl(registro_dict, nomes_colunas)
            
            if registro_fato:
                lote_fato.append(registro_fato)
                lote_dim_ref.extend(referencias_legais)
                lote_dim_assuntos.extend(assuntos_segmentados)

            # 4. CARREGAMENTO (L) - Inserção em lote
            if len(lote_fato) >= TAMANHO_LOTE:
                
                inserir_em_lote(write_cursor, TABELA_FATO, lote_fato)
                inserir_em_lote(write_cursor, TABELA_DIM_REF, lote_dim_ref)
                inserir_em_lote(write_cursor, TABELA_DIM_ASSUNTOS, lote_dim_assuntos)
                
                conn.commit()
                
                registros_inseridos_fato += len(lote_fato)
                
                _exibir_progresso_console(registros_lidos, total_registros, registros_inseridos_fato, 0)
                sys.stdout.write('\n') 
                logger.info(f"Lote commitado. Total inserido/atualizado (Fato): {registros_inseridos_fato:,}")
                
                lote_fato, lote_dim_ref, lote_dim_assuntos = [], [], []
                
        # Insere os lotes restantes
        if lote_fato:
            inserir_em_lote(write_cursor, TABELA_FATO, lote_fato)
            inserir_em_lote(write_cursor, TABELA_DIM_REF, lote_dim_ref)
            inserir_em_lote(write_cursor, TABELA_DIM_ASSUNTOS, lote_dim_assuntos)
            registros_inseridos_fato += len(lote_fato)
            
        conn.commit()
        
        _exibir_progresso_console(registros_lidos, total_registros, registros_inseridos_fato, 0)
        sys.stdout.write('\n')
        
        logger.info("\n--- ETL PARA DATA WAREHOUSE CONCLUÍDO ---")
        logger.info(f"Total de registros lidos da Staging: {registros_lidos:,}")
        logger.info(f"Total de registros de Fato inseridos/atualizados: {registros_inseridos_fato:,}")

    except (Exception, psycopg2.Error) as error:
        logger.critical(f"ERRO CRÍTICO DURANTE O ETL: {error}")
        if conn: conn.rollback()
            
    finally:
        # Tratamento de erro robusto no finally para o InvalidCursorName
        if read_cursor:
            try:
                read_cursor.close()
            except psycopg2.ProgrammingError: 
                pass 
        
        if write_cursor:
            try:
                write_cursor.close()
            except psycopg2.ProgrammingError:
                pass
                
        if conn:
            conn.close()
            logger.info("Conexão com o banco de dados fechada.")

# =================================================================
# 4. EXECUÇÃO
# =================================================================
if __name__ == '__main__':
    executar_etl_stj(PASTA_BASE)