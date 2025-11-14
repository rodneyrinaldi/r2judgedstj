# etl_processo_dw.py

import psycopg2
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple

# =================================================================
# 1. CONFIGURAÇÕES E NOMES DE TABELAS (AJUSTE AQUI)
# =================================================================
DB_CONFIG = {
    "host": "localhost",  # Altere para o host correto
    "port": 5432,         
    "dbname": "legal",    
    "user": "admin",      
    "password": "admin"   
}

# Nomes das Tabelas
TABELA_ORIGEM = "judged" 
TABELA_FATO = "FATO_JULGADOS_STJ"
TABELA_DIM_REF = "DIM_REFERENCIAS_LEGAIS"
TABELA_DIM_ASSUNTOS = "DIM_ASSUNTOS_STJ"

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
        
        # 1. LEIS FEDERAIS E SIMILARES
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
            
        # 2. SÚMULAS
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
    
    # Extrai o TEMA Repetitivo
    tema = limpar_texto(campos.get('tema', ''))
    if tema:
        assuntos.append({"ID_JULGADO_FK": id_julgado, "TIPO_ASSUNTO": "TEMA_REPETITIVO", "TERMO": tema})
        
    # Extrai a TESE JURÍDICA
    tese = limpar_texto(campos.get('teseJuridica', ''))
    if tese:
        assuntos.append({"ID_JULGADO_FK": id_julgado, "TIPO_ASSUNTO": "TESE_JURIDICA", "TERMO": tese})
        
    # Extrai Termos Auxiliares (Tags de busca)
    termos_aux = limpar_texto(campos.get('termosAuxiliares', '')).split(';')
    for termo in [t.strip() for t in termos_aux if t.strip()]:
        assuntos.append({"ID_JULGADO_FK": id_julgado, "TIPO_ASSUNTO": "TERMO_AUXILIAR", "TERMO": termo})

    return assuntos

def tratar_registro_etl(registro: Dict[str, Any], nomes_colunas: List[str]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """ Aplica todas as transformações ETL em um único registro. """
    
    try: id_julgado = int(registro.get("id"))
    except (ValueError, TypeError): return None, [], []

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
        "TEOR_BRUTO_JSON": json.dumps({k: registro[k] for k in nomes_colunas if k in registro}),
    }
    
    # 2. TRATAMENTO PARA DIM_REFERENCIAS_LEGAIS
    referencias_legais = extrair_referencias_legais(registro.get("referenciasLegislativas", ""), id_julgado)

    # 3. TRATAMENTO PARA DIM_ASSUNTOS_STJ
    campos_assuntos = {
        'tema': registro.get('tema'),
        'teseJuridica': registro.get('teseJuridica'),
        'termosAuxiliares': registro.get('termosAuxiliares')
    }
    assuntos_segmentados = extrair_assuntos_e_teses(campos_assuntos, id_julgado)

    return registro_fato, referencias_legais, assuntos_segmentados

# =================================================================
# 3. CARREGAMENTO (L) e PROCESSO PRINCIPAL
# =================================================================

def inserir_em_lote(cursor, tabela, dados: List[Dict[str, Any]]):
    """ Função auxiliar para inserção eficiente de múltiplos registros. """
    if not dados: return

    colunas = list(dados[0].keys())
    colunas_sql = ", ".join(colunas)
    placeholders = ", ".join(["%s"] * len(colunas))
    valores = [[item.get(coluna) for coluna in colunas] for item in dados]
    
    if tabela == TABELA_FATO:
        set_updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in colunas if col not in ['ID_JULGADO', 'TEOR_BRUTO_JSON']])
        comando_sql = f"INSERT INTO {tabela} ({colunas_sql}) VALUES ({placeholders}) ON CONFLICT (ID_JULGADO) DO UPDATE SET {set_updates};"
    else:
        comando_sql = f"INSERT INTO {tabela} ({colunas_sql}) VALUES ({placeholders});"
    
    cursor.executemany(comando_sql, valores)

def executar_etl_stj():
    """ Coordena o processo de Extração, Transformação e Carregamento (ETL). """
    conn = None
    try:
        # 1. CONEXÃO E EXTRAÇÃO (E)
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {TABELA_ORIGEM} LIMIT 0;")
        nomes_colunas = [desc[0] for desc in cursor.description]

        print(f"Iniciando Extração de dados da tabela: {TABELA_ORIGEM}")
        cursor.execute(f"SELECT * FROM {TABELA_ORIGEM};")
        
        lote_fato, lote_dim_ref, lote_dim_assuntos = [], [], []
        registros_lidos = 0

        for linha_bruta in cursor:
            registros_lidos += 1
            registro_dict = dict(zip(nomes_colunas, linha_bruta))

            # 2. TRANSFORMAÇÃO (T)
            registro_fato, referencias_legais, assuntos_segmentados = tratar_registro_etl(registro_dict, nomes_colunas)
            
            if registro_fato:
                lote_fato.append(registro_fato)
                lote_dim_ref.extend(referencias_legais)
                lote_dim_assuntos.extend(assuntos_segmentados)

            # 3. CARREGAMENTO (L) - Inserção em lote
            if len(lote_fato) >= 1000:
                inserir_em_lote(cursor, TABELA_FATO, lote_fato)
                inserir_em_lote(cursor, TABELA_DIM_REF, lote_dim_ref)
                inserir_em_lote(cursor, TABELA_DIM_ASSUNTOS, lote_dim_assuntos)
                
                conn.commit()
                print(f"Processados e inseridos {registros_lidos} registros...")
                lote_fato, lote_dim_ref, lote_dim_assuntos = [], [], []
        
        # Insere os lotes restantes
        if lote_fato:
            inserir_em_lote(cursor, TABELA_FATO, lote_fato)
            inserir_em_lote(cursor, TABELA_DIM_REF, lote_dim_ref)
            inserir_em_lote(cursor, TABELA_DIM_ASSUNTOS, lote_dim_assuntos)
        
        conn.commit()
        print(f"\n--- ETL CONCLUÍDO ---")
        print(f"Total de registros lidos: {registros_lidos}")

    except (Exception, psycopg2.Error) as error:
        print(f"Erro durante o ETL: {error}")
        if conn: conn.rollback()
            
    finally:
        if conn:
            cursor.close()
            conn.close()

# =================================================================
# 4. EXECUÇÃO
# =================================================================
if __name__ == '__main__':
    print("Iniciando o processo ETL para Data Warehouse...")
    # NOTE: Certifique-se de que 'criar_schema_dw.py' foi executado primeiro.
    executar_etl_stj()