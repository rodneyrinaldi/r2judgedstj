# -*- coding: utf-8 -*-
# process_data_v2_refatorado.py - Programa de Processamento em Lote e Carregamento para a Staging Area
# ATUALIZAÇÃO: Agora utiliza uma PASTA_BASE pré-definida para todos os logs e para a busca de dados JSON.
# ATUALIZAÇÃO 2: O log de controle agora salva o caminho relativo do arquivo (pasta_imediata/arquivo.json).
import os
import json
import re
import sys 
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set

# Presume-se que este módulo lida com a inserção no banco de dados.
# O caminho de importação deve estar correto no ambiente de execução.
from data.db_insert import inserir_dados_lote 

# --- Configurações de Arquivos ---
# 🌟 PASTA_BASE DEFINITIVA: O script usará este caminho absoluto para buscar JSONs e salvar logs.
PASTA_BASE = r"D:\Sincronizado\tecnologia\data\stj-files" 

LOG_FILE_NAME = r"D:\Sincronizado\tecnologia\data\stj-files\upload_control.log" 
ERROR_LOG_FILE_NAME = r"D:\Sincronizado\tecnologia\data\stj-files\upload_status.log" 

DEBUG_MODE = True 
BATCH_SIZE = 1000 
TABLE_NAME = "staging_data" # Nome da tabela de destino

# Variáveis globais para os caminhos completos (definidas em tempo de execução)
LOG_FILE = "" 
ERROR_LOG_FILE = "" 

# Lista de campos que são do tipo TEXT na tabela de origem (Staging) e NÃO DEVEM SER TRUNCADOS.
FIELDS_TO_KEEP_LONG = [
    "descricaoClasse", "ementa", "decisao", "jurisprudenciaCitada", 
    "informacoesComplementares", "notas", "termosAuxiliares", 
    "teseJuridica", "referenciasLegislativas", "acordaosSimilares", "tema"
]

# Campos obrigatórios. Se um desses for None após o tratamento, o registro é descartado.
REQUIRED_FIELDS = ["id_origem"] 

# --- Configuração do Logging (A configuração base será feita em uma função) ---
logger = logging.getLogger(__name__)

def _setup_environment(base_path: str):
    """ 
    Configura a PASTA_BASE global, cria a estrutura de pastas e configura o logger
    para salvar o log de erros DENTRO da pasta base.
    """
    global PASTA_BASE, LOG_FILE, ERROR_LOG_FILE
    
    # PASTA_BASE é reatribuída aqui apenas para garantir que a função use o path validado
    PASTA_BASE = base_path
    
    # 1. Cria a pasta base se ela não existir
    if not os.path.exists(PASTA_BASE):
        try:
            os.makedirs(PASTA_BASE)
        except OSError as e:
            # Não usa logger aqui porque o logger pode não estar configurado corretamente
            print(f"Erro Crítico: Não foi possível criar a pasta base '{PASTA_BASE}': {e}")
            sys.exit(1)
            
    # 2. Define os caminhos completos
    LOG_FILE = os.path.join(PASTA_BASE, LOG_FILE_NAME)
    ERROR_LOG_FILE = os.path.join(PASTA_BASE, ERROR_LOG_FILE_NAME)

    # 3. Configura o Logger para usar o caminho do log de erro
    # Limpa handlers existentes para reconfigurar (garante que não haja duplicação)
    while logger.handlers:
        logger.handlers.pop()
    
    logger.setLevel(logging.INFO)
    
    # Adiciona StreamHandler para logs informativos no console
    logger.addHandler(logging.StreamHandler(sys.stdout))
    
    # Adiciona FileHandler para logs de erro e status (dentro da PASTA_BASE)
    error_file_handler = logging.FileHandler(ERROR_LOG_FILE, mode='a', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - %(name)s')
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)
    
    logger.info(f"Ambiente configurado. Logs de status em: {ERROR_LOG_FILE}")

# --- Funções de Log de Arquivo (Usando variáveis globais configuradas) ---

# --- ALTERAÇÃO ---
# Nova função auxiliar para obter o caminho relativo para o log.
def _get_relative_log_path(full_path: str, base_folder: str) -> str:
    """
    Retorna o caminho do arquivo relativo à pasta base.
    Ex: 'subpasta/arquivo.json'
    """
    return os.path.relpath(full_path, base_folder)

def carregar_log() -> Set[str]:
    """ 
    Carrega o log de arquivos já processados. O log agora contém caminhos relativos.
    """
    if not LOG_FILE:
        logger.error("LOG_FILE não configurado. O setup falhou.")
        return set()
        
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as log:
                # O log agora armazena o caminho RELATIVO (ex: subpasta/arquivo.json)
                return set(log.read().splitlines())
        except IOError as e:
            logger.error(f"Erro ao carregar log de arquivos: {e}")
            return set()
    return set()

def atualizar_log(arquivo: str):
    """ 
    --- ALTERAÇÃO ---
    Atualiza o log com o caminho relativo do arquivo processado com sucesso.
    'arquivo' ainda é o caminho completo, a conversão é feita aqui.
    """
    if not LOG_FILE:
        logger.error("LOG_FILE não configurado.")
        return

    try:
        # Converte o caminho completo para o formato relativo antes de salvar
        log_entry = _get_relative_log_path(arquivo, PASTA_BASE)
        
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"{log_entry}\n")
    except IOError as e:
        logger.error(f"Erro ao atualizar log para o arquivo {arquivo}: {e}")

# --- Funções de SQL (Sem alterações) ---

def sanitize_sql_value(value: Any) -> str:
    """ 
    Formata um valor Python para ser usado diretamente em uma string SQL.
    Adiciona aspas simples para strings e NULL para None.
    """
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        # Trata listas serializando para JSON string
        value = json.dumps(value, ensure_ascii=False)
        
    # Trata strings, escapando aspas simples e envolvendo em aspas simples
    if isinstance(value, str):
        # Substitui ' por '' (padrão SQL para aspas simples em string)
        safe_value = value.replace("'", "''") 
        return f"'{safe_value}'"
        
    return "NULL" # Caso o tipo não seja reconhecido

def gerar_sql_insert_lote(registros: List[Dict[str, Any]]) -> str:
    """ 
    Gera uma string SQL de INSERT com múltiplos VALUES para fins de log de erro.
    """
    if not registros:
        return ""
    
    # Usa as chaves do primeiro registro como base para a ordem das colunas
    colunas = registros[0].keys()
    colunas_sql = ", ".join(colunas)
    
    values_list = []
    
    for registro in registros:
        valores = []
        for coluna in colunas:
            # Garante que a ordem dos valores é a mesma ordem das colunas
            valores.append(sanitize_sql_value(registro.get(coluna)))
        
        values_list.append(f"({', '.join(valores)})")
        
    # Constrói o comando INSERT completo
    sql = (
        f"INSERT INTO {TABLE_NAME} ({colunas_sql})\n"
        f"VALUES\n"
        f"{',\n'.join(values_list)};"
    )
    
    return sql

# --- Funções de Limpeza e Validação (Sem alterações) ---

def limpar_texto(valor: Any) -> Any:
    """ Remove espaços extras, quebras de linha, tabs e normaliza espaços. """
    if isinstance(valor, str):
        # Substitui quebras de linha/tabs por um único espaço
        valor = re.sub(r"[\r\n\t]+", " ", valor)
        # Normaliza múltiplos espaços para um único
        valor = re.sub(r"\s{2,}", " ", valor)
        return valor.strip()
    return valor 

def extrair_data(valor: Any) -> str:
    """ 
    Tenta converter datas em vários formatos comuns para o formato SQL YYYY-MM-DD.
    """
    data_bruta = str(valor).strip()
    if not data_bruta:
        return ""

    formatos = [
        "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"
    ]
    
    try:
        data_para_tentar = data_bruta
        if "DATA:" in data_bruta.upper():
            data_para_tentar = data_bruta.split("DATA:")[1].strip().split(" ")[0]

        for fmt in formatos:
            try:
                data_obj = datetime.strptime(data_para_tentar, fmt)
                return data_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        return data_bruta

    except Exception:
        return data_bruta
    
def validar_inteiro(valor: Any) -> int | None:
    """ Valida e converte um valor para inteiro. """
    try:
        if valor is None or (isinstance(valor, str) and not valor.strip()):
            return None
            
        if isinstance(valor, str):
            valor = valor.strip()
            
        return int(float(valor))

    except (ValueError, TypeError) as e:
        if DEBUG_MODE:
            logger.warning(f"Falha na conversão para inteiro ('{valor}'). Erro: {e}")
        return None

def tratar_dados(dados: Dict[str, Any]) -> Dict[str, Any] | None:
    """ 
    Trata os dados, aplica as conversões de tipo, truncamento e validação de obrigatoriedade.
    """
    dados_padronizados = {}
    for key, value in dados.items():
        key_lower = key.lower()
        if key_lower == 'id_registro' or key_lower == 'id':
            dados_padronizados['id_origem'] = value
        else:
            dados_padronizados[key] = value

    dados_tratados = {}
    
    for chave, valor in dados_padronizados.items():
        valor_original = valor 
        
        # 1. Limpeza de Listas e Strings
        if isinstance(valor, list) and all(isinstance(item, str) for item in valor):
            valor = [limpar_texto(item) for item in valor]
        elif isinstance(valor, str):
            valor = limpar_texto(valor)

        # 2. Regras de Validação e Formatação (Tipos DDL)
        chave_lower = chave.lower()
        
        # Conversão de Datas
        if chave_lower in ("datadecisao", "datadisponibilizacao", "dataregistro", "dataregistrada", "datanascimento", "datarequisicao", "datapublicacao"):
            valor = extrair_data(valor_original if isinstance(valor_original, str) else str(valor))
        
        # Conversão de IDs estritamente inteiros (Exemplo: campos com "id" ou "numero" no nome)
        elif "id" in chave_lower and chave_lower not in ("id_origem"): 
             # Lógica de validação de IDs para exemplo (pode ser ajustada)
             pass 

        # 3. Truncamento condicional para VARCHARs
        if isinstance(valor, str) and chave not in FIELDS_TO_KEEP_LONG:
            
            # Tratamento específico para campos VARCHAR(50)
            if chave_lower in ("id", "numeroprocesso", "numeroregistro") and len(valor) > 50:
                if DEBUG_MODE:
                    logger.debug(f"Campo VARCHAR '{chave}' truncado de {len(valor)} para 50 caracteres.")
                valor = valor[:50]
                
            # Tratamento específico para dataPublicacao (VARCHAR(300))
            elif chave == "dataPublicacao" and len(valor) > 300:
                if DEBUG_MODE:
                    logger.debug(f"Campo VARCHAR '{chave}' truncado de {len(valor)} para 300 caracteres.")
                valor = valor[:300]
            
            # Tratamento para outros VARCHARs, limitando a 255
            elif len(valor) > 255:
                if DEBUG_MODE:
                    logger.debug(f"Campo VARCHAR '{chave}' truncado de {len(valor)} para 255 caracteres.")
                valor = valor[:255]

        dados_tratados[chave] = valor
    
    # 4. Validação de Campos Obrigatórios (Rotina de Segurança)
    for campo_obrigatorio in REQUIRED_FIELDS:
        if campo_obrigatorio not in dados_tratados or dados_tratados[campo_obrigatorio] is None:
            logger.error(f"REGISTRO DESCARTADO: Campo obrigatório '{campo_obrigatorio}' faltando/nulo. Dados parciais: {json.dumps(dados_tratados)}")
            return None 
            
    return dados_tratados

# --- Funções de Progresso e Coleta ---

def imprimir_progresso(progresso_atual: int, total_arquivos: int, arquivo_atual: str):
    """ Exibe a porcentagem de progresso. """
    if total_arquivos == 0:
        porcentagem = 100.0
    else:
        porcentagem = (progresso_atual / total_arquivos) * 100
        
    sys.stdout.write(f"\rProgresso: {porcentagem:.2f}% ({progresso_atual}/{total_arquivos} arquivos) - Último: {os.path.basename(arquivo_atual):<40}")
    sys.stdout.flush()

def coletar_arquivos(pasta: str) -> Tuple[List[str], int]:
    """ 
    --- ALTERAÇÃO ---
    Coleta arquivos JSON, comparando seus caminhos relativos com os do log.
    A 'pasta' é a PASTA_BASE, onde os arquivos JSON estão.
    """
    arquivos_a_processar = []
    arquivos_processados = carregar_log()  # Carrega o set de caminhos relativos processados
    
    arquivos_total = []
    # os.walk percorre a PASTA_BASE em busca dos JSONs
    for raiz, _, arquivos in os.walk(pasta):
        for arquivo in arquivos:
            if arquivo.endswith(".json"):
                caminho_completo_arquivo = os.path.join(raiz, arquivo)
                arquivos_total.append(caminho_completo_arquivo)
    
    for caminho_completo in arquivos_total:
        # Converte o caminho completo do arquivo atual para o formato relativo do log para comparação
        caminho_relativo_log = _get_relative_log_path(caminho_completo, pasta)
        
        if caminho_relativo_log not in arquivos_processados:
            # Adiciona o caminho COMPLETO à lista de processamento, pois é necessário para abrir o arquivo.
            arquivos_a_processar.append(caminho_completo)

    return arquivos_a_processar, len(arquivos_total)

# --- Função de Processamento Principal ---
def processar_arquivos(pasta: str):
    """
    Processa todos os arquivos JSON, acumulando e inserindo em lotes.
    A 'pasta' é a PASTA_BASE configurada.
    """
    
    arquivos_a_processar, total_arquivos_encontrados = coletar_arquivos(pasta)
    total_arquivos = len(arquivos_a_processar)
    arquivos_concluidos = 0
    
    registros_lote = [] 

    if total_arquivos == 0:
        if total_arquivos_encontrados > 0:
            logger.info(f"Todos os {total_arquivos_encontrados} arquivos JSON na pasta já foram processados (Verifique {LOG_FILE}).")
        else:
            logger.info(f"Nenhum arquivo JSON encontrado na PASTA BASE '{PASTA_BASE}' para processamento.")
        return

    logger.info(f"Arquivos JSON encontrados: {total_arquivos_encontrados}. Arquivos a processar: {total_arquivos}. Tamanho do Lote: {BATCH_SIZE}")
    
    imprimir_progresso(0, total_arquivos, "Início...")

    for caminho_arquivo in arquivos_a_processar:
        
        try:
            # A leitura do arquivo é feita com o caminho_arquivo completo
            with open(caminho_arquivo, "r", encoding="utf-8") as f:
                dados = json.load(f)

            registros = []
            if isinstance(dados, list):
                registros = dados
            elif isinstance(dados, dict):
                registros = [dados]
            else:
                logger.error(f"Formato inválido (nem lista, nem dicionário) no arquivo: {caminho_arquivo}")
                arquivos_concluidos += 1
                imprimir_progresso(arquivos_concluidos, total_arquivos, caminho_arquivo)
                continue

            for elemento in registros:
                elemento_tratado = tratar_dados(elemento)
                
                if elemento_tratado is not None:
                    
                    # Lógica para garantir consistência das chaves no lote
                    if not registros_lote and elemento_tratado:
                        registros_lote.append(elemento_tratado)
                    elif registros_lote and set(elemento_tratado.keys()) == set(registros_lote[0].keys()):
                        registros_lote.append(elemento_tratado)
                    elif registros_lote:
                        # Tenta inserir o lote atual (estrutura consistente) antes de iniciar um novo
                        try:
                            inserir_dados_lote(registros_lote) 
                            registros_lote = [] 
                            registros_lote.append(elemento_tratado) # Começa novo lote
                        except Exception as e:
                            sql_insert_completo = gerar_sql_insert_lote(registros_lote)
                            logger.critical(f"ERRO CRÍTICO DE INSERÇÃO EM LOTE. O LOTE NÃO FOI INSERIDO. Erro: {e}")
                            logger.critical("-" * 80)
                            logger.critical(f"SQL COMPLETO DO LOTE FALHO (LIMITADO):\n{sql_insert_completo[:10240]}...") 
                            logger.critical("-" * 80)
                            raise # Interrompe o processamento
                            
                    # CHECAGEM DO LOTE E INSERÇÃO DE ALTA PERFORMANCE
                    if len(registros_lote) >= BATCH_SIZE:
                        try:
                            inserir_dados_lote(registros_lote) 
                            registros_lote = [] # Reinicia a lista SÓ APÓS INSERÇÃO BEM-SUCEDIDA
                        except Exception as e:
                            sql_insert_completo = gerar_sql_insert_lote(registros_lote)
                            
                            logger.critical(f"ERRO CRÍTICO DE INSERÇÃO EM LOTE. O LOTE NÃO FOI INSERIDO. Erro: {e}")
                            logger.critical("-" * 80)
                            logger.critical(f"SQL COMPLETO DO LOTE FALHO (LIMITADO):\n{sql_insert_completo[:10240]}...")
                            logger.critical("-" * 80)
                            raise 

            # Atualiza o log de sucesso (a função atualizar_log já faz a conversão para o caminho relativo)
            atualizar_log(caminho_arquivo)
            arquivos_concluidos += 1
            imprimir_progresso(arquivos_concluidos, total_arquivos, caminho_arquivo)

        except json.JSONDecodeError as e:
            logger.error(f"Erro de formato JSON no arquivo {caminho_arquivo}: {e}")
            arquivos_concluidos += 1
            imprimir_progresso(arquivos_concluidos, total_arquivos, caminho_arquivo)
        except Exception as e:
            logger.critical(f"ERRO INESPERADO (Processamento Interrompido) no arquivo {caminho_arquivo}: {e}")
            break 
            
    # FINALIZAÇÃO: Insere o lote parcial restante, se houver
    if registros_lote:
        try:
            inserir_dados_lote(registros_lote)
        except Exception as e:
            sql_insert_completo = gerar_sql_insert_lote(registros_lote)
            
            logger.critical(f"ERRO CRÍTICO DE INSERÇÃO DO LOTE FINAL ({len(registros_lote)} registros). Estes registros não foram inseridos. Erro: {e}")
            logger.critical("-" * 80)
            logger.critical(f"SQL COMPLETO DO LOTE FINAL FALHO (LIMITADO):\n{sql_insert_completo[:10240]}...")
            logger.critical("-" * 80)
        
    # Imprime 100% final
    sys.stdout.write(f"\rProgresso: 100.00% ({arquivos_concluidos}/{total_arquivos} arquivos) - Último: {'Concluído!':<40}\n")
    sys.stdout.flush()
    
    logger.info("Processamento de dados concluído.")

if __name__ == "__main__":
    
    # 🌟 ALTERAÇÃO: O script usa a PASTA_BASE predefinida e remove o input do usuário.
    pasta_base_final = PASTA_BASE 

    # 1. Garante que a pasta base exista e configura os caminhos de log.
    _setup_environment(pasta_base_final)
    
    # 2. Inicia o processamento, usando a PASTA_BASE como root para a busca de JSONs
    if os.path.isdir(PASTA_BASE):
        logger.info(f"Iniciando processamento em lote contínuo na PASTA BASE: {PASTA_BASE}")
        try:
            # processar_arquivos usa PASTA_BASE como o diretório raiz para os arquivos JSON.
            processar_arquivos(PASTA_BASE)
        except Exception as e:
            logger.critical(f"Processamento abortado devido a um erro crítico: {e}")
    else:
        # Esta mensagem só aparecerá se a criação da pasta em _setup_environment falhar.
        logger.error(f"O caminho fornecido não é uma pasta válida ou não pôde ser criado: {PASTA_BASE}")