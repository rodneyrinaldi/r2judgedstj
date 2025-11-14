# -*- coding: utf-8 -*-
# process_data_v2.py - Programa de Processamento em Lote e Carregamento para a Staging Area
# ATUALIZAÇÃO: 'numeroprocesso' e 'numeroregistro' agora são tratados como VARCHAR(50) e não mais convertidos para INT.
import os
import json
import re
import sys 
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set
from data.db_insert import inserir_dados_lote

# --- Configurações ---
LOG_FILE = "processamento.log" # Arquivos JSON processados com sucesso
ERROR_LOG_FILE = "processamento-status.log" # Erros críticos ou de registro
DEBUG_MODE = True 
BATCH_SIZE = 1000 
TABLE_NAME = "staging_data" # Nome da tabela de destino (usado para gerar a SQL de log)

# Lista de campos que são do tipo TEXT na tabela de origem (Staging) e NÃO DEVEM SER TRUNCADOS.
FIELDS_TO_KEEP_LONG = [
    "descricaoClasse", "ementa", "decisao", "jurisprudenciaCitada", 
    "informacoesComplementares", "notas", "termosAuxiliares", 
    "teseJuridica", "referenciasLegislativas", "acordaosSimilares", "tema"
]

# Campos obrigatórios. Se um desses for None após o tratamento, o registro é descartado.
REQUIRED_FIELDS = ["id_origem"] 

# --- Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - %(name)s',
    handlers=[
        logging.StreamHandler(sys.stdout), # Logs informativos no console
        # Configura o FileHandler para logs de erro
        logging.FileHandler(ERROR_LOG_FILE, mode='a', encoding='utf-8') 
    ]
)
logger = logging.getLogger(__name__)

# --- Funções de Log de Arquivo ---

def carregar_log() -> Set[str]:
    """ Carrega o log de arquivos já processados. """
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as log:
                return set(log.read().splitlines())
        except IOError as e:
            logger.error(f"Erro ao carregar log de arquivos: {e}")
            return set()
    return set()

def atualizar_log(arquivo: str):
    """ Atualiza o log com o nome do arquivo processado com sucesso. """
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"{arquivo}\n")
    except IOError as e:
        logger.error(f"Erro ao atualizar log para o arquivo {arquivo}: {e}")

# --- Funções de SQL (Novo) ---

def sanitize_sql_value(value: Any) -> str:
    """ 
    Formata um valor Python para ser usado diretamente em uma string SQL.
    Adiciona aspas simples para strings e NULL para None.
    CUIDADO: Isso não previne SQL injection, mas serve para log de debug.
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
    Retorna uma string vazia se a lista de registros estiver vazia.
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

# --- Funções de Limpeza e Validação (Mantidas do original) ---

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
    Tenta converter datas em vários formatos comuns (incluindo Brasil e formatos de sistema como YYYYMMDD) 
    para o formato SQL YYYY-MM-DD.

    Em caso de falha na conversão, retorna o valor original como string.
    """
    # 1. Pré-processamento: Garante que é uma string e remove espaços
    data_bruta = str(valor).strip()
    
    # Se o valor for vazio, retorna-o
    if not data_bruta:
        return ""

    # Lista de formatos a tentar (Priorizando Brasil e formatos de sistema)
    formatos = [
        "%d/%m/%Y",  # Brasil Comum (DD/MM/YYYY)
        "%d-%m-%Y",  # Brasil Alternativo (DD-MM-YYYY)
        "%Y%m%d",    # Formato de Sistema Comum (YYYYMMDD)
        "%Y-%m-%d",  # Formato SQL/ISO (YYYY-MM-DD)
        "%Y/%m/%d"   # Formato Alternativo de Sistema (YYYY/MM/DD)
    ]
    
    try:
        # 2. Lógica para extrair data que pode estar aninhada ("DATA: dd/mm/yyyy")
        data_para_tentar = data_bruta
        if "DATA:" in data_bruta.upper():
            # Extrai apenas a parte da data e ignora o resto da linha
            data_para_tentar = data_bruta.split("DATA:")[1].strip().split(" ")[0]

        # 3. Tenta converter usando os formatos conhecidos
        for fmt in formatos:
            try:
                # Tenta parsear a data usando o formato atual
                data_obj = datetime.strptime(data_para_tentar, fmt)
                
                # Se for bem-sucedido, formata para o padrão SQL (YYYY-MM-DD) e retorna
                # Este é o formato "inteligível e organizado" de retorno
                return data_obj.strftime("%Y-%m-%d")
            except ValueError:
                # Tenta o próximo formato
                continue
        
        # 4. Se a conversão falhar após todas as tentativas, 
        # retorna a data bruta original, como solicitado.
        return data_bruta

    except Exception:
        # 5. Em caso de qualquer erro inesperado (como falha no split, etc.),
        # retorna a data bruta original para evitar gerar erro.
        return data_bruta
    

def validar_inteiro(valor: Any) -> int | None:
    """ Valida e converte um valor para inteiro. """
    try:
        if valor is None or (isinstance(valor, str) and not valor.strip()):
            return None
            
        if isinstance(valor, str):
            valor = valor.strip()
            
        # Tenta a conversão robusta (float(valor) para lidar com "1.0")
        return int(float(valor))

    except (ValueError, TypeError) as e:
        if DEBUG_MODE:
            logger.warning(f"Falha na conversão para inteiro ('{valor}'). Erro: {e}")
        return None

def tratar_dados(dados: Dict[str, Any]) -> Dict[str, Any] | None:
    """ 
    Trata os dados, aplica as conversões de tipo, truncamento e validação de obrigatoriedade.
    Retorna o dicionário tratado ou None se o registro for inválido/descartado.
    """
    # Dicionário temporário para mapear chaves inconsistentes para as chaves do DDL da Staging.
    dados_padronizados = {}
    for key, value in dados.items():
        # Padroniza a chave de ID principal (id_registro/id) para 'id_origem'
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
        
        # Conversão de IDs estritamente inteiros (numeroprocesso e numeroregistro agora são VARCHAR)
        elif "xxxxx" in chave_lower:
             valor = validar_inteiro(valor)

        # 3. Truncamento condicional para VARCHARs
        if isinstance(valor, str) and chave not in FIELDS_TO_KEEP_LONG:
            
            # NOVO: Tratamento específico para campos VARCHAR(50)
            if chave_lower in ("id", "numeroprocesso", "numeroregistro") and len(valor) > 50:
                 if DEBUG_MODE:
                     logger.debug(f"Campo VARCHAR '{chave}' truncado de {len(valor)} para 50 caracteres.")
                 valor = valor[:50]
                 
            # Tratamento específico para dataPublicacao (VARCHAR(300))
            elif chave == "dataPublicacao" and len(valor) > 300:
                 if DEBUG_MODE:
                     logger.debug(f"Campo VARCHAR '{chave}' truncado de {len(valor)} para 300 caracteres.")
                 # O valor já foi limpo, então não usamos valor_original aqui
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
            # Loga o registro específico para inspeção
            logger.error(f"REGISTRO DESCARTADO: Campo obrigatório '{campo_obrigatorio}' faltando/nulo. Dados parciais: {json.dumps(dados_tratados)}")
            return None 
            
    return dados_tratados

# --- Funções de Progresso e Coleta (Mantidas do original) ---

def imprimir_progresso(progresso_atual: int, total_arquivos: int, arquivo_atual: str):
    """ Exibe a porcentagem de progresso. """
    if total_arquivos == 0:
        porcentagem = 100.0
    else:
        porcentagem = (progresso_atual / total_arquivos) * 100
        
    sys.stdout.write(f"\rProgresso: {porcentagem:.2f}% ({progresso_atual}/{total_arquivos} arquivos) - Último: {os.path.basename(arquivo_atual):<40}")
    sys.stdout.flush()

def coletar_arquivos(pasta: str) -> Tuple[List[str], int]:
    """ Coleta todos os arquivos JSON e separa os a processar dos já processados. """
    arquivos_a_processar = []
    arquivos_processados = carregar_log()
    
    arquivos_total = []
    for raiz, _, arquivos in os.walk(pasta):
        for arquivo in arquivos:
            if arquivo.endswith(".json"):
                caminho_arquivo = os.path.join(raiz, arquivo)
                arquivos_total.append(caminho_arquivo)
    
    for caminho in arquivos_total:
        if caminho not in arquivos_processados:
            arquivos_a_processar.append(caminho)

    return arquivos_a_processar, len(arquivos_total)

# --- Função de Processamento Principal (Alterada para logar o SQL) ---
def processar_arquivos(pasta: str):
    """
    Processa todos os arquivos JSON, acumulando e inserindo em lotes.
    """
    
    arquivos_a_processar, total_arquivos_encontrados = coletar_arquivos(pasta)
    total_arquivos = len(arquivos_a_processar)
    arquivos_concluidos = 0
    
    registros_lote = [] 

    if total_arquivos == 0:
        if total_arquivos_encontrados > 0:
            logger.info(f"Todos os {total_arquivos_encontrados} arquivos JSON na pasta já foram processados (Verifique {LOG_FILE}).")
        else:
            logger.info("Nenhum arquivo JSON encontrado na pasta para processamento.")
        return

    logger.info(f"Arquivos JSON encontrados: {total_arquivos_encontrados}. Arquivos a processar: {total_arquivos}. Tamanho do Lote: {BATCH_SIZE}")
    
    imprimir_progresso(0, total_arquivos, "Início...")

    for caminho_arquivo in arquivos_a_processar:
        
        try:
            with open(caminho_arquivo, "r", encoding="utf-8") as f:
                dados = json.load(f)

            registros = []
            if isinstance(dados, list):
                registros = dados
            elif isinstance(dados, dict):
                registros = [dados]
            else:
                logger.error(f"Formato inválido (nem lista, nem dicionário) no arquivo: {caminho_arquivo}")
                # Não atualiza o log para que o arquivo possa ser verificado e corrigido.
                arquivos_concluidos += 1
                imprimir_progresso(arquivos_concluidos, total_arquivos, caminho_arquivo)
                continue

            for elemento in registros:
                elemento_tratado = tratar_dados(elemento)
                
                # Somente adiciona ao lote se o tratamento for bem-sucedido (não for None)
                if elemento_tratado is not None:
                    # Garantir que todos os registros no lote tenham o mesmo conjunto de chaves
                    # (necessário para a geração de SQL)
                    if not registros_lote and elemento_tratado:
                         # Primeiro elemento: usa-o como base
                        registros_lote.append(elemento_tratado)
                    elif registros_lote and set(elemento_tratado.keys()) == set(registros_lote[0].keys()):
                        # Elementos subsequentes: se as chaves coincidirem, adiciona
                        registros_lote.append(elemento_tratado)
                    elif registros_lote and set(elemento_tratado.keys()) != set(registros_lote[0].keys()):
                         # Caso as chaves sejam diferentes, é melhor forçar a inserção do lote atual 
                         # antes de começar um novo sub-lote com estrutura diferente.
                        
                        # TENTA INSERIR O LOTE ATUAL (COM ESTRUTURA CONSISTENTE)
                        try:
                            inserir_dados_lote(registros_lote) 
                            # Se a inserção do lote anterior for bem-sucedida, ele é limpo
                            registros_lote = [] 
                            # Adiciona o elemento com a nova estrutura para começar o novo lote
                            registros_lote.append(elemento_tratado)
                        except Exception as e:
                            # Loga o erro crítico, incluindo o SQL completo gerado
                            sql_insert_completo = gerar_sql_insert_lote(registros_lote)
                            logger.critical(f"ERRO CRÍTICO DE INSERÇÃO EM LOTE. O LOTE NÃO FOI INSERIDO. Erro: {e}")
                            logger.critical("-" * 80)
                            # Limita a 10KB para evitar logs excessivamente grandes, mas mantém o máximo possível
                            logger.critical(f"SQL COMPLETO DO LOTE FALHO (LIMITADO):\n{sql_insert_completo[:10240]}...") 
                            logger.critical("-" * 80)
                            raise # Interrompe o processamento

                
                # CHECAGEM DO LOTE E INSERÇÃO DE ALTA PERFORMANCE
                if len(registros_lote) >= BATCH_SIZE:
                    try:
                        inserir_dados_lote(registros_lote) 
                        registros_lote = [] # Reinicia a lista SÓ APÓS INSERÇÃO BEM-SUCEDIDA
                    except Exception as e:
                        # GERAÇÃO DA QUERY SQL PARA O LOG DE ERRO (NOVA FUNCIONALIDADE)
                        sql_insert_completo = gerar_sql_insert_lote(registros_lote)
                        
                        # Loga o erro crítico, incluindo o SQL completo gerado
                        logger.critical(f"ERRO CRÍTICO DE INSERÇÃO EM LOTE. O LOTE NÃO FOI INSERIDO. Erro: {e}")
                        logger.critical("-" * 80)
                        logger.critical(f"SQL COMPLETO DO LOTE FALHO (LIMITADO):\n{sql_insert_completo[:10240]}...")
                        logger.critical("-" * 80)
                        # Interrompe o processamento, pois há um erro de banco de dados
                        raise 

            atualizar_log(caminho_arquivo)
            arquivos_concluidos += 1
            imprimir_progresso(arquivos_concluidos, total_arquivos, caminho_arquivo)

        except json.JSONDecodeError as e:
            logger.error(f"Erro de formato JSON no arquivo {caminho_arquivo}: {e}")
            arquivos_concluidos += 1
            imprimir_progresso(arquivos_concluidos, total_arquivos, caminho_arquivo)
        except Exception as e:
            # Captura a exceção e loga, interrompendo o fluxo se for um erro crítico de inserção
            logger.critical(f"ERRO INESPERADO (Processamento Interrompido) no arquivo {caminho_arquivo}: {e}")
            break # Interrompe o loop de arquivos
            
    # FINALIZAÇÃO: Insere o lote parcial restante, se houver
    if registros_lote:
        try:
            inserir_dados_lote(registros_lote)
        except Exception as e:
            # GERAÇÃO DA QUERY SQL PARA O LOG DE ERRO DO LOTE FINAL
            sql_insert_completo = gerar_sql_insert_lote(registros_lote)
            
            # Tratamento de erro robusto para o lote final (EVITA PERDA SILENCIOSA DE REGISTROS)
            logger.critical(f"ERRO CRÍTICO DE INSERÇÃO DO LOTE FINAL ({len(registros_lote)} registros). Estes registros não foram inseridos. Erro: {e}")
            logger.critical("-" * 80)
            logger.critical(f"SQL COMPLETO DO LOTE FINAL FALHO (LIMITADO):\n{sql_insert_completo[:10240]}...")
            logger.critical("-" * 80)
        
    # Imprime 100% final
    sys.stdout.write(f"\rProgresso: 100.00% ({arquivos_concluidos}/{total_arquivos} arquivos) - Último: {'Concluído!':<40}\n")
    sys.stdout.flush()
    
    logger.info("Processamento de dados concluído.")

if __name__ == "__main__":
    pasta_base = input("Digite o caminho da pasta a ser processada (padrão: ./files/process_stj): ").strip()
    if not pasta_base:
        pasta_base = "./files/process_stj"

    if os.path.isdir(pasta_base):
        logger.info(f"Iniciando processamento em lote contínuo na pasta: {pasta_base}")
        try:
            processar_arquivos(pasta_base)
        except Exception as e:
            # Captura exceções que podem ter sido levantadas (como no erro crítico de inserção)
            logger.critical(f"Processamento abortado devido a um erro crítico: {e}")
    else:
        logger.error(f"O caminho fornecido não é uma pasta válida: {pasta_base}")
