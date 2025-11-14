import os
import json
import re
import sys 
from datetime import datetime
import math 
from collections import defaultdict
from typing import Dict, Any, List, Tuple, Union

# --- Configurações ---
# Arquivo de saída para o relatório de dimensionamento e estatísticas.
RELATORIO_FILE = "check-files.log" 

# Lista de campos que são do tipo TEXT no DDL (Data Definition Language)
# e devem ser mantidos como TEXT (para evitar truncamento).
FIELDS_TO_KEEP_LONG = [
    "descricaoClasse", "ementa", "decisao", "jurisprudenciaCitada", "notas", 
    "informacoesComplementares", "termosAuxiliares", "teseJuridica", 
    "referenciasLegislativas", "acordaosSimilares",
    "tema" 
]

# Tamanho VARCHAR padrão para campos que apareceram, mas que tinham valor vazio ('').
DEFAULT_VARCHAR_SIZE = 50 

# --- Variável Global para Monitoramento ---
# Estrutura para rastrear o tipo e tamanho/valor máximo de cada campo.
MAX_FIELD_SIZES = defaultdict(lambda: {'type': 'unknown', 'max_len': 0, 'max_val': -math.inf})

# Pré-carrega campos de texto longo (TEXT) para garantir que sejam tratados como 'string'.
for field in FIELDS_TO_KEEP_LONG:
    MAX_FIELD_SIZES[field]['type'] = 'string'
    
# --- Funções Auxiliares de Dimensionamento ---

def limpar_texto(valor: Any) -> Any:
    """ 
    Rotina de Segurança: Remove espaços extras, quebras de linha, tabs e normaliza espaços.
    Retorna o valor original se não for string, garantindo a preservação do tipo.
    """
    if isinstance(valor, str):
        # Substitui quebras de linha/tabs por um único espaço
        valor = re.sub(r"[\r\n\t]+", " ", valor)
        # Normaliza múltiplos espaços para um único
        valor = re.sub(r"\s{2,}", " ", valor)
        return valor.strip()
    return valor 

def validar_inteiro(valor: Any) -> Union[int, None]:
    """ 
    Rotina de Validação de Tamanho: Tenta converter um valor para inteiro.
    Isto é usado APENAS para determinar se um campo numérico cabe em um SMALLINT, INTEGER ou BIGINT.
    Não adapta strings arbitrárias (como datas) para int.
    """
    try:
        if isinstance(valor, str):
            valor = valor.strip()
            if not valor:
                return None
        
        # Tenta a conversão robusta (float(valor) para lidar com strings como "1.0")
        if isinstance(valor, (float, str)):
             return int(float(valor))

        return int(valor)

    except (ValueError, TypeError):
        # Em caso de falha (ex: string "texto"), retorna None, e o campo será tratado como string
        return None

def atualizar_max_size(chave_original: str, valor_tratado: Any):
    """ Registra o tamanho/valor máximo encontrado para cada atributo. """
    global MAX_FIELD_SIZES
    
    # Padroniza a chave 'id' para evitar conflitos com IDs internos do DB
    chave = 'id_origem' if chave_original == 'id' else chave_original
    
    if isinstance(valor_tratado, str):
        tamanho = len(valor_tratado)
        if MAX_FIELD_SIZES[chave]['type'] in ('unknown', 'string'):
            MAX_FIELD_SIZES[chave]['type'] = 'string'
            MAX_FIELD_SIZES[chave]['max_len'] = max(MAX_FIELD_SIZES[chave]['max_len'], tamanho)

    elif isinstance(valor_tratado, int):
        # Rastreamos o valor absoluto para dimensionar o tipo INT corretamente
        valor_abs = abs(valor_tratado)
        if MAX_FIELD_SIZES[chave]['type'] in ('unknown', 'int'):
            MAX_FIELD_SIZES[chave]['type'] = 'int'
            MAX_FIELD_SIZES[chave]['max_val'] = max(MAX_FIELD_SIZES[chave]['max_val'], valor_abs)
    
def tratar_dados_apenas_para_validacao(dados: Dict[str, Any]):
    """ 
    Rotina de Dimensionamento: Percorre o registro e atualiza o tamanho máximo encontrado
    para cada campo. NÃO realiza adaptação semântica de tipos (data -> date, etc.).
    """
    for chave, valor in dados.items():
        # Ignora listas ou objetos aninhados (o relatório de DDL não os dimensiona diretamente)
        if isinstance(valor, (list, dict)):
            continue 

        # 1. Limpeza e Preservação de Tipo
        # O valor limpo é usado para o cálculo de tamanho, mas o tipo é preservado.
        valor_limpo = limpar_texto(valor)

        # 2. Heurística para campos que PODEM ser numéricos (para dimensionar INT)
        if "numero" in chave.lower() or "id" in chave.lower():
            valor_numerico = validar_inteiro(valor_limpo)
            if valor_numerico is not None:
                atualizar_max_size(chave, valor_numerico)
                continue # Se foi tratado como INT, pula o tratamento STRING

        # 3. Tratamento Padrão (Tudo o que restou é tratado como STRING)
        # Campos que parecem data, float ou booleanos são tratados como string para preservar o formato.
        if isinstance(valor_limpo, str):
            atualizar_max_size(chave, valor_limpo)
        elif isinstance(valor_limpo, (int, float, bool)):
            # Converte tipos primitivos não rastreados acima para string para fins de tamanho (ex: booleanos)
            atualizar_max_size(chave, str(valor_limpo))
        # Note: Valores None são ignorados e não afetam o dimensionamento.

# --- Funções de Relatório e Estatísticas ---

def gerar_relatorio_final(estatisticas: Dict[str, int]):
    """ 
    Gera o relatório final de dimensionamento, estatísticas e previsão DDL.
    """
    
    total_registros_disponiveis = estatisticas['iteracoes_processadas']
    
    # Filtra apenas chaves válidas (que apareceram ou que são TEXT longo predefinido)
    valid_keys = {
        k: v for k, v in MAX_FIELD_SIZES.items() 
        if v['type'] != 'unknown' or k in FIELDS_TO_KEEP_LONG
    }
    
    if not valid_keys:
        print("Nenhum dado válido encontrado para gerar o relatório.", file=sys.stderr)
        return
        
    try:
        with open(RELATORIO_FILE, "w", encoding="utf-8") as f:
            f.write("##########################################################\n")
            f.write("### RELATÓRIO DE DIMENSIONAMENTO E INVENTÁRIO          ###\n")
            f.write("##########################################################\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("==========================================================\n")
            f.write("### ESTATÍSTICAS DE PROCESSAMENTO GERAL\n")
            f.write(f"Total de Pastas Lidas: {estatisticas['pastas_lidas']}\n")
            f.write(f"Total de Arquivos JSON Encontrados: {estatisticas['arquivos_encontrados']}\n")
            f.write("----------------------------------------------------------\n")
            f.write("### INVENTÁRIO DE DADOS DISPONÍVEIS NO DISCO\n")
            f.write(f"TOTAL DE REGISTROS (OBJETOS) DISPONÍVEIS PARA PROCESSAMENTO: {total_registros_disponiveis} registros\n")
            f.write("==========================================================\n")
            f.write("### DIMENSIONAMENTO DE CAMPO (DDL BRUTO SUGERIDO)\n")
            f.write("{:<30} | {:<10} | {:<20}\n".format("ATRIBUTO (DB)", "TIPO DETECTADO", "TAMANHO MÁXIMO/VALOR"))
            f.write("----------------------------------------------------------\n")
            
            for chave, dados in sorted(valid_keys.items()):
                tipo = dados.get('type', 'unknown')
                is_long_field = chave in FIELDS_TO_KEEP_LONG
                
                # --- Lógica de Sugestão DDL ---
                if is_long_field or (tipo == 'string' and dados['max_len'] > 255):
                    # Tipo TEXT
                    sugestao = "TEXT"
                    max_len = dados['max_len'] if tipo == 'string' else 'N/A'
                    if is_long_field:
                        sugestao += " (Texto Longo Predefinido)"
                    f.write("{:<30} | {:<10} | {:<20} (Sugestão: {})\n".format(
                        chave, 'STRING', max_len, sugestao))
                
                elif tipo == 'string':
                    # Tipo VARCHAR (Tamanho ajustado com margem de segurança)
                    max_len = dados['max_len']
                    if max_len == 0:
                        max_len_display = f"0 (Default {DEFAULT_VARCHAR_SIZE})"
                        sugestao = f"VARCHAR({DEFAULT_VARCHAR_SIZE})"
                    else:
                        # Aplica margem de segurança de 25% (1.25)
                        tamanho_base = max_len * 1.25 
                        
                        # Arredonda para o múltiplo mais próximo de 10, 50 ou 100
                        if tamanho_base < 100:
                            arredondado = int(math.ceil(tamanho_base / 10.0)) * 10
                            arredondado = max(arredondado, 50) # Mínimo 50
                        elif tamanho_base < 500:
                            arredondado = int(math.ceil(tamanho_base / 50.0)) * 50
                        else:
                            arredondado = int(math.ceil(tamanho_base / 100.0)) * 100
                            
                        sugestao = f"VARCHAR({arredondado})"
                        max_len_display = max_len
                        
                    f.write("{:<30} | {:<10} | {:<20} (Sugestão: {})\n".format(
                        chave, 'STRING', max_len_display, sugestao))
                        
                elif tipo == 'int':
                    # Tipo INT (Baseado no valor máximo encontrado)
                    max_val = dados['max_val']
                    if max_val <= 32767:
                        sugestao = "SMALLINT"
                    elif max_val <= 2147483647:
                        sugestao = "INTEGER"
                    else:
                        sugestao = "BIGINT"
                        
                    f.write("{:<30} | {:<10} | {:<20} (Sugestão: {})\n".format(
                        chave, 'INT', max_val, sugestao))
                
                elif tipo == 'unknown':
                    # Chaves que apareceram, mas nunca receberam um valor válido (ex: sempre nulo)
                    sugestao = f"VARCHAR({DEFAULT_VARCHAR_SIZE}) (Chave sem Amostra)"
                    f.write("{:<30} | {:<10} | {:<20} (Sugestão: {})\n".format(
                        chave, 'UNKNOWN', 'N/A', sugestao))

            f.write("----------------------------------------------------------\n")
            print(f"\n[SUCESSO] Relatório de dimensionamento e estatísticas salvo em: {RELATORIO_FILE}")
            
    except Exception as e:
        print(f"\nErro ao gerar o relatório de dimensionamento: {e}", file=sys.stderr)

# --- Função de Validação Principal ---

def validar_dimensionamento(pasta: str):
    """
    Escaneia todos os arquivos JSON para validar o tamanho dos campos e coletar estatísticas,
    contando o total de registros disponíveis no disco.
    """
    
    estatisticas = {
        'arquivos_encontrados': 0,
        'pastas_lidas': 0,
        'iteracoes_processadas': 0, # Total de todos os registros (contagem de inventário)
    }
    
    # Conjunto para rastrear chaves totais encontradas no disco
    chaves_totais = set(MAX_FIELD_SIZES.keys()) 

    print("Iniciando varredura e validação de dimensionamento robusto...")
    
    for raiz, _, arquivos in os.walk(pasta):
        estatisticas['pastas_lidas'] += 1 
        
        for arquivo in arquivos:
            if arquivo.endswith(".json"):
                caminho_arquivo = os.path.join(raiz, arquivo)
                estatisticas['arquivos_encontrados'] += 1
                
                # Exibe o progresso no console
                sys.stdout.write(f"\rEscaneando arquivo: {os.path.basename(caminho_arquivo):<50}")
                sys.stdout.flush()

                try:
                    # Rotina de segurança: Abrindo e carregando JSON
                    with open(caminho_arquivo, "r", encoding="utf-8") as f:
                        dados = json.load(f)

                    registros = []
                    if isinstance(dados, list):
                        registros = dados
                    elif isinstance(dados, dict):
                        registros = [dados]
                    else:
                        # Ignora formatos inválidos, mas continua
                        continue

                    for elemento in registros:
                        # A contagem de inventário é a primeira ação (não depende do sucesso do tratamento)
                        estatisticas['iteracoes_processadas'] += 1 
                        
                        # Rotina de segurança: Identifica novas chaves no registro
                        for chave in elemento.keys():
                            chave_no_mapa = 'id_origem' if chave == 'id' else chave
                            if chave_no_mapa not in chaves_totais:
                                # Inicializa a chave com 'unknown'
                                MAX_FIELD_SIZES[chave_no_mapa]['type'] = 'unknown' 
                                chaves_totais.add(chave_no_mapa)

                        # Atualiza o dimensionamento do campo
                        tratar_dados_apenas_para_validacao(elemento)

                except json.JSONDecodeError as e:
                    # Rotina de segurança: Erro no formato JSON
                    print(f"\n[ERRO GRAVE] Falha no formato JSON do arquivo {caminho_arquivo}: {e}", file=sys.stderr)
                except Exception as e:
                    # Rotina de segurança: Erro de I/O, permissão, etc.
                    print(f"\n[ERRO CRÍTICO] Falha ao processar arquivo {caminho_arquivo}: {e}", file=sys.stderr)

    print(f"\rVarredura concluída. Total de arquivos JSON escaneados: {estatisticas['arquivos_encontrados']}. {' ' * 20}")
    
    if estatisticas['arquivos_encontrados'] > 0:
        gerar_relatorio_final(estatisticas)
    else:
        print("Nenhum arquivo JSON encontrado para validação.")


if __name__ == "__main__":
    pasta_base = input("Digite o caminho da pasta a ser validada (padrão: ./files/process_stj): ").strip()
    if not pasta_base:
        pasta_base = "./files/process_stj"

    if os.path.isdir(pasta_base):
        validar_dimensionamento(pasta_base)
        print("Validação de dimensionamento e estatísticas concluída.")
    else:
        print(f"O caminho fornecido não é uma pasta válida: {pasta_base}", file=sys.stderr)
