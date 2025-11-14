import argparse
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from typing import Set
import logging

# ==============================================================================
# 🌟 BASE_PATH: Definida como o diretório final para logs e downloads.
# As subpastas de download (ex: 'corte-especial') serão criadas DENTRO dela.
# ==============================================================================
BASE_PATH = r"D:\Sincronizado\tecnologia\data\stj-files" 
# Usamos 'r' (raw string) para tratar corretamente as barras invertidas do Windows.

# Lista de todas as URLs a serem processadas.
TARGET_URLS = [
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-corte-especial",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-primeira-secao",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-segunda-secao",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-terceira-secao",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-primeira-turma",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-segunda-turma",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-terceira-turma",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-quarta-turma",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-quinta-turma",
    "https://dadosabertos.web.stj.jus.br/dataset/espelhos-de-acordaos-sexta-turma"
]

# Nome do arquivo de controle. Ele será criado DENTRO de cada subpasta de download.
CONTROL_FILENAME = "download_control.txt"

# Configuração do Logging
def setup_logging(base_path: str):
    """Configura o logger para imprimir no console e salvar em um arquivo de log, usando base_path."""
    
    # Garante que a pasta base exista para salvar o log
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    # Define o formato da mensagem de log
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Configura o logger raiz
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # Define o nível mínimo de logs a serem processados

    # Handler para console (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    # Handler para arquivo - AGORA USA A BASE_PATH
    log_filepath = os.path.join(base_path, 'stj_scraper.log')
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    
    logging.info("----------------------------------------------------------------------------------")
    logging.info(f"Configuração de log concluída. Logando no console e em '{log_filepath}'.")
    logging.info("----------------------------------------------------------------------------------")


def load_downloaded_files(control_filepath: str) -> Set[str]:
    """Carrega a lista de nomes de arquivos já baixados do arquivo de controle específico da subpasta."""
    downloaded_files = set()
    if os.path.exists(control_filepath):
        logging.info(f" -> Carregando controle de '{control_filepath}'...")
        try:
            with open(control_filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    downloaded_files.add(line.strip())
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo de controle '{control_filepath}'. Erro: {e}")
            
    return downloaded_files

def get_filename_from_url(url, response_headers=None):
    """Tenta obter o nome do arquivo a partir do cabeçalho ou da URL, e o sanitiza."""
    filename = 'downloaded_file'

    if response_headers and 'content-disposition' in response_headers:
        cd = response_headers['content-disposition']
        filename_start = cd.find('filename=')
        if filename_start != -1:
            filename = cd[filename_start + len('filename='):].strip('\"')
    
    if filename == 'downloaded_file':
        path = urlparse(url).path
        filename = os.path.basename(path) or 'downloaded_file'

    # Sanitiza o nome do arquivo, removendo caracteres inválidos para sistemas de arquivos
    return "".join(c for c in filename if c.isalnum() or c in ('.', '_', '-')).strip()


def download_file(url: str, download_dir: str, filename: str, control_filepath: str, downloaded_files_set: Set[str], pause_seconds: int = 3):
    """
    Baixa um arquivo, verifica o arquivo de controle ESPECÍFICO da subpasta e atualiza o registro.
    download_dir JÁ CONTÉM A BASE_PATH.
    """
    
    # 1. Checagem de arquivo já baixado
    if filename in downloaded_files_set:
        logging.info(f" -> PULANDO: Arquivo '{filename}' já está no controle desta pasta.")
        return False
    
    # 2. Pausa antes do download
    logging.info(f" -> Pausando por {pause_seconds}s e tentando baixar: {filename}...")
    time.sleep(pause_seconds)
    
    # Cria o caminho completo do arquivo - download_dir JÁ CONTÉM A BASE_PATH
    filepath = os.path.join(download_dir, filename)
    
    try:
        # 3. Requisição HTTP
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # 4. Escreve o arquivo em blocos
        # filepath JÁ CONTÉM A BASE_PATH
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        # 5. Atualiza o arquivo de controle e o set em memória
        # control_filepath JÁ CONTÉM A BASE_PATH
        with open(control_filepath, 'a', encoding='utf-8') as f:
            f.write(filename + '\n')
            
        downloaded_files_set.add(filename)
        
        logging.info(f" -> SUCESSO: Arquivo salvo em '{filepath}' e controle ATUALIZADO.")
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f" -> ERRO (Requisição): Não foi possível baixar '{url}'. Erro: {e}")
        # Tenta remover o arquivo parcial se ele existir
        if os.path.exists(filepath):
               os.remove(filepath)
               logging.warning(f" -> Aviso: Arquivo parcial '{filepath}' foi removido.")
        return False
    except Exception as e:
        logging.error(f" -> ERRO (Geral): Erro inesperado ao processar o download. Erro: {e}")
        return False


def process_page(url: str, base_download_dir: str):
    """
    Processa uma única página, define a subpasta, carrega o controle específico e inicia os downloads.
    base_download_dir é a BASE_PATH final.
    """

    # 1. Define e cria a subpasta (DENTRO da BASE_PATH)
    subfolder_name = url.split('/')[-1]
    # download_dir AGORA É BASE_PATH/subfolder_name
    download_dir = os.path.join(base_download_dir, subfolder_name) 
    control_filepath = os.path.join(download_dir, CONTROL_FILENAME)
    
    # Cria a subpasta se ela não existir
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    logging.info(f"\n========================================================")
    logging.info(f"Processando URL: {url}")
    logging.info(f" -> Destino: {download_dir}")
    logging.info(f"========================================================")
    
    # 2. Carrega o controle de download ESPECÍFICO desta subpasta
    downloaded_files_set = load_downloaded_files(control_filepath)
    logging.info(f" -> {len(downloaded_files_set)} arquivos já registrados nesta pasta.")
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Seletor CSS para os links de download (o segundo <a> dentro da estrutura)
        download_links = soup.select('#dataset-resources > ul > li > div > ul > li:nth-child(2) > a')

        if not download_links:
            logging.warning(" -> Nenhum link de download encontrado no seletor esperado. Pulando página.")
            return

        logging.info(f" -> Encontrados {len(download_links)} links de recurso nesta página.")

        for i, link in enumerate(download_links):
            href = link.get('href')
            
            if href:
                full_url = urljoin(url, href)
                preliminary_filename = get_filename_from_url(full_url)
                
                # 3. Chama a função de download
                download_file(
                    url=full_url, 
                    download_dir=download_dir, # BASE_PATH/subpasta
                    filename=preliminary_filename,
                    control_filepath=control_filepath, # BASE_PATH/subpasta/controle.txt
                    downloaded_files_set=downloaded_files_set 
                )
            else:
                logging.warning(f" -> Aviso: Link {i + 1} sem atributo href. Pulando.")
        
    except requests.exceptions.RequestException as e:
        logging.error(f" -> ERRO na Requisição para {url}: {e}")
    except Exception as e:
        logging.error(f" -> ERRO Inesperado ao processar a página: {e}")


def run_all_scrapers(urls: list, base_download_dir: str):
    """Orquestra o processo de download em todas as URLs."""
    
    for url in urls:
        process_page(url, base_download_dir)

    logging.info("\n--------------------------------------------------------")
    logging.info("RODADA DE DOWNLOADS CONCLUÍDA.")
    logging.info("--------------------------------------------------------")


# --- Inicialização ---

if __name__ == "__main__":
    
    # A lógica de argparse foi removida para usar BASE_PATH diretamente.
    
    # 1. Garante que a BASE_PATH exista para logs e downloads.
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        logging.info(f"Diretório BASE '{BASE_PATH}' criado.")
        
    setup_logging(BASE_PATH) # 2. Configura as rotinas de log, usando BASE_PATH
    
    # 3. Execução: BASE_PATH é o diretório final para o download.
    run_all_scrapers(TARGET_URLS, BASE_PATH)