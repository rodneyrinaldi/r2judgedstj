import argparse
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from typing import Set

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

# Nome do arquivo de controle. Ele será criado DENTRO de cada subpasta.
CONTROL_FILENAME = "download_control.txt"

def load_downloaded_files(control_filepath: str) -> Set[str]:
    """Carrega a lista de nomes de arquivos já baixados do arquivo de controle específico da subpasta."""
    downloaded_files = set()
    if os.path.exists(control_filepath):
        print(f"   -> Carregando controle de '{control_filepath}'...")
        with open(control_filepath, 'r', encoding='utf-8') as f:
            for line in f:
                downloaded_files.add(line.strip())
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
    """
    
    # 1. Checagem de arquivo já baixado
    # A checagem agora é feita apenas contra o set carregado DO ARQUIVO DE CONTROLE DA SUBPASTA ATUAL.
    if filename in downloaded_files_set:
        print(f"   -> PULANDO: Arquivo '{filename}' já está no controle desta pasta.")
        return False
    
    # 2. Pausa antes do download
    print(f"   -> Pausando por {pause_seconds}s e tentando baixar: {filename}...")
    time.sleep(pause_seconds)
    
    filepath = os.path.join(download_dir, filename)
    
    try:
        # 3. Requisição HTTP
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # 4. Escreve o arquivo em blocos
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        # 5. Atualiza o arquivo de controle e o set em memória
        # O arquivo de controle é o CONTROL_FILENAME dentro de download_dir (a subpasta)
        with open(control_filepath, 'a', encoding='utf-8') as f:
            f.write(filename + '\n')
            
        downloaded_files_set.add(filename)
        
        print(f"   -> SUCESSO: Arquivo salvo em '{filepath}' e controle ATUALIZADO.")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"   -> ERRO (Requisição): Não foi possível baixar '{url}'. Erro: {e}")
        return False
    except Exception as e:
        print(f"   -> ERRO (Geral): Erro inesperado ao processar o download. Erro: {e}")
        return False


def process_page(url: str, base_download_dir: str):
    """
    Processa uma única página, define a subpasta, carrega o controle específico e inicia os downloads.
    """

    # 1. Define e cria a subpasta
    # Extrai o nome da subpasta (tudo após o último '/')
    subfolder_name = url.split('/')[-1]
    
    # Cria o caminho completo para a subpasta
    download_dir = os.path.join(base_download_dir, subfolder_name)
    
    # Define o caminho do arquivo de controle DENTRO DA SUBPASTA
    control_filepath = os.path.join(download_dir, CONTROL_FILENAME)
    
    # Cria a subpasta se ela não existir
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        
    print(f"\n========================================================")
    print(f"Processando URL: {url}")
    print(f"   -> Destino: {download_dir}")
    print(f"========================================================")
    
    # 2. Carrega o controle de download ESPECÍFICO desta subpasta
    downloaded_files_set = load_downloaded_files(control_filepath)
    print(f"   -> {len(downloaded_files_set)} arquivos já registrados nesta pasta.")
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Seletor CSS para os links de download (o segundo <a> dentro da estrutura)
        download_links = soup.select('#dataset-resources > ul > li > div > ul > li:nth-child(2) > a')

        if not download_links:
            print("   -> Nenhum link de download encontrado no seletor esperado. Pulando página.")
            return

        print(f"   -> Encontrados {len(download_links)} links de recurso nesta página.")

        for i, link in enumerate(download_links):
            href = link.get('href')
            
            if href:
                full_url = urljoin(url, href)
                preliminary_filename = get_filename_from_url(full_url)
                
                # 3. Chama a função de download
                download_file(
                    url=full_url, 
                    download_dir=download_dir, # Subpasta
                    filename=preliminary_filename,
                    control_filepath=control_filepath, # Caminho do controle da subpasta
                    downloaded_files_set=downloaded_files_set # Set do controle da subpasta
                )
            else:
                print(f"   -> Aviso: Link {i + 1} sem atributo href. Pulando.")
        
    except requests.exceptions.RequestException as e:
        print(f"   -> ERRO na Requisição para {url}: {e}")
    except Exception as e:
        print(f"   -> ERRO Inesperado ao processar a página: {e}")


def run_all_scrapers(urls: list, base_download_dir: str):
    """Orquestra o processo de download em todas as URLs."""
    
    # 1. Loop principal sobre todas as URLs
    for url in urls:
        # Não precisamos mais carregar o controle aqui, ele é carregado dentro de process_page
        process_page(url, base_download_dir)

    print("\n--------------------------------------------------------")
    print("RODADA DE DOWNLOADS CONCLUÍDA.")
    print("--------------------------------------------------------")


# --- Configuração da Linha de Comando e Inicialização ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Programa para baixar arquivos de dados abertos do STJ, criando subpastas e controle de duplicidade por pasta.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        '-d', '--dir', 
        type=str, 
        default="downloads_stj",
        help="Diretório BASE onde as subpastas de download serão criadas.\n(Padrão: 'downloads_stj' na pasta atual)"
    )
    
    args = parser.parse_args()

    base_download_dir = args.dir

    # Cria o diretório BASE se não existir (se não existir, as subpastas também não existirão)
    if not os.path.exists(base_download_dir):
        os.makedirs(base_download_dir)
        print(f"Diretório BASE '{base_download_dir}' criado.")
        
    run_all_scrapers(TARGET_URLS, base_download_dir)

    