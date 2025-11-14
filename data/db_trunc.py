import psycopg2
from psycopg2 import sql

# Configurações do banco de dados
DB_CONFIG = {
    "host": "localhost",  # Altere para o host do container Docker, se necessário
    "port": 5432,         # Porta padrão do PostgreSQL
    "dbname": "legal",    # Nome do banco de dados
    "user": "admin",   # Usuário do banco de dados
    "password": "admin"  # Senha do banco de dados
}

# Nome da tabela a ser deletada
TABLE_NAME = "judged"

# Função para deletar a tabela
def deletar_tabela():
    try:
        # Conecta ao banco de dados
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Monta o comando SQL para deletar a tabela
        delete_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(
            sql.Identifier(TABLE_NAME)
        )

        # Executa o comando de exclusão
        cursor.execute(delete_table_query)
        conn.commit()

        print(f"Tabela '{TABLE_NAME}' deletada com sucesso.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao deletar a tabela '{TABLE_NAME}': {e}")

# Programa principal
if __name__ == "__main__":
    print(f"Deletando a tabela '{TABLE_NAME}'...")
    deletar_tabela()