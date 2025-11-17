FROM python:3.11-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Copia e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código principal da aplicação
COPY agent_app.py .

# Comando de execução: Inicia o servidor FastAPI (Uvicorn)
CMD ["uvicorn", "agent_app:app", "--host", "0.0.0.0", "--port", "8000"]