import os
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

# --- Correções e Importações LangChain ---
# Importações necessárias para construir a nova cadeia RAG com LCEL
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_community.llms import Ollama
from langchain_community.vectorstores.pgvector import PGVector
from langchain_community.embeddings import OllamaEmbeddings


# --- Configuração: Mapeando Variáveis do Docker-Compose ---

# 1. Parâmetros do Agente (usando defaults baseados no docker-compose)
# Se a aplicação for executada DENTRO do Docker, ela usará as variáveis de ambiente.
# Se for executada LOCALMENTE, ela usará os DEFAULTS (localhost).

# URL do Ollama. No docker é 'http://ollama:11434'. Localmente é 'http://localhost:11434'.
LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:11434")

# URL do PGVector (judged_llm_db). No docker é 'judged_llm_db:5432'.
# Localmente, a porta exposta é 5433.
DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost" # 'judged_llm_db' no docker
DB_PORT = "5433"      # '5432' no docker
DB_NAME = "vector_storage"

# Constrói o URL de conexão para o PGVector local:
DEFAULT_DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

# Modelos
OLLAMA_MODEL = "llama3" # Modelo LLM principal
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text") 
COLLECTION_NAME = "judged_documents" # Nome da coleção PGVector


# --- Configuração FastAPI ---

app = FastAPI(
    title="Agente Especialista RAG (Português)", 
    description="Agente que consulta o LLM e o banco vetorizado legal. Respostas em Português do Brasil."
)


# --- Template do Prompt em Português ---
# Este template é crucial para instruir o LLM a usar os documentos E responder em Português.
SYSTEM_PROMPT = """Você é um assistente RAG especializado em direito legal. 
Sua tarefa é responder a perguntas estritamente com base no contexto fornecido. 
Se a pergunta não puder ser respondida usando o contexto, diga que você não tem informações suficientes, 
mas mantenha a resposta no idioma Português do Brasil.
Contexto: {context}"""

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", SYSTEM_PROMPT),
        ("human", "{question}"),
    ]
)


# --- Inicialização dos Componentes RAG ---

def setup_rag_components():
    """Inicializa o LLM e o VectorStore/Retriever usando as configurações definidas e constrói a cadeia LCEL."""
    print(f"⏳ Conectando ao LLM em: {LLM_API_URL}")
    print(f"⏳ Conectando ao DB em: {DATABASE_URL}")
    
    # 1. LLM (Llama 3 via Ollama)
    llm = Ollama(
        model=OLLAMA_MODEL, 
        base_url=LLM_API_URL, 
        temperature=0
    )
    
    # 2. Embedding
    embeddings = OllamaEmbeddings(
        model=EMBEDDING_MODEL,
        base_url=LLM_API_URL
    )
    
    # 3. VectorStore (Conexão ao pgvector)
    vector_store = PGVector(
        connection_string=DATABASE_URL,
        embedding_function=embeddings,
        collection_name=COLLECTION_NAME, 
        pre_delete_collection=False 
    )
    
    # 4. Retrieval Chain (Combina LLM + Retriever + Prompt em PT-BR)
    retriever = vector_store.as_retriever(search_kwargs={"k": 3})
    
    # Constrói a cadeia RAG usando LCEL:
    # 1. Recebe a pergunta e passa para as chaves 'context' e 'question'.
    # 2. O 'context' é preenchido pelo retriever, o 'question' é a pergunta original.
    # 3. Passa para o prompt formatado.
    # 4. Envia para o LLM.
    # 5. Analisa a saída como string.
    
    rag_chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    
    return rag_chain

# Inicializa o agente na inicialização do FastAPI
# A tipagem é Any aqui porque a cadeia LCEL não é estritamente RetrievalQA.
rag_chain: Optional[any] = None 
try:
    rag_chain = setup_rag_components()
    print("✅ Agente RAG inicializado com sucesso.")
except Exception as e:
    print(f"❌ Erro ao inicializar o Agente RAG. Detalhe: {e}")
    # Define como None, para o endpoint retornar erro em caso de falha na inicialização
    rag_chain = None
    
# --- Endpoint da API ---

class QueryInput(BaseModel):
    query: str

@app.post("/ask/")
async def ask_agent(input: QueryInput):
    """Endpoint para enviar uma consulta ao Agente RAG. A resposta será em Português do Brasil."""
    if rag_chain is None:
        return {"error": "O Agente RAG não foi inicializado corretamente. Verifique se o Ollama e o PostgreSQL estão rodando."}
        
    try:
        # Invoca a cadeia LCEL que retorna diretamente a resposta em string (já formatada pelo prompt)
        answer = rag_chain.invoke(input.query)
        
        return {
            "query": input.query,
            "answer": answer 
        }
    except Exception as e:
        return {"error": f"Erro durante a execução da chain RAG: {e}"}