# agent_app.py
import os
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

# --- Correções e Importações LangChain ---
from langchain.chains import RetrievalQA
from langchain.chains.base import Chain
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_community.llms import Ollama
from langchain_community.vectorstores.pgvector import PGVector
from langchain_community.embeddings import OllamaEmbeddings


# --- Configuração: Mapeando Variáveis do Docker-Compose ---

# ... (Configurações de Conexão Mantidas) ...
LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:11434")

DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost" # 'judged_llm_db' no docker
DB_PORT = "5433"      # '5432' no docker
DB_NAME = "vector_storage"

DEFAULT_DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

OLLAMA_MODEL = "llama3" # Modelo LLM principal
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text") 
COLLECTION_NAME = "judged_documents" # Nome da coleção PGVector

# --- NOVA VARIÁVEL DE CONFIGURAÇÃO ---
DEFAULT_LANGUAGE = "Português do Brasil"


# --- Configuração FastAPI ---

app = FastAPI(
    title="Agente Especialista RAG", 
    description="Agente que consulta o LLM e o banco vetorizado legal."
)


# --- Inicialização dos Componentes RAG ---

def setup_rag_components() -> Chain:
    """Inicializa o LLM, VectorStore/Retriever e a Chain RAG com Prompt em PT-BR."""
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
    
    # 3. VectorStore
    vector_store = PGVector(
        connection_string=DATABASE_URL,
        embedding_function=embeddings,
        collection_name=COLLECTION_NAME, 
        pre_delete_collection=False 
    )
    
    # 4. Prompt Customizado para forçar o idioma
    # O template deve ser em Inglês para garantir que o LLM entenda a instrução,
    # mas a instrução deve ser clara sobre o idioma de saída.
    CUSTOM_PROMPT_TEMPLATE = """
    Você é um agente especialista em direito e deve responder a perguntas com base exclusivamente no contexto fornecido. 
    Sua resposta deve ser sempre concisa, objetiva e, mais importante, **estritamente no idioma: {language}**.
    
    Se o contexto não contiver a resposta, diga "Não tenho informações suficientes no contexto para responder."
    
    Contexto: {context}
    Pergunta: {question}
    Resposta em {language}:
    """.strip()
    
    # Cria a instância do PromptTemplate
    CUSTOM_PROMPT = PromptTemplate(
        template=CUSTOM_PROMPT_TEMPLATE,
        input_variables=["context", "question", "language"]
    )
    
    # 5. Retrieval Chain (Combina LLM + Retriever)
    retriever = vector_store.as_retriever(search_kwargs={"k": 3})
    
    # Passamos o prompt customizado
    qa_chain = RetrievalQA.from_chain_type(
        llm=llm, 
        chain_type="stuff", 
        retriever=retriever,
        # Argumentos adicionados para customizar o prompt
        chain_type_kwargs={
            "prompt": CUSTOM_PROMPT,
            "language": DEFAULT_LANGUAGE # Passa a variável de configuração para o prompt
        }
    )
    
    return qa_chain

# Inicializa o agente na inicialização do FastAPI
rag_chain: Optional[Chain] = None
try:
    rag_chain = setup_rag_components()
    print("✅ Agente RAG inicializado com sucesso.")
except Exception as e:
    print(f"❌ Erro ao inicializar o Agente RAG. Detalhe: {e}")
    rag_chain = None
    
# --- Endpoint da API ---

class QueryInput(BaseModel):
    query: str

@app.post("/ask/")
async def ask_agent(input: QueryInput):
    """Endpoint para enviar uma consulta ao Agente RAG."""
    if rag_chain is None:
        return {"error": "O Agente RAG não foi inicializado corretamente. Verifique se o Ollama e o PostgreSQL estão rodando."}
        
    try:
        # A chain_type_kwargs não é mais necessária aqui, pois foi passada na criação da chain
        result = rag_chain.invoke({"query": input.query, "language": DEFAULT_LANGUAGE})
        
        return {
            "query": input.query,
            "answer": result.get('result', 'Resposta não encontrada no resultado da chain.')
        }
    except Exception as e:
        return {"error": f"Erro durante a execução da chain RAG: {e}"}