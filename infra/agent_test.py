from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_community.llms import Ollama

prompt = PromptTemplate.from_template(
    "Use o contexto abaixo para responder:\n\n{context}\n\nPergunta: {question}\nResposta:"
)

llm = Ollama(model="llama2")

chain = (
    {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)

print(chain.invoke({"context": "Paris é a capital da França.", "question": "Qual é a capital da França?"}))
