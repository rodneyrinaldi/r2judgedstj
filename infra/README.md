 docker-compose down 
 
 docker-compose up -d

python db_init.py

python db_test.py

python db_trunc.py

D:/Repositorios/pocs/py-stj-judged/venv/Scripts/Activate.ps1

docker exec -it judged_db psql -U admin -d legal

docker exec -it postgres_pgvector psql -U admin -d legal


docker exec -it ollama ollama pull nomic-embed-text
docker exec -it ollama ollama pull llama3


uvicorn agent_app:app --reload

select * from judged limited 1;

select count(*) from judged;
