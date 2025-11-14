docker-compose up -d

python db_init.py

python db_test.py

python db_trunc.py

D:/Repositorios/pocs/py-stj-judged/venv/Scripts/Activate.ps1

docker exec -it judged_db psql -U admin -d legal

docker exec -it postgres_pgvector psql -U admin -d legal

select * from judged limited 1;

select count(*) from judged;
