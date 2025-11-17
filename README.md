```
### `README.md`

py --list
py -3.12 -m venv venv
.\venv\Scripts\activate
python.exe -m pip install --upgrade pip
pip freeze > requirements.txt
pip install -r requirements.txt

docker-compose up judged_llm_db ollama -d
uvicorn agent_app:app --reload

http://127.0.0.1:8000/docs
{
  "query": "Quando um juiz pode usar a analogia na decisão?"
}


```markdown
# Processador de Acórdãos com PostgreSQL e Vetores

Este projeto processa arquivos JSON contendo acórdãos, grava os dados relacionais e vetoriais em um banco de dados PostgreSQL com suporte ao plugin **pgvector**. Ele permite realizar análises detalhadas de atributos processuais, como estado de origem, resultado da decisão, aplicação de súmulas, gratuidade, entre outros, além de possibilitar buscas semânticas para sistemas de **RAG (Retrieval-Augmented Generation)**.

---

## 🛠️ Requisitos

Antes de começar, certifique-se de ter os seguintes softwares instalados:

- **Docker**: Versão 20.10 ou superior
- **Docker Compose**: Versão 1.29 ou superior
- **Python**: Versão 3.8 ou superior
- **pip**: Gerenciador de pacotes do Python

---

## 📂 Estrutura do Projeto

```plaintext
.
├── process_stj/                # Pasta de entrada com os arquivos JSON a serem processados
│   ├── subpasta1/
│   │   ├── arquivo1.json
│   │   ├── arquivo2.json
│   ├── subpasta2/
│       ├── arquivo3.json
├── postgres_data/              # Volume externo para persistência dos dados do PostgreSQL
├── processed_files.txt         # Arquivo de controle para registrar os arquivos já processados
├── docker-compose.yml          # Configuração do Docker para o PostgreSQL com pgvector
├── main.py                     # Código principal do programa
├── requirements.txt            # Dependências do Python
└── README.md                   # Este arquivo
```

---

## 🚀 Configuração e Execução

### 1️⃣ Configurar o Banco de Dados PostgreSQL com Docker

O projeto utiliza um contêiner Docker para executar o PostgreSQL com suporte ao plugin **pgvector**.

1. **Crie o contêiner PostgreSQL**:

   Execute o comando abaixo para iniciar o banco de dados:

   ```bash
   docker-compose up -d
   ```
2. **Verifique se o contêiner está rodando**:

   Use o comando:

   ```bash
   docker ps
   ```

   Você deve ver o contêiner `postgres_pgvector` em execução.
3. **Conecte-se ao banco de dados**:

   Para acessar o banco de dados, execute:

   ```bash
   docker exec -it postgres_pgvector psql -U admin -d jurisprudencia
   docker exec -it judged_db psql -U admin -d legal 
   ```
4. **Crie as tabelas necessárias**:

   No terminal do PostgreSQL, execute os seguintes comandos SQL para criar as tabelas:

   ```sql
   -- Extensão para suporte a vetores
   CREATE EXTENSION IF NOT EXISTS vector;

   -- Tabela para dados relacionais
   CREATE TABLE decisoes (
       id SERIAL PRIMARY KEY,
       conteudo TEXT NOT NULL,
       estado_origem VARCHAR(2),       -- Estado de origem do processo (ex.: SP, RJ)
       resultado VARCHAR(20),          -- Resultado da decisão (ex.: deferido, indeferido)
       aplicacao_sumula BOOLEAN,       -- Se houve aplicação de súmula (true/false)
       idoso BOOLEAN,                  -- Se o processo envolve idoso (true/false)
       mulher BOOLEAN,                 -- Se o processo envolve mulher (true/false)
       preliminares BOOLEAN,           -- Se há preliminares no processo (true/false)
       gratuidade BOOLEAN              -- Se o processo foi deferido com gratuidade (true/false)
   );

   -- Tabela para dados vetoriais
   CREATE TABLE decisoes_vetoriais (
       id SERIAL PRIMARY KEY,
       conteudo TEXT NOT NULL,
       teses TEXT[],                   -- Lista de teses extraídas
       embedding vector(768)           -- Vetor de dimensão 768 (ajuste conforme necessário)
   );
   ```

---

### 2️⃣ Configurar o Ambiente Python

1. **Crie um ambiente virtual (opcional)**:

   É recomendado criar um ambiente virtual para gerenciar as dependências do projeto:

   ```bash
   python -m venv venv
   source venv/bin/activate  # No Windows: venv\Scripts\activate
   ```
2. **Instale as dependências**:

   Use o comando abaixo para instalar as bibliotecas necessárias:

   ```bash
   pip install -r requirements.txt
   ```

---

### 3️⃣ Estrutura dos Arquivos JSON

Os arquivos JSON devem estar na pasta `process_stj` e podem ser organizados em subpastas. Cada arquivo JSON pode conter:

- Um único objeto JSON (dicionário).
- Uma lista de objetos JSON.

Exemplo de arquivo JSON:

```json
[
    {
        "id": "123",
        "numeroProcesso": "0001234-56.2023.1.00.0000",
        "estadoOrigem": "SP",
        "resultado": "deferido",
        "aplicacaoSumula": true,
        "idoso": false,
        "mulher": true,
        "preliminares": true,
        "gratuidade": true,
        "ementa": "Exemplo de ementa",
        "teseJuridica": "Tese 1. Tese 2. Tese 3."
    },
    {
        "id": "124",
        "numeroProcesso": "0005678-90.2023.1.00.0000",
        "estadoOrigem": "RJ",
        "resultado": "indeferido",
        "aplicacaoSumula": false,
        "idoso": true,
        "mulher": false,
        "preliminares": false,
        "gratuidade": false,
        "ementa": "Outro exemplo de ementa",
        "teseJuridica": null
    }
]
```

---

### 4️⃣ Executar o Programa

Para processar os arquivos JSON e gravar os dados no banco de dados, execute o seguinte comando:

```bash
python main.py
```

---

## 🗃️ Estrutura do Banco de Dados

### Tabela `decisoes`

Armazena os dados relacionais dos acórdãos.

| Coluna               | Tipo    | Descrição                                            |
| -------------------- | ------- | ------------------------------------------------------ |
| `id`               | SERIAL  | Identificador único                                   |
| `conteudo`         | TEXT    | Conteúdo completo do acórdão                        |
| `estado_origem`    | VARCHAR | Estado de origem do processo (ex.: SP, RJ)             |
| `resultado`        | VARCHAR | Resultado da decisão (ex.: deferido, indeferido)      |
| `aplicacao_sumula` | BOOLEAN | Se houve aplicação de súmula (true/false)           |
| `idoso`            | BOOLEAN | Se o processo envolve idoso (true/false)               |
| `mulher`           | BOOLEAN | Se o processo envolve mulher (true/false)              |
| `preliminares`     | BOOLEAN | Se há preliminares no processo (true/false)           |
| `gratuidade`       | BOOLEAN | Se o processo foi deferido com gratuidade (true/false) |

### Tabela `decisoes_vetoriais`

Armazena os dados vetoriais dos acórdãos.

| Coluna        | Tipo   | Descrição                                                             |
| ------------- | ------ | ----------------------------------------------------------------------- |
| `id`        | SERIAL | Identificador único                                                    |
| `conteudo`  | TEXT   | Conteúdo completo do acórdão                                         |
| `teses`     | TEXT[] | Lista de teses extraídas                                               |
| `embedding` | VECTOR | Representação vetorial do conteúdo (dimensão configurada, ex.: 768) |

---

## 🔍 Consultas Úteis

### 1️⃣ Processos por Estado de Origem

```sql
SELECT estado_origem, COUNT(*)
FROM decisoes
GROUP BY estado_origem
ORDER BY COUNT(*) DESC;
```

### 2️⃣ Processos Deferidos ou Indeferidos

```sql
SELECT resultado, COUNT(*)
FROM decisoes
GROUP BY resultado;
```

### 3️⃣ Processos com Aplicação de Súmulas

```sql
SELECT COUNT(*)
FROM decisoes
WHERE aplicacao_sumula = TRUE;
```

### 4️⃣ Processos com Gratuidade

```sql
SELECT COUNT(*)
FROM decisoes
WHERE gratuidade = TRUE;
```

### 5️⃣ Busca Semântica com Vetores

```sql
SELECT id, conteudo, embedding <=> '[0.1, 0.2, 0.3, ..., 0.768]'::vector AS distancia
FROM decisoes_vetoriais
ORDER BY distancia
LIMIT 5;
```

---

## 🧪 Testes

1. **Verificar os Dados no Banco**:

   Após executar o programa, conecte-se ao banco de dados e verifique os dados inseridos:

   ```sql
   SELECT * FROM decisoes;
   SELECT * FROM decisoes_vetoriais;
   ```
2. **Testar o Plugin Vetorial**:

   Para testar consultas vetoriais, use o seguinte exemplo:

   ```sql
   SELECT id, conteudo, embedding <=> '[0.1, 0.2, 0.3, ..., 0.768]'::vector AS distancia
   FROM decisoes_vetoriais
   ORDER BY distancia
   LIMIT 5;
   ```

---

## 📚 Referências

- [Docker](https://www.docker.com/)
- [PostgreSQL](https://www.postgresql.org/)
- [pgvector](https://github.com/pgvector/pgvector)
- [Psycopg2](https://www.psycopg.org/)

---

## 📝 Licença

Este projeto é distribuído sob a licença MIT. Consulte o arquivo `LICENSE` para mais informações.

```

---

### O que foi atualizado no `README.md`?

1. **Configuração do Banco de Dados**:
   - Adicionadas instruções para criar colunas adicionais na tabela `decisoes` para armazenar atributos processuais (estado, resultado, súmulas, etc.).

2. **Estrutura dos Dados**:
   - Explicação detalhada das tabelas `decisoes` e `decisoes_vetoriais`.

3. **Consultas Úteis**:
   - Exemplos de consultas para analisar os dados processuais e realizar buscas semânticas.

4. **Execução do Programa**:
   - Passo a passo atualizado para processar os arquivos JSON e gravar os dados no banco.

Se precisar de mais ajustes ou explicações, é só avisar! 😊
```
