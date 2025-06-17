# 🚚 Transportes Pipeline

Pipeline de dados para processamento, transformação e agregação de informações de transportes utilizando **PySpark**, com ambiente totalmente orquestrado via **Docker Compose** e cobertura de testes automatizada.

---

## 📦 Estrutura do Projeto

```
transportes-pipeline/
│
├── docker_version/
│   ├── data/                # Dados de entrada (CSV)
│   ├── src/                 # Código-fonte do pipeline
│   │   ├── main.py
│   │   ├── ingest.py
│   │   ├── date_transform.py
│   │   ├── create_ref.py
│   │   ├── aggregate.py
│   │   └── transform_facada.py
│   ├── tests/               # Testes unitários (pytest)
│   ├── coverage_html/       # Relatório de cobertura de testes (gerado automaticamente)
│   ├── Dockerfile           # Dockerfile principal (Spark + Python)
│   ├── docker-compose.yml   # Orquestração dos serviços
│   └── requirements.txt     # Dependências Python
│
├── .env                     # Variáveis de ambiente do pipeline
└── .gitignore
```

---

## 🐳 Serviços Docker

- **spark**: Executa o pipeline principal com Spark.
- **tests**: Roda os testes unitários com cobertura.
- **coverage**: Servidor HTTP para visualizar o relatório de cobertura em [http://localhost:8081](http://localhost:8081).

---

## ⚙️ Como rodar o projeto

### 1. **Preparação**

- Certifique-se de ter [Docker](https://www.docker.com/) e [Docker Compose](https://docs.docker.com/compose/) instalados.
- Coloque seus arquivos de dados (ex: `info_transportes.csv`) na pasta data.

### 2. **Configuração**

Edite o arquivo `.env` conforme necessário, por exemplo:

```
EXPECTED_COLUMNS=DATA_INICIO,DATA_FIM,CATEGORIA,DISTANCIA,PROPOSITO
EXPECTED_DATA_FORMAT=MM-dd-yyyy HH:mm
EXPECTED_DEFAULT_DATA_REF=yyyy-MM-dd
DATE_COLUMNS=DATA_INICIO,DATA_FIM
AGG_COLUMN=DT_REF
```

### 3. **Executando os testes e cobertura**

```sh
docker-compose run --rm tests
docker-compose up -d coverage
```

Acesse o relatório de cobertura em: [http://localhost:8081](http://localhost:8081)

### 4. **Executando o pipeline Spark**

```sh
docker-compose up spark
```

---

## 🧪 Testes

- Os testes estão localizados em tests.
- São executados automaticamente pelo serviço `tests` e geram um relatório HTML em coverage_html.

### Exemplo de execução manual dos testes:

```sh
docker-compose run --rm tests
```

---

## 📝 Principais Componentes

- **main.py**: Ponto de entrada do pipeline.
- **ingest.py**: Leitura dos dados CSV.
- **date_transform.py**: Transformação e padronização de colunas de data.
- **create_ref.py**: Criação de coluna de referência para agregação.
- **aggregate.py**: Agregação dos dados para geração do nível gold.
- **transform_facada.py**: Orquestra as transformações do pipeline.

---

## 🗂️ Volumes e Relatórios

- Os dados de entrada e saída são persistidos via volumes Docker.
- O relatório de cobertura de testes é gerado em coverage_html e exposto via HTTP na porta 8081.

---

## 🛠️ Dicas Úteis

- Para limpar containers, volumes e imagens:
  ```sh
  docker-compose down -v --rmi all --remove-orphans
  ```
- Para reconstruir tudo do zero:
  ```sh
  docker-compose build --no-cache
  ```

---

## 👩‍💻 Desenvolvimento

- Adicione novos testes em tests.
- Adicione novas dependências Python em `requirements.txt`.
- O pipeline pode ser facilmente adaptado para outros formatos de dados ou regras de negócio.

---

## 📢 Observações

- Sempre rode os testes antes de executar o pipeline para garantir a integridade dos dados.
- O pipeline foi projetado para ser modular e facilmente extensível.
- O relatório de cobertura é atualizado a cada execução dos testes.

---
