# 📓 Notebooks Databricks

Esta pasta contém dois notebooks adaptados para diferentes ambientes:

## ✅ `info_transportes_CE.ipynb`

- Executado no **Databricks Community Edition**
- Permite upload de CSV via DBFS
- Salva arquivos Parquet no `/dbfs/tmp/...`
- Estrutura em camadas (bronze/silver/gold)

## 🏢 `info_transportes_HK.ipynb`

- Usado em **ambiente corporativo com restrições**
- Sem permissão para criar schemas, salvar Delta ou Parquet
- Toda lógica é feita apenas com DataFrames

> Ambos os notebooks usam o mesmo dataset `info_transportes.csv` e realizam tratamento, padronização de datas e agregações.

