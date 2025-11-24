# 🛒 Ecommerce Analytics Pipeline — Snowflake + dbt + Airflow + Streamlit

Este projeto implementa um pipeline completo de dados para um ambiente de e‑commerce usando **Snowflake**, **dbt**, **Airflow**, **Python** e **Streamlit**. É um projeto moderno, modular, escalável e com arquitetura inspirada na Modern Data Stack.

---

<p align="center">
  <a href="https://youtu.be/gRw-gSxdzoI">
    <img src="https://img.youtube.com/vi/gRw-gSxdzoI/hqdefault.jpg" 
         alt="Pipeline vídeo" width="600">
  </a>
</p>


## 🚀 Visão Geral do Pipeline

O pipeline foi construído para processar dados de eventos de navegação, sessões, produtos, campanhas e comportamento do usuário, desde a ingestão até a disponibilização para dashboards e análises.

### 🔹 Stack utilizada

* **n8n** → Gera e orquestra os eventos de e-commerce (workflow de simulação de tráfego)
* **AWS S3** → Armazena os arquivos de eventos brutos gerados pelo n8n
* **Snowflake (External Stage + Warehouse)** → Ingestão a partir do S3, armazenamento e processamento
* **dbt Core** → Transformações SQL (camadas Bronze → Silver → Gold)
* **Airflow** → Orquestra o dbt dentro do ambiente de dados
* **Docker** → Sobe toda a stack de forma isolada e reproduzível
* **Streamlit** → Dashboard analítico interativo consumindo a camada Gold

---


## 📦 Estrutura de Pastas

```
📂 ecommerce_dbt/                     # Raiz do projeto (este repositório)
│
├── 📁 airflow/                        # Orquestração (Docker + Airflow)
│   └── dags/
│       └── dbt_ecommerce_pipeline.py
│
├── 📁 ecommerce_dbt/                  # Projeto dbt
│   ├── analyses/
│   ├── ecommerce_streamlit/           # App Streamlit
│   │   ├── analytics.py
│   │   ├── app.py
│   │   └── config.py
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── bronze/
│   │   │   └── schema.yml
│   │   ├── silver/
│   │   │   ├── dim_campaigns.sql
│   │   │   ├── dim_products.sql
│   │   │   ├── dim_sessions.sql
│   │   │   ├── dim_users.sql
│   │   │   ├── fct_event_items.sql
│   │   │   └── fct_events.sql
│   │   ├── gold/
│   │   │   ├── campaign_performance.sql
│   │   │   ├── funnel_daily.sql
│   │   │   ├── product_performance.sql
│   │   │   └── revenue_daily.sql
│   │   └── schema.yml
│   ├── seeds/
│   ├── snapshots/
│   └── tests/
│
├── 📁 logs/
│
├── docker-compose.yml
└── sources.yml
```

## 🗄️ Arquitetura (Bronze → Silver → Gold) (Bronze → Silver → Gold)

### **BRONZE — Dados brutos**

Recebe os arquivos diretamente do Stage S3:

* Estrutura original dos eventos
* Sem limpeza
* Sem normalização

### **SILVER — Dados tratados**

Camada com:

* Normalização de colunas
* Padronização de datas
* Junções entre dimensões
* Modelagem incremental

### **GOLD — Métricas finais para BI**

Tabelas prontas para uso:

* `revenue_daily`
* `product_performance`
* `campaign_performance`
* `funnel_daily`

Prontas para consumo via dashboards, Streamlit e análises.

---

## 🧪 Testes dbt

Testes incluídos:

* Uniqueness
* Not null
* Relationship integrity

---

## 🛠️ Tecnologias

* Python 3.11
* dbt 1.7
* Airflow 2.8
* Docker Compose
* Streamlit 1.31
* Snowflake

---

## 📬 Contato

**Lucas Negrini**
📧 Email: Luccanegrini@outlook.com.br
🔗 GitHub: [https://github.com/luccanegrini](https://github.com/luccanegrini)
🔗 Site: Luccanegrini.com.br
---
