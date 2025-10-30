#  Analytics de Compras Públicas de Medicamentos (2020-2025)

##  Visão Geral do Projeto

Este projeto implementa um **Pipeline de Engenharia de Dados (ETL)** robusto para transformar dados brutos e inconsistentes de Compras Públicas de Medicamentos (OpenDataSUS - BPS, 2020-2025) em um **Data Mart Analítico (Star Schema)**.

O objetivo estratégico é fornecer à gestão pública indicadores de **Risco, Economicidade e Demanda**, essenciais para auditoria de preços e planejamento de suprimentos.

---

## A Solução: Data Engineering para Gestão de Risco

O foco técnico do projeto está na **qualidade dos dados** e na **geração de indicadores avançados** de gestão, aplicando o ciclo **CRISP-DM** completo.

| Fase | Objetivo Central | Módulos Envolvidos |
| :--- | :--- | :--- |
| **ETL (Limpeza)** | Consolidar dados de múltiplos anos (2020-2025), corrigindo *schemas* instáveis, *encodings* variáveis e erros de formatação regional (R$). | `etl_compras_antigos.py`, `etl_compras.py` |
| **Modelagem (Feature Engineering)**| Gerar indicadores estatísticos de risco de preço e intermitência de demanda. | `modelagem_dim.py` |
| **Deployment (Orquestração)** | Entregar o produto final (Star Schema) pronto para consumo em BI (Power BI/Tableau). | `main.py`, `dimensoes.py` |



### **Indicadores de Gestão (Key Features):**

* **Z-Score de Risco:** Identifica transações com preços estatisticamente desviantes em relação ao *benchmark* do mercado.
* **Economia Potencial:** Quantifica o valor em R$ da oportunidade de economia em cada linha de compra.
* **Risco de Intermitência:** Mede a estabilidade da demanda por produto para gestão de estoque.
* **Concentração de Fornecedor:** Alerta sobre a dependência de fornecedores únicos (Risco de Suprimento).

---

## Stack Tecnológico e Reprodutibilidade

| Categoria | Tecnologia | Uso no Projeto |
| :--- | :--- | :--- |
| **Linguagem Principal** | Python 3.10+ | Lógica de ETL e Modelagem. |
| **Processamento de Dados** | Pandas, NumPy | Limpeza de dados, manipulação de DataFrames e cálculos estatísticos (Z-Score, PMP Mediano). |
| **Orquestração** | `main.py` + `argparse` | Ponto de entrada único para execução do *pipeline* completo e flexibilidade de comandos (`--apenas-analises`). |
| **Saída Analítica** | CSV (UTF-8-SIG e separador `;`) | Formato otimizado para importação direta e performática no Power BI, garantindo compatibilidade com caracteres especiais. |

### Estrutura do Repositório

. 
├── data/ 
│ ├── raw/ # Arquivos CSV brutos (input) 
│ ├── processed/ # Arquivos CSV intermediários (consolidados) 
│ └── outputs/ # Star Schema Final (output para BI) 
├── src/ # Módulos Python com a lógica (ETL, Modelagem, Dimensões) 
│ ├── etl_compras.py 
│ ├── modelagem_dim.py 
│ └── dimensoes.py 
├── Notebooks/ # Documentação completa e validação (CRISP-DM) 
├── main.py # Orquestrador Central do Pipeline 
└── README.md

---

##  Como Executar o Projeto

Para replicar e executar o pipeline completo, siga os passos abaixo.

### Pré-requisitos

1.  Python 3.10+ instalado.
2.  Gerenciador de pacotes `pip`.
3.  Dados brutos (`.csv`s do OpenDataSUS) salvos na pasta `data/raw/`.

### 1. Instalação das Dependências

Crie um ambiente virtual (recomendado) e instale as bibliotecas:

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# ou
.\venv\Scripts\activate   # Windows

pip install pandas numpy
```

### 2. Execução do Pipeline Completo

O main.py executa o ETL (Limpeza, Consolidação), o Feature Engineering e a Carga Final (Deployment) em uma única linha:

```Bash

python main.py
```

### 3. Execução Acelerada (Apenas Análises)

Se os dados já foram limpos e o arquivo consolidado existe, use a flag de análise:

````Bash

python main.py 

````

## Próximos Passos (Roadmap)

Este projeto está pronto para a fase de Análise de Negócios (Power BI). As seguintes melhorias são planejadas para a evolução do sistema:

 - Integração com APIs: Automação da coleta de dados de referência (ex: CMED) via API para reduzir dependência de CSVs estáticos.

 - Módulo de IA: Inclusão de recursos de Inteligência Artificial Generativa para auxiliar o usuário na interpretação de outliers e legislação.

 - Monitoramento: Criação de uma página de auditoria e controle para monitorar a performance do pipeline e dos indicadores de gestão


