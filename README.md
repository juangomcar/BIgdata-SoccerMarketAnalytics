# Big Data – Soccer Market Analytics
Este proyecto corresponde al **Proyecto Final – Herramientas de Big Data (Coterminal, Maestría en Analítica Aplicada, Universidad de La Sabana)**.  
El objetivo es **predecir el valor de mercado de futbolistas profesionales** mediante un pipeline reproducible basado en técnicas de Big Data, Machine Learning y orquestación con Docker + PostgreSQL.

---

## Objetivos del Proyecto
- Construir un **pipeline ETL** (Extract, Transform, Load) para integrar datos de mercado y rendimiento.
- Implementar un entorno reproducible en **Docker** con persistencia en **PostgreSQL**.
- Entrenar y comparar modelos de Machine Learning (**Regresión Lineal, Random Forest y XGBoost**).
- Evaluar diferencias entre valores oficiales de Transfermarkt y valores ajustados por los modelos.
- Realizar un **forecast 2025/2026** para jugadores top del mercado.

---

## Estructura del Repositorio
├── data/ # Datos crudos y procesados
├── notebooks/ # Jupyter Notebooks (EDA, modelado, resultados)
├── src/ # Código fuente ETL y consultas
├── Dockerfile # Imagen base para entorno reproducible
├── docker-compose.yml # Orquestación de contenedores (app + PostgreSQL)
├── reporte_proyecto.tex # Informe final en LaTeX
├── reporte_proyecto.pdf # Informe compilado
└── README.md # Documentación principal

yaml
Copiar código

---

## Instalación y Ejecución
### 1️Clonar el repositorio
```bash
git clone https://github.com/juangomcar/Bigdata-SoccerMarketAnalytics.git
cd Bigdata-SoccerMarketAnalytics
2️Levantar el entorno con Docker
bash
Copiar código
docker-compose up --build
Esto levanta:

JupyterLab en http://localhost:8888/lab

PostgreSQL en localhost:5432

Explorar los notebooks
Abrir desde JupyterLab:

notebooks/eda.ipynb → Exploración de datos.

notebooks/modeling.ipynb → Entrenamiento de modelos.

notebooks/notebook_1_top50_actual.ipynb → Top jugadores actuales.

notebooks/notebook_2_top50_forecast.ipynb → Predicción 2025/2026.

notebooks/03_player_value_query.ipynb → Consultas por jugador.

     Resultados principales
XGBoost fue el modelo con mejor desempeño (MAE más bajo y $R^2$ más alto).

Identificamos jugadores sobrevalorados (ej. Haaland, Mbappé) y oportunidades de inversión (ej. jóvenes talentos).

El forecast 25/26 sugiere consolidación de Bellingham, Yamal y Musiala como estrellas top.

Informe
El informe académico completo se encuentra en:

reporte_proyecto.pdf
Incluye: introducción, metodología, resultados, discusión y conclusiones.

Autores
Juan Gómez – Cód. 286774
Esteban Bernal – Cód. 271930
Juan Montes – Cód. 272113

Profesor: Hugo Franco, Ph.D.
Asignatura: Herramientas de Big Data (Coterminal)   
Universidad de La Sabana – Facultad de Ingeniería
Septiembre 2025
