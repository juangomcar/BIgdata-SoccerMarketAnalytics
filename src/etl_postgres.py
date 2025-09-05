import pandas as pd
from sqlalchemy import create_engine

# Cargar dataset limpio (ajusta al tuyo)
df = pd.read_csv("data/final_players.csv")

# Conexión a Postgres (según docker-compose)
engine = create_engine("postgresql://psqluser:psqlpassword@db:5432/football_db")

# Subir tabla
df.to_sql("players_market_values", engine, if_exists="replace", index=False)

print("[i] Datos cargados en PostgreSQL con éxito 🚀")

