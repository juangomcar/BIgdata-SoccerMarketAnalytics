# merge_transfermarkt.py — Une FBref (rendimiento) con Transfermarkt (valores de mercado)

import pandas as pd
import os

# Paths
FBREF_PATH = os.path.join("data", "processed", "master.csv")
VALUATIONS_PATH = os.path.join("data", "transfermark", "player_valuations_clean.csv")
PLAYERS_PATH = os.path.join("data", "transfermark", "players.csv")
CLUBS_PATH = os.path.join("data", "transfermark", "clubs.csv")
COMPS_PATH = os.path.join("data", "transfermark", "competitions.csv")

OUT_PATH = os.path.join("data", "processed", "master_with_values.csv")

# 1. Cargar datasets
fbref = pd.read_csv(FBREF_PATH)
vals = pd.read_csv(VALUATIONS_PATH)
players = pd.read_csv(PLAYERS_PATH)
clubs = pd.read_csv(CLUBS_PATH)
comps = pd.read_csv(COMPS_PATH)

# 2. Enlazar valuations con metadata
vals = vals.merge(players[["player_id", "name"]], on="player_id", how="left")
vals = vals.merge(clubs[["club_id", "name"]], 
                  left_on="current_club_id", right_on="club_id", 
                  how="left", suffixes=("", "_club"))

# Si existe la columna de competiciones, hacemos el merge
if "player_club_domestic_comp_id" in vals.columns:
    vals = vals.merge(comps[["competition_id", "name"]], 
                      left_on="player_club_domestic_comp_id", right_on="competition_id", 
                      how="left", suffixes=("", "_comp"))


# 3. Normalizar nombres para join
fbref["Player_clean"] = fbref["Player"].str.lower().str.strip()
vals["name_clean"] = vals["name"].str.lower().str.strip()

# 4. Crear columna de temporada a partir de la fecha en formato FBref ("2023-2024")
vals["year"] = pd.to_datetime(vals["date"]).dt.year
vals["season"] = vals["year"].astype(str) + "-" + (vals["year"]+1).astype(str)

# 5. Filtrar seasons que te interesan
vals = vals[vals["year"].between(2022, 2025)]

# 6. Merge por jugador + temporada
merged = pd.merge(
    fbref,
    vals,
    left_on=["Player_clean", "Season"],
    right_on=["name_clean", "season"],
    how="left"
)

# 7. Guardar dataset final
merged.to_csv(OUT_PATH, index=False, encoding="utf-8")
print(f"[✓] Guardado dataset final en {OUT_PATH} con {len(merged)} filas y {len(merged.columns)} columnas")
