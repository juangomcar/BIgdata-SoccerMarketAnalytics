# clean_transfermark.py — Prepara valuations de Transfermark (2022+)

import pandas as pd
import os

# Paths
INPUT = os.path.join("data", "transfermark", "player_valuations.csv")
OUTPUT = os.path.join("data", "transfermark", "player_valuations_clean.csv")

def main():
    df = pd.read_csv(INPUT)
    print(f"[i] Dataset original: {df.shape[0]} filas, {df.shape[1]} columnas")

    # 1. Convertir fechas
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # 2. Filtrar rango 2022–2025
    df = df[df["date"].between("2022-01-01", "2025-08-30")]

    # 3. Crear temporada estilo FBref
    df["year"] = df["date"].dt.year
    df["season"] = df["year"].astype(str) + "-" + (df["year"] + 1).astype(str)

    # 4. Eliminar duplicados (jugador + temporada, nos quedamos con el último valor)
    df = df.sort_values("date").drop_duplicates(
        subset=["player_id", "season"], keep="last"
    )

    # 5. Guardar limpio
    df.to_csv(OUTPUT, index=False, encoding="utf-8")
    print(f"[✓] Guardado en {OUTPUT} con {df.shape[0]} filas, {df.shape[1]} columnas")

if __name__ == "__main__":
    main()
