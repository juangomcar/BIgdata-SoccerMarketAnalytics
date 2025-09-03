# prepare_dataset.py — Limpieza de master_with_values.csv para entrenamiento

import pandas as pd
import os

INPUT = os.path.join("data", "processed", "master_with_values.csv")
OUTPUT = os.path.join("data", "processed", "dataset_ready.csv")

def main():
    # 1. Leer dataset
    df = pd.read_csv(INPUT)
    print(f"[i] Dataset original: {df.shape[0]} filas, {df.shape[1]} columnas")

    # 2. Eliminar columnas de IDs y duplicadas (_id, _x, _y)
    drop_cols = [c for c in df.columns if c.endswith("_id") or c.endswith("_x") or c.endswith("_y")]
    df = df.drop(columns=drop_cols, errors="ignore")

    # 3. Eliminar columnas irrelevantes de texto
    text_cols = ["Player_clean", "name", "name_club", "name_comp"]
    df = df.drop(columns=[c for c in text_cols if c in df.columns], errors="ignore")

    # 4. Llenar valores nulos en numéricas con 0
    num_cols = df.select_dtypes(include=["float64", "int64"]).columns
    df[num_cols] = df[num_cols].fillna(0)

    # 5. Quitar duplicados (jugador + temporada)
    if "Player" in df.columns and "Season" in df.columns:
        df = df.drop_duplicates(subset=["Player", "Season"], keep="last")

    # 6. Guardar dataset limpio
    df.to_csv(OUTPUT, index=False, encoding="utf-8")
    print(f"[✓] Dataset limpio guardado en {OUTPUT}")
    print(f"[✓] Filas: {df.shape[0]}, Columnas: {df.shape[1]}")

if __name__ == "__main__":
    main()
