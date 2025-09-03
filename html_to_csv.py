# html_to_csv.py — Convierte HTMLs guardados de FBref en CSVs dentro de data/raw

import os
import argparse
import pandas as pd
from io import StringIO

def html_to_csv(html_path: str, out_root="data/raw", comp="champions", season="2022-2023", slug="passing"):
    # Leer archivo HTML
    with open(html_path, "r", encoding="utf-8") as f:
        html_text = f.read()

    # Parsear tablas desde el HTML
    tables = pd.read_html(StringIO(html_text))
    if not tables:
        raise ValueError(f"No se encontró ninguna tabla en {html_path}")
    df = tables[0]

    # Aplanar MultiIndex si es necesario
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(c) for c in tup if c and c != 'nan']).strip() for tup in df.columns]

    # Crear carpeta destino
    outdir = os.path.join(out_root, comp, season)
    os.makedirs(outdir, exist_ok=True)

    # Guardar CSV
    outpath = os.path.join(outdir, f"{slug}.csv")
    df.to_csv(outpath, index=False, encoding="utf-8")
    print(f"[✓] Guardado {outpath} ({len(df)} filas, {len(df.columns)} columnas)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Convierte HTML de FBref a CSV")
    ap.add_argument("--html", required=True, help="Ruta al archivo HTML")
    ap.add_argument("--comp", default="champions", help="Competencia (ej. champions, europa, conference)")
    ap.add_argument("--season", default="2022-2023", help="Temporada (ej. 2022-2023)")
    ap.add_argument("--slug", default="passing", help="Tabla (ej. passing, gca, shooting, defense, keepers, standard)")
    args = ap.parse_args()

    html_to_csv(args.html, comp=args.comp, season=args.season, slug=args.slug)
