# merge_all.py — Une todo data/raw en un dataset maestro sin duplicados ni cartesianazos
import os
import re
import unicodedata
import argparse
from functools import reduce
from typing import Dict, List

import pandas as pd

DEFAULT_RAW_ROOT = os.path.join("data", "raw")
DEFAULT_OUT_DIR  = os.path.join("data", "processed")

# Columnas de contexto que nunca se prefijan
CONTEXT_COLS = {
    "Player","Squad","Nation","Pos","Comp","Age","Born","Season","Competition",
    "player_key","squad_key","season_key","comp_key","Rk"
}

# ---- Utilidades --------------------------------------------------------------

def _norm_text(s: str) -> str:
    """lower + sin acentos + espacios compactados."""
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s

def _simplify(col: str) -> str:
    """Normaliza un nombre de columna para hacer matching flexible."""
    c = unicodedata.normalize("NFKD", str(col).lower())
    c = "".join(ch for ch in c if not unicodedata.combining(ch))
    c = re.sub(r"[^a-z0-9]+", "", c)  # quita espacios, símbolos
    return c

def normalize_core_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas típicas de FBref, que a veces vienen duplicadas (p.ej. 'Player Player').
    Map flexible por 'substring' simplificado.
    """
    df = df.copy()
    mapping = {}
    taken = set()

    # candidatos -> nombre canónico
    targets = [
        (["player"], "Player"),
        (["squad","team"], "Squad"),
        (["nation","country"], "Nation"),
        (["pos","position"], "Pos"),
        (["comp","competition","league"], "Comp"),
        (["age"], "Age"),
        (["born","birthyear","birth"], "Born"),
        (["rk","rank"], "Rk"),
    ]

    simple_cols = {col: _simplify(col) for col in df.columns}

    for col, simple in simple_cols.items():
        for keys, canon in targets:
            if canon in taken:
                continue
            if any(k in simple for k in keys):
                mapping[col] = canon
                taken.add(canon)
                break

    # aplica mapeo
    df = df.rename(columns=mapping)
    return df

def clean_fbref_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Quita encabezados/totales/filas vacías típicas de FBref."""
    df = df.copy()
    df = normalize_core_columns(df)

    if "Rk" in df.columns:
        df = df[df["Rk"].astype(str) != "Rk"]

    if "Player" in df.columns:
        p = df["Player"].astype(str).str.strip().str.lower()
        df = df[~p.isin(["", "nan", "team total", "squad total", "players"])]

    if "Squad" in df.columns:
        df = df[df["Squad"].astype(str).str.strip().str.lower() != ""]

    return df.drop_duplicates()

def add_keys(df: pd.DataFrame, season: str, comp: str) -> pd.DataFrame:
    """Agrega Season/Competition y llaves normalizadas."""
    df = df.copy()
    df = normalize_core_columns(df)

    df["Season"] = season
    df["Competition"] = comp
    for c in ["Player", "Squad"]:
        if c not in df.columns:
            df[c] = ""

    df["player_key"] = df["Player"].map(_norm_text)
    df["squad_key"]  = df["Squad"].map(_norm_text)
    df["season_key"] = _norm_text(season)
    df["comp_key"]   = _norm_text(comp)
    return df

def prefix_metrics(df: pd.DataFrame, slug: str, keep_cols: List[str]) -> pd.DataFrame:
    """Prefija métricas (todo lo que NO es contexto) con el nombre de la tabla."""
    df = df.copy()
    rename = {}
    for c in df.columns:
        if c in keep_cols:
            continue
        rename[c] = f"{slug}__{c}"
    return df.rename(columns=rename)

def smart_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte a numérico cuando se puede (sin warnings)."""
    df = df.copy()
    for c in df.columns:
        if c in CONTEXT_COLS:
            continue
        try:
            df[c] = pd.to_numeric(df[c])
        except Exception:
            pass
    return df

# ---- Carga / Merge -----------------------------------------------------------

def load_tables_for(comp: str, season: str, raw_root: str) -> Dict[str, pd.DataFrame]:
    """
    Carga todas las tablas *.csv para (comp, season).
    slug = nombre del archivo sin .csv (p.ej. standard, shooting, etc.).
    """
    base_dir = os.path.join(raw_root, comp, season)
    tables = {}
    if not os.path.exists(base_dir):
        return tables

    for fname in os.listdir(base_dir):
        if not fname.lower().endswith(".csv"):
            continue
        slug = fname[:-4]  # sin .csv
        path = os.path.join(base_dir, fname)
        try:
            df = pd.read_csv(path)
            df = clean_fbref_rows(df)
            df = add_keys(df, season, comp)
            tables[slug] = df
        except Exception as e:
            print(f"[!] Error leyendo {path}: {e}")
    return tables

def compact_on_keys(df: pd.DataFrame, join_keys: List[str]) -> pd.DataFrame:
    """
    Asegura 1 fila por llave:
    - elimina filas con llaves vacías
    - agrupa por llaves y toma la primera
    """
    df = df.copy()
    if df.empty:
        return df

    key_ok = True
    for k in join_keys:
        if k not in df.columns:
            # si no existen llaves, no podemos usar estas filas
            return df.iloc[0:0]
        key_ok &= df[k].astype(str).str.len() > 0
    df = df[key_ok]
    if df.empty:
        return df

    df = df.sort_index()
    return df.groupby(join_keys, as_index=False).first()

def merge_season_comp(comp: str, season: str, raw_root: str) -> pd.DataFrame | None:
    tables = load_tables_for(comp, season, raw_root)
    if not tables:
        return None

    keep_cols = list(CONTEXT_COLS)

    # Prefijar métricas y asegurar columnas de contexto mínimas
    for slug, df in list(tables.items()):
        for c in ["Nation","Pos","Comp","Age","Born"]:
            if c not in df.columns:
                df[c] = pd.NA
        tables[slug] = prefix_metrics(df, slug, keep_cols)

    # Base: 'standard' si existe; si no, la primera
    order = ["standard","shooting","passing","defense","gca","keepers"]
    present = [s for s in order if s in tables] or list(tables.keys())
    base_slug = present[0]
    base = tables[base_slug]

    join_keys = ["player_key","squad_key","season_key","comp_key"]

    # Compactar todas a 1 fila por llave
    base = compact_on_keys(base, join_keys)
    others = {s: compact_on_keys(tables[s], join_keys) for s in present[1:]}

    def _prep_right(right: pd.DataFrame) -> pd.DataFrame:
        # Solo llaves + métricas prefijadas (evita colisiones con contexto)
        metric_cols = [c for c in right.columns if (c not in join_keys and c not in CONTEXT_COLS)]
        return right[list(join_keys) + metric_cols]

    def _merge(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        r = _prep_right(right)
        if r.empty:
            return left
        try:
            return pd.merge(left, r, how="left", on=join_keys, validate="m:1")
        except Exception:
            r2 = compact_on_keys(r, join_keys)
            return pd.merge(left, r2, how="left", on=join_keys, validate="m:1")

    merged = reduce(_merge, others.values(), base)

    # Orden de columnas: contexto primero
    front = ["Player","Squad","Nation","Pos","Comp","Age","Born","Season","Competition",
             "player_key","squad_key","season_key","comp_key"]
    existing_front = [c for c in front if c in merged.columns]
    rest = [c for c in merged.columns if c not in existing_front]
    merged = merged[existing_front + rest]

    return smart_numeric(merged)

def scan_all(raw_root: str) -> List[tuple]:
    todo = []
    if not os.path.exists(raw_root):
        return todo
    for comp in sorted(os.listdir(raw_root)):
        comp_dir = os.path.join(raw_root, comp)
        if not os.path.isdir(comp_dir):
            continue
        for season in sorted(os.listdir(comp_dir)):
            season_dir = os.path.join(comp_dir, season)
            if not os.path.isdir(season_dir):
                continue
            has_csv = any(fn.lower().endswith(".csv") for fn in os.listdir(season_dir))
            if has_csv:
                todo.append((comp, season))
    return todo

# ---- Main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Fusiona CSVs de FBref (data/raw) en un dataset maestro sin duplicados/cartesianazos."
    )
    ap.add_argument("--raw-dir", type=str, default=DEFAULT_RAW_ROOT, help="Directorio raíz de datos crudos (default: data/raw).")
    ap.add_argument("--out-dir", type=str, default=DEFAULT_OUT_DIR, help="Directorio de salida (default: data/processed).")
    ap.add_argument("--out-name", type=str, default="master", help="Nombre base del archivo de salida (sin extensión).")
    args = ap.parse_args()

    raw_root = args.raw_dir
    out_dir  = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    pairs = scan_all(raw_root)
    if not pairs:
        print("[!] No se encontraron datos en data/raw/. Descarga primero con el scraper.")
        return

    frames = []
    print(f"[i] Detectadas {len(pairs)} combinaciones (Competition, Season).")
    for comp, season in pairs:
        print(f"[i] Uniendo {comp} — {season} ...")
        merged = merge_season_comp(comp, season, raw_root)
        if merged is not None and len(merged):
            frames.append(merged)
        else:
            print(f"    [·] Saltando {comp} — {season} (sin tablas).")

    if not frames:
        print("[!] No se pudo construir el dataset maestro (frames vacíos).")
        return

    master = pd.concat(frames, ignore_index=True)

    sort_cols = [c for c in ["Season","Competition","Squad","Player"] if c in master.columns]
    if sort_cols:
        master = master.sort_values(sort_cols, kind="stable")

    csv_path = os.path.join(out_dir, f"{args.out_name}.csv")
    pq_path  = os.path.join(out_dir, f"{args.out_name}.parquet")

    master.to_csv(csv_path, index=False, encoding="utf-8")
    try:
        master.to_parquet(pq_path, index=False)
    except Exception as e:
        print(f"[!] No se pudo escribir Parquet (ok ignorar): {e}")

    print(f"[✓] Guardado dataset maestro:\n  - CSV: {csv_path}\n  - Parquet: {pq_path}\n  Filas: {len(master)}  Columnas: {len(master.columns)}")

if __name__ == "__main__":
    main()
