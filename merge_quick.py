# merge_quick.py — Merge minimalista: une lo que haya en data/raw en un master
# - Detecta todas las carpetas con CSVs (data/raw/<comp>/<season>/)
# - Toma "standard.csv" como base si existe; si no, la primera tabla
# - Une por claves simples (Player + Squad + Season + Competition) tras normalizar
# - Compacta duplicados por llave (toma la primera fila)
# - Evita cartesianazos y columnas duplicadas
# - Genera data/processed/master.csv y .parquet

import os
import re
import unicodedata
import argparse
import pandas as pd

RAW_DIR_DEFAULT = os.path.join("data", "raw")
OUT_DIR_DEFAULT = os.path.join("data", "processed")

# ------------ utilidades simples ------------

def _norm_text(s: str) -> str:
    if pd.isna(s): return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s

def _has(colname: str, token: str) -> bool:
    c = unicodedata.normalize("NFKD", str(colname).lower())
    c = "".join(ch for ch in c if not unicodedata.combining(ch))
    return token in re.sub(r"[^a-z0-9]+", "", c)

CANON_MAP = {
    "player": "Player",
    "squad": "Squad",
    "nation": "Nation",
    "country": "Nation",
    "pos": "Pos",
    "position": "Pos",
    "comp": "Comp",
    "competition": "Comp",
    "league": "Comp",
    "age": "Age",
    "born": "Born",
    "birth": "Born",
    "rk": "Rk",
    "rank": "Rk",
}

def canon_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename = {}
    taken = set()
    for col in df.columns:
        for key, canon in CANON_MAP.items():
            if canon in taken:
                continue
            if _has(col, key):
                rename[col] = canon
                taken.add(canon)
                break
    return df.rename(columns=rename)

def clean_fbref(df: pd.DataFrame) -> pd.DataFrame:
    df = canon_columns(df.copy())
    # eliminar duplicados de encabezado y totales de equipo
    if "Rk" in df.columns:
        df = df[df["Rk"].astype(str) != "Rk"]
    if "Player" in df.columns:
        p = df["Player"].astype(str).str.strip().str.lower()
        df = df[~p.isin(["", "nan", "team total", "squad total", "players"])]
    if "Squad" in df.columns:
        df = df[df["Squad"].astype(str).str.strip().str.lower() != ""]
    return df.drop_duplicates()

def add_keys(df: pd.DataFrame, season: str, comp: str) -> pd.DataFrame:
    df = canon_columns(df.copy())
    df["Season"] = season
    df["Competition"] = comp
    for c in ("Player","Squad"):
        if c not in df.columns:
            df[c] = ""
    df["player_key"] = df["Player"].map(_norm_text)
    df["squad_key"]  = df["Squad"].map(_norm_text)
    df["season_key"] = _norm_text(season)
    df["comp_key"]   = _norm_text(comp)
    return df

def compact_by_keys(df: pd.DataFrame, keys) -> pd.DataFrame:
    if any(k not in df.columns for k in keys):
        return df.iloc[0:0].copy()
    ok = True
    for k in keys:
        ok &= df[k].astype(str).str.len() > 0
    df = df[ok].copy()
    if df.empty:
        return df
    return df.groupby(list(keys), as_index=False).first()

# ------------ merge por comp/season ------------

def merge_one_comp_season(comp_dir: str, comp: str, season: str) -> pd.DataFrame | None:
    base_path = os.path.join(comp_dir, season)
    csvs = [f for f in os.listdir(base_path) if f.lower().endswith(".csv")]
    if not csvs:
        return None

    # leer todas
    frames = {}
    for fname in csvs:
        slug = fname[:-4]
        df = pd.read_csv(os.path.join(base_path, fname))
        df = clean_fbref(df)
        df = add_keys(df, season, comp)
        # prefijar métricas para evitar colisiones (excepto contexto)
        context = {"Player","Squad","Nation","Pos","Comp","Age","Born","Season","Competition",
                   "player_key","squad_key","season_key","comp_key","Rk"}
        df = df.rename(columns={c: f"{slug}__{c}" for c in df.columns if c not in context})
        frames[slug] = df

    # base: standard si existe, sino cualquiera
    base_slug = "standard" if "standard" in frames else next(iter(frames))
    left = frames.pop(base_slug)
    join_keys = ["player_key","squad_key","season_key","comp_key"]
    left = compact_by_keys(left, join_keys)

    # une secuencialmente solo llaves + métricas prefijadas
    for slug, right in frames.items():
        cols = join_keys + [c for c in right.columns if c not in join_keys and not c.endswith(("Player","Squad","Season","Competition"))]
        # asegurar que no pasen columnas de contexto
        cols = [c for c in cols if c in right.columns and (c in join_keys or "__" in c)]
        r = right[cols].copy()
        r = compact_by_keys(r, join_keys)
        if r.empty:
            continue
        try:
            left = pd.merge(left, r, how="left", on=join_keys, validate="m:1")
        except Exception:
            # si hay duplicados, recompacta de nuevo por seguridad
            r = compact_by_keys(r, join_keys)
            left = pd.merge(left, r, how="left", on=join_keys, validate="m:1")

    # ordenar columnas: contexto primero
    front = ["Player","Squad","Nation","Pos","Comp","Age","Born","Season","Competition"] + join_keys
    front = [c for c in front if c in left.columns]
    rest = [c for c in left.columns if c not in front]
    left = left[front + rest]
    return left

# ------------ scan & write ------------

def scan_all(raw_dir: str):
    pairs = []
    if not os.path.exists(raw_dir):
        return pairs
    for comp in sorted(os.listdir(raw_dir)):
        comp_dir = os.path.join(raw_dir, comp)
        if not os.path.isdir(comp_dir):
            continue
        for season in sorted(os.listdir(comp_dir)):
            season_dir = os.path.join(comp_dir, season)
            if not os.path.isdir(season_dir):
                continue
            has_csv = any(f.lower().endswith(".csv") for f in os.listdir(season_dir))
            if has_csv:
                pairs.append((comp, comp_dir, season))
    return pairs

def main():
    ap = argparse.ArgumentParser(description="Merge minimalista para FBref.")
    ap.add_argument("--raw-dir", type=str, default=RAW_DIR_DEFAULT)
    ap.add_argument("--out-dir", type=str, default=OUT_DIR_DEFAULT)
    ap.add_argument("--out-name", type=str, default="master")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    pairs = scan_all(args.raw_dir)
    if not pairs:
        print("[!] No hay CSVs en data/raw/.")
        return

    all_frames = []
    print(f"[i] Detectadas {len(pairs)} combinaciones (Competition, Season).")
    for comp, comp_dir, season in pairs:
        print(f"[i] Merge {comp} — {season} ...")
        df = merge_one_comp_season(comp_dir, comp, season)
        if df is None or df.empty:
            print(f"    [·] Sin filas válidas: {comp}/{season}")
            continue
        all_frames.append(df)

    if not all_frames:
        print("[!] No se pudo construir el master (sin frames).")
        return

    master = pd.concat(all_frames, ignore_index=True)
    # orden amable
    sort_cols = [c for c in ["Season","Competition","Squad","Player"] if c in master.columns]
    if sort_cols:
        master = master.sort_values(sort_cols, kind="stable")

    csv_path = os.path.join(args.out_dir, f"{args.out_name}.csv")
    pq_path  = os.path.join(args.out_dir, f"{args.out_name}.parquet")
    master.to_csv(csv_path, index=False, encoding="utf-8")
    try:
        master.to_parquet(pq_path, index=False)
    except Exception as e:
        print(f"[!] No se pudo escribir Parquet (ok): {e}")

    print(f"[✓] Master listo:\n  CSV: {csv_path}\n  Parquet: {pq_path}\n  Filas: {len(master)}  Columnas: {len(master.columns)}")

if __name__ == "__main__":
    main()
