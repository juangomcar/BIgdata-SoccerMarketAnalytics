# uefa_scraper.py — Descarga SOLO lo que falte de Champions/Europa/Conference
# - Usa headers y backoff para evitar 403/429
# - Respeta archivos existentes (a menos que pases --force)
# - Puedes elegir comp, seasons y tablas

import os
import time
import random
import argparse
import requests
import pandas as pd
from io import StringIO

TABLES = {
    "standard": "stats/players",
    "shooting": "shooting/players",
    "passing":  "passing/players",
    "defense":  "defense/players",
    "gca":      "gca/players",
    "keepers":  "keepers/players",
}

UEFA = {
    "champions": {
        "base": "https://fbref.com/en/comps/8",
        "suffix": "Champions-League-Stats",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
    },
    "europa": {
        "base": "https://fbref.com/en/comps/19",
        "suffix": "Europa-League-Stats",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
    },
    "conference": {
        "base": "https://fbref.com/en/comps/220",
        "suffix": "Europa-Conference-League-Stats",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
    },
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://fbref.com/",
}

def build_url(base: str, season: str, path: str, suffix: str) -> str:
    return f"{base}/{season}/{path}/{season}-{suffix}"

def fetch_html(url: str, cooldown_403: float, max_retries: int) -> str | None:
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            status = r.status_code
            if status == 200:
                return r.text
            elif status in (403, 429):
                wait = cooldown_403 * attempt
                print(f"   [!] {status} en {url} -> esperando {wait:.0f}s (intento {attempt}/{max_retries})")
                time.sleep(wait)
            else:
                print(f"   [!] HTTP {status} en {url} (intento {attempt}/{max_retries})")
                time.sleep(5 * attempt)
        except Exception as e:
            print(f"   [!] Error de red {e} (intento {attempt}/{max_retries})")
            time.sleep(5 * attempt)
    return None

def download_one(comp: str, meta: dict, season: str, slug: str,
                 out_root: str, min_sleep: float, max_sleep: float,
                 cooldown_403: float, max_retries: int, force: bool) -> bool:
    """True si se guardó; False si se omitió o falló."""
    outdir = os.path.join(out_root, comp, season)
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{slug}.csv")
    if os.path.exists(outpath) and not force and os.path.getsize(outpath) > 1000:
        print(f"   [·] Ya existe, se omite: {outpath}")
        return False

    url = build_url(meta["base"], season, TABLES[slug], meta["suffix"])
    print(f"[+] {comp} – {season} – {slug}: {url}")

    html = fetch_html(url, cooldown_403=cooldown_403, max_retries=max_retries)
    if not html:
        print(f"   [x] No se pudo obtener HTML.")
        return False

    try:
        # usar lxml sobre el HTML descargado
        tables = pd.read_html(StringIO(html))
    except Exception as e:
        print(f"   [x] read_html falló: {e}")
        return False

    if not tables:
        print("   [x] No se encontró ninguna tabla.")
        return False

    df = tables[0]
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(c) for c in tup if c and c != 'nan']).strip()
                      for tup in df.columns]

    df.to_csv(outpath, index=False)
    print(f"   [✓] Guardado {outpath} ({len(df)} filas, {len(df.columns)} cols)")

    # si todo ok, dormir un poco para no espamear
    sleep_s = random.uniform(min_sleep, max_sleep)
    time.sleep(sleep_s)
    return True

def main():
    ap = argparse.ArgumentParser(description="Scraper para Champions/Europa/Conference con pausas y headers.")
    ap.add_argument("--comp", type=str, default="all",
                    choices=["champions", "europa", "conference", "all"],
                    help="Competencia a descargar")
    ap.add_argument("--seasons", type=str, default="",
                    help='Temporadas separadas por coma, ej: "2023-2024,2024-2025". Vacío = todas.')
    ap.add_argument("--tables", type=str, default="",
                    help='Tablas separadas por coma (standard,shooting,passing,defense,gca,keepers). Vacío = todas.')
    ap.add_argument("--raw-dir", type=str, default=os.path.join("data","raw"))
    ap.add_argument("--min-sleep", type=float, default=35.0)
    ap.add_argument("--max-sleep", type=float, default=65.0)
    ap.add_argument("--cooldown-403", type=float, default=300.0)
    ap.add_argument("--max-retries", type=int, default=6)
    ap.add_argument("--force", action="store_true", help="Reescribe aunque exista")
    args = ap.parse_args()

    comps = ["champions","europa","conference"] if args.comp == "all" else [args.comp]
    tables = list(TABLES.keys()) if not args.tables else [t.strip() for t in args.tables.split(",") if t.strip()]
    out_root = args.raw_dir

    for comp in comps:
        meta = UEFA[comp]
        seasons = meta["seasons"] if not args.seasons else [s.strip() for s in args.seasons.split(",") if s.strip()]
        for season in seasons:
            for slug in tables:
                try:
                    download_one(
                        comp, meta, season, slug,
                        out_root=out_root,
                        min_sleep=args.min_sleep, max_sleep=args.max_sleep,
                        cooldown_403=args.cooldown_403, max_retries=args.max_retries,
                        force=args.force
                    )
                except KeyboardInterrupt:
                    print("\nInterrumpido por el usuario.")
                    return
                except Exception as e:
                    print(f"   [x] Error inesperado: {e}")

if __name__ == "__main__":
    main()
