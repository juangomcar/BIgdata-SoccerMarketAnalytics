# fbref_scraper_all.py — scraper robusto con headers, reintentos, cooldown y CLI flexible
import os, time, random, argparse
from io import StringIO
import pandas as pd
import requests

# ===== Tablas a scrapear (ruta relativa en FBref) =====
TABLES = {
    "standard": "stats/players",
    "shooting": "shooting/players",
    "passing":  "passing/players",
    "defense":  "defense/players",
    "gca":      "gca/players",
    "keepers":  "keepers/players",
}

# ===== Competencias con sus códigos en FBref =====
COMPETITIONS = {
    "big5": {
        "base": "https://fbref.com/en/comps/Big5",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
        "suffix": "Big-5-European-Leagues-Stats",
    },
    "champions": {
        "base": "https://fbref.com/en/comps/8",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
        "suffix": "Champions-League-Stats",
    },
    "europa": {
        "base": "https://fbref.com/en/comps/19",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
        "suffix": "Europa-League-Stats",
    },
    "conference": {
        "base": "https://fbref.com/en/comps/220",
        "seasons": ["2022-2023", "2023-2024", "2024-2025"],
        "suffix": "Europa-Conference-League-Stats",
    },
    "world_cup": {
        "base": "https://fbref.com/en/comps/1",
        "seasons": ["2022"],
        "suffix": "World-Cup-Stats",
    },
    "euro": {
        "base": "https://fbref.com/en/comps/676",
        "seasons": ["2020"],
        "suffix": "European-Championship-Stats",
    },
    "copa_america": {
        "base": "https://fbref.com/en/comps/29",
        "seasons": ["2021"],
        "suffix": "Copa-America-Stats",
    },
}

DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/127.0.0.1 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
}

def fetch_html(url: str,
               session: requests.Session,
               attempt: int,
               base_sleep: float,
               jitter: float,
               cooldown_403: float) -> str | None:
    """GET con headers; maneja 403/429 con cooldown y respeta Retry-After si existe."""
    # pequeña espera inicial para distribuir arranques
    if attempt == 1:
        time.sleep(random.uniform(2.0, 6.0))

    try:
        r = session.get(url, timeout=30)
        status = r.status_code

        if status == 200:
            return r.text

        # Respeta Retry-After si viene en headers
        retry_after = r.headers.get("Retry-After")
        if retry_after:
            try:
                wait = float(retry_after)
            except ValueError:
                wait = cooldown_403 * attempt
            print(f"   [!] status{status} + Retry-After={wait:.0f}s en {url}")
            time.sleep(wait)
            return None

        if status in (403, 429):
            wait = cooldown_403 * attempt + random.uniform(0, jitter)
            print(f"   [!] status{status} en {url} ...pausando {wait:.1f}s")
            time.sleep(wait)
            return None

        # Otros HTTP → backoff simple
        wait = base_sleep * attempt + random.uniform(0, jitter)
        print(f"   [!] HTTP {status} en {url} ...reintento en {wait:.1f}s")
        time.sleep(wait)
        return None

    except requests.RequestException as e:
        wait = base_sleep * attempt + random.uniform(0, jitter)
        print(f"   [!] Error de red: {e} ...reintento en {wait:.1f}s")
        time.sleep(wait)
        return None

def read_table_with_headers(url: str,
                            session: requests.Session,
                            max_retries: int,
                            min_sleep: float,
                            max_sleep: float,
                            cooldown_403: float):
    """Descarga HTML con headers, reintentos y devuelve la 1ª tabla parseada."""
    jitter = max(0.0, max_sleep - min_sleep)
    for attempt in range(1, max_retries + 1):
        html = fetch_html(url, session, attempt, base_sleep=min_sleep, jitter=jitter, cooldown_403=cooldown_403)
        if html:
            try:
                tables = pd.read_html(StringIO(html))
                if tables:
                    return tables[0]
            except ValueError as e:
                print(f"   [!] No se pudo parsear tabla: {e}")
        # si falló, ya dormimos dentro de fetch_html
    return None

def download_table(comp: str, base: str, season: str, path: str, suffix: str, slug: str,
                   session: requests.Session, min_sleep: float, max_sleep: float,
                   max_retries: int, cooldown_403: float):
    url = f"{base}/{season}/{path}/{season}-{suffix}"
    print(f"[+] {comp} – {season} – {slug}: {url}")

    outdir = os.path.join("data", "raw", comp, season)
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"{slug}.csv")
    if os.path.exists(outpath):
        print(f"   [·] Ya existe, se omite: {outpath}")
        return

    df = read_table_with_headers(
        url, session=session, max_retries=max_retries,
        min_sleep=min_sleep, max_sleep=max_sleep, cooldown_403=cooldown_403
    )
    if df is None:
        print(f"   [!] Falló descarga tras reintentos: {url}")
        return

    # Aplanar columnas si son MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(c) for c in tup if c and str(c) != "nan"]).strip()
            for tup in df.columns
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    df.to_csv(outpath, index=False, encoding="utf-8")
    print(f"   [✓] Guardado en {outpath} ({len(df)} filas, {len(df.columns)} cols)")

    # pausa educada entre URLs
    sleep_s = random.uniform(min_sleep, max_sleep)
    print(f"   [·] Pausa {sleep_s:.1f}s")
    time.sleep(sleep_s)

def main():
    ap = argparse.ArgumentParser(description="Scraper FBref con headers, reintentos y cooldown 403/429.")
    ap.add_argument("--comp", type=str, default="", help="big5, champions, europa, conference, world_cup, euro, copa_america")
    ap.add_argument("--seasons", type=str, default="", help='Ej: "2023-2024,2024-2025" (vacío = por defecto)')
    ap.add_argument("--tables", type=str, default="", help='Ej: "standard,shooting" (vacío = todas)')
    ap.add_argument("--min-sleep", type=float, default=20.0, help="Segundos mínimos de pausa general.")
    ap.add_argument("--max-sleep", type=float, default=35.0, help="Segundos máximos de pausa general.")
    ap.add_argument("--max-retries", type=int, default=6, help="Reintentos por URL.")
    ap.add_argument("--cooldown-403", type=float, default=120.0, help="Cooldown base para 403/429 (escala con el intento).")
    args = ap.parse_args()

    min_sleep = max(2.0, args.min_sleep)
    max_sleep = max(min_sleep + 0.1, args.max_sleep)
    max_retries = max(1, args.max_retries)
    cooldown_403 = max(30.0, args.cooldown_403)

    with requests.Session() as sess:
        sess.headers.update(DEFAULT_HEADERS)

        comps = [args.comp] if args.comp else list(COMPETITIONS.keys())
        for comp in comps:
            meta = COMPETITIONS[comp]
            seasons = [s.strip() for s in args.seasons.split(",") if s.strip()] or meta["seasons"]
            tables  = [t.strip() for t in args.tables.split(",")  if t.strip()] or list(TABLES.keys())

            for season in seasons:
                for slug in tables:
                    path = TABLES[slug]
                    download_table(
                        comp, meta["base"], season, path, meta["suffix"], slug,
                        session=sess, min_sleep=min_sleep, max_sleep=max_sleep,
                        max_retries=max_retries, cooldown_403=cooldown_403
                    )

if __name__ == "__main__":
    main()
