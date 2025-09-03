# what_is_missing.py — Reporta qué archivos faltan por competencia/temporada/tabla

import os

RAW = os.path.join("data", "raw")

TABLES = ["standard", "shooting", "passing", "defense", "gca", "keepers"]

WANTED = {
    "big5":        ["2022-2023", "2023-2024", "2024-2025"],
    "champions":   ["2022-2023", "2023-2024", "2024-2025"],
    "europa":      ["2022-2023", "2023-2024", "2024-2025"],
    "conference":  ["2022-2023", "2023-2024", "2024-2025"],
    "world_cup":   ["2022"],      # Mundial Qatar 2022
    "euro":        ["2024"],      # Eurocopa Alemania 2024
    "copa_america":["2024"],      # Copa América USA 2024
}

def main():
    any_missing = False
    for comp, seasons in WANTED.items():
        for season in seasons:
            season_dir = os.path.join(RAW, comp, season)
            want = {t: False for t in TABLES}
            if os.path.isdir(season_dir):
                for t in TABLES:
                    path = os.path.join(season_dir, f"{t}.csv")
                    if os.path.exists(path) and os.path.getsize(path) > 1000:
                        want[t] = True
            missing = [t for t, ok in want.items() if not ok]
            if missing:
                any_missing = True
                print(f"[ ] Falta {comp}/{season}: {', '.join(missing)}")
            else:
                print(f"[✓] OK     {comp}/{season}")
    if not any_missing:
        print("\nTodo está completo ✅")
    else:
        print("\nUsa uefa_scraper.py o html_to_csv.py para descargar solo lo que falte.")

if __name__ == "__main__":
    main()
