import os
import pandas as pd
from datetime import timedelta

DATA_DIR = "data"

def _read_csv_safe(path: str, **kwargs) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"[EXTRACT] Fichier introuvable : {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="ISO-8859-1", **kwargs)

def extract_sensor_data(sensor_path: str = os.path.join(DATA_DIR, "sensor_data.csv")) -> pd.DataFrame:
    # 1) lire le fichier sensor_data.csv
    df = _read_csv_safe(sensor_path)
    if df.empty:
        print("[EXTRACT] sensor_data.csv est vide ou introuvable.")
        return df

    # 2) convertir la colonne timestamp en vraie date/heure
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 3) garder seulement les 7 derniers jours (option demandé dans le sujet)
    last_day = df["timestamp"].max()
    seven_days_ago = last_day - timedelta(days=7)
    df = df[df["timestamp"] >= seven_days_ago].copy()

    # 4) sauvegarder une copie intermédiaire
    df.to_csv(os.path.join(DATA_DIR, "sensor_data_filtered.csv"), index=False)
    return df

def extract_quality_data(quality_path: str = os.path.join(DATA_DIR, "quality_data.csv")) -> pd.DataFrame:
    # 1) lire le fichier quality_data.csv
    df = _read_csv_safe(quality_path)
    if df.empty:
        print("[EXTRACT] quality_data.csv est vide ou introuvable.")
        return df

    # si tu as une colonne qui indique l'état (par ex. status = completed),
    # on pourra filtrer ici plus tard. Pour l’instant on garde tout.
    return df
