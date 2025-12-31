import os
import uuid
import pandas as pd

DATA_DIR = "data"

def clean_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyer les données capteurs (valeurs invalides, données manquantes)."""
    if df.empty:
        return df

    df = df.copy()

    # Colonnes numériques de ton fichier sensor_data.csv
    numeric_cols = [
        "temperature",
        "vibration",
        "humidity",
        "pressure",
        "energy_consumption",
    ]

    # Valeurs d'erreur à transformer en manquant
    error_values = [-999, -1]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col].replace(error_values, pd.NA, inplace=True)

    # Colonne data_quality
    df["data_quality"] = "good"

    # Contrôles simples d'intervalle
    if "temperature" in df.columns:
        invalid_temp = (df["temperature"] < 0) | (df["temperature"] > 150)
        df.loc[invalid_temp, "temperature"] = pd.NA
        df.loc[invalid_temp, "data_quality"] = "estimated"

    if "pressure" in df.columns:
        invalid_press = (df["pressure"] < 0) | (df["pressure"] > 10)
        df.loc[invalid_press, "pressure"] = pd.NA
        df.loc[invalid_press, "data_quality"] = "estimated"

    if "vibration" in df.columns:
        invalid_vib = (df["vibration"] < 0) | (df["vibration"] > 100)
        df.loc[invalid_vib, "vibration"] = pd.NA
        df.loc[invalid_vib, "data_quality"] = "estimated"

    # Tri par temps et remplissage vers l'avant
    df.sort_values("timestamp", inplace=True)

    existing_numeric = [c for c in numeric_cols if c in df.columns]
    df[existing_numeric] = df[existing_numeric].ffill()

    df["data_quality"] = df["data_quality"].fillna("good")

    df.to_csv(os.path.join(DATA_DIR, "cleaned_sensor_data.csv"), index=False)
    return df


def standardize_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """Standardiser les noms de colonnes et ajouter record_id."""
    if df.empty:
        return df

    df = df.copy()

    # timestamp au bon format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # noms de colonnes en minuscules sans espaces
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # machine_id en texte
    if "machine_id" in df.columns:
        df["machine_id"] = df["machine_id"].astype(str).str.lower()

    # identifiant unique
    df["record_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    df.to_csv(os.path.join(DATA_DIR, "standardized_sensor_data.csv"), index=False)
    return df


def join_sensor_quality(sensor_df: pd.DataFrame,
                        quality_df: pd.DataFrame) -> pd.DataFrame:
    """Joindre les données capteurs et qualité."""
    if sensor_df.empty:
        return sensor_df

    sensor = sensor_df.copy()
    quality = quality_df.copy()

    if quality.empty:
        # pas de données qualité, on ajoute seulement une colonne "not_checked"
        sensor["quality_status"] = "not_checked"
        sensor.to_csv(os.path.join(DATA_DIR, "joined_data.csv"), index=False)
        return sensor

    # standardiser les colonnes qualité
    quality.columns = [c.strip().lower().replace(" ", "_") for c in quality.columns]
    if "timestamp" in quality.columns:
        quality["timestamp"] = pd.to_datetime(quality["timestamp"])

    # colonnes communes pour la jointure
    on_cols = []
    for col in ["timestamp", "machine_id"]:
        if col in sensor.columns and col in quality.columns:
            on_cols.append(col)

    joined = sensor.merge(
        quality,
        how="left",
        on=on_cols,
        suffixes=("", "_q"),
    )

    if "result" in joined.columns:
        joined["quality_status"] = joined["result"].fillna("not_checked")
    else:
        joined["quality_status"] = "not_checked"

    joined.to_csv(os.path.join(DATA_DIR, "joined_data.csv"), index=False)
    return joined


def build_hourly_summary(joined_df: pd.DataFrame) -> pd.DataFrame:
    """Créer un résumé horaire par machine."""
    if joined_df.empty:
        return joined_df

    df = joined_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.floor("H")

    group_cols = ["hour"]
    if "machine_id" in df.columns:
        group_cols.append("machine_id")

    agg_dict = {}
    for col in ["temperature", "pressure", "vibration"]:
        if col in df.columns:
            agg_dict[col] = ["mean", "min", "max"]

    grouped = df.groupby(group_cols).agg(agg_dict)
    grouped.columns = ["_".join(filter(None, col)).strip() for col in grouped.columns]
    grouped = grouped.reset_index()

    # compter les contrôles qualité
    if "result" in df.columns:
        checks = df.groupby(group_cols)["result"].count().reset_index(name="total_checks")
        defects = (
            df[df["result"] == "fail"]
            .groupby(group_cols)["result"]
            .count()
            .reset_index(name="defect_count")
        )
        grouped = grouped.merge(checks, on=group_cols, how="left")
        grouped = grouped.merge(defects, on=group_cols, how="left")
    else:
        grouped["total_checks"] = 0
        grouped["defect_count"] = 0

    grouped["total_checks"] = grouped["total_checks"].fillna(0)
    grouped["defect_count"] = grouped["defect_count"].fillna(0)
    grouped["defect_rate"] = grouped.apply(
        lambda r: (r["defect_count"] / r["total_checks"] * 100)
        if r["total_checks"] > 0
        else 0,
        axis=1,
    )

    grouped.rename(
        columns={
            "temperature_mean": "avg_temperature",
            "temperature_min": "min_temperature",
            "temperature_max": "max_temperature",
            "pressure_mean": "avg_pressure",
            "vibration_mean": "avg_vibration",
        },
        inplace=True,
    )

    grouped.to_csv(os.path.join(DATA_DIR, "hourly_summary.csv"), index=False)
    return grouped
