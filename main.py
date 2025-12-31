from etl.extract import extract_sensor_data, extract_quality_data
from etl.transform import (
    clean_sensor_data,
    standardize_sensor_data,
    join_sensor_quality,
    build_hourly_summary,
)
from etl.load import (
    get_connection,
    create_tables,
    load_sensor_readings,
    load_quality_checks,
    load_hourly_summary,
)

def run_pipeline():
    print("=== PHASE 1 : EXTRACT ===")

    # Task 1.1
    print("\n=== Task 1.1 : Extract Sensor Data ===")
    sensor_df = extract_sensor_data()
    print(f"[EXTRACT SENSOR] Lignes totales : {len(sensor_df)}")
    print("\n=== Aperçu sensor_df (brut) ===")
    print(sensor_df.head())          # 5 premières lignes

    # Task 1.2
    print("\n=== Task 1.2 : Extract Quality Data ===")
    quality_df = extract_quality_data()
    print(f"[EXTRACT QUALITY] Lignes totales : {len(quality_df)}")
    print("\n=== Aperçu quality_df (brut) ===")
    print(quality_df.head())

    print("\n=== PHASE 2 : TRANSFORM ===")

    # Task 2.1
    print("\n=== Task 2.1 : Clean Sensor Data ===")
    cleaned_df = clean_sensor_data(sensor_df)
    print("=== Aperçu cleaned_sensor_df ===")
    print(cleaned_df.head())

    # Task 2.2
    print("\n=== Task 2.2 : Standardize Sensor Data ===")
    standardized_df = standardize_sensor_data(cleaned_df)
    print("=== Aperçu standardized_sensor_df ===")
    print(standardized_df.head())

    # Task 2.3
    print("\n=== Task 2.3 : Join Sensor + Quality Data ===")
    joined_df = join_sensor_quality(standardized_df, quality_df)
    print("=== Aperçu joined_df ===")
    print(joined_df.head())
    print(f"\n[{len(joined_df)} rows x {len(joined_df.columns)} columns]")

    # Task 2.4
    print("\n=== Task 2.4 : Calculate Hourly Summaries ===")
    hourly_summary_df = build_hourly_summary(joined_df)
    print("=== Aperçu hourly_summary_df ===")
    print(hourly_summary_df.head())
    print(f"\n[{len(hourly_summary_df)} rows x {len(hourly_summary_df.columns)} columns]")

    print("\n=== PHASE 3 : LOAD ===")

    # Task 3.1
    print("\n=== Task 3.1 : Create Tables in SQLite (Schema) ===")
    conn = get_connection()
    create_tables(conn)

    # Task 3.2
    print("\n=== Task 3.2 : Open Connection to production.db ===")
    # (connexion déjà ouverte ci‑dessus)

    # Task 3.3
    print("\n=== Task 3.3 : Load Data into Tables ===")
    print("-> Loading sensor_readings ...")
    load_sensor_readings(conn)
    print("-> Loading quality_checks ...")
    load_quality_checks(conn)
    print("-> Loading hourly_summary ...")
    load_hourly_summary(conn)

    conn.close()
    print("\n=== Phase 3 LOAD terminée. Base SQLite : production.db ===")

if __name__ == "__main__":
    run_pipeline()

