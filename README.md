# California Traffic Collision ETL & Analysis

This repository contains an end-to-end workflow for processing and analyzing California traffic collision data using PySpark, Spark SQL, SQLite, and AWS S3. The project performs extraction, transformation, standardization, and visualization of collision records sourced from the Statewide Integrated Traffic Records System (SWITRS).

## Dataset Source

The dataset is provided in SQLite format and includes structured tables for:
- collisions
- parties
- victims

## Technology Stack

- Python
- SQLite
- Pandas
- Apache Spark (PySpark)
- Spark SQL
- AWS S3
- Google Colab

## Data Extraction

```python
conn = sqlite3.connect('/content/.../collisions.db')
df = pd.read_sql_query("SELECT * FROM collisions", conn)
spark_df = spark.createDataFrame(df)
```

## Transformations

Operations applied in the notebook:
- column renaming
- derived field creation
- recoding
- schema reordering
- Spark SQL view creation

Example:
```python
df_main = df_main.withColumn(
    "hit_and_run_flag",
    when(col("hit_and_run") == "no", 0).otherwise(1)
)
```

## Export to S3

```python
df_final.write.csv("s3://<bucket>/traffic-collisions/", header=True)
```

## Charts Generated

Below charts were extracted from the notebook:

![Collision Severity](assets/chart_01.png)
![Severity Counts](assets/chart_02.png)
![Road Surface](assets/chart_03.png)
![Lighting](assets/chart_04.png)
![Collision Type](assets/chart_05.png)
![Party Age](assets/chart_06.png)
![Victim Age](assets/chart_07.png)
![Hit & Run](assets/chart_08.png)
![Primary Factor](assets/chart_09.png)
![Hour Dist](assets/chart_10.png)
![Weekday](assets/chart_11.png)
![Month](assets/chart_12.png)

## Outputs

This project produces:
- cleaned Spark DataFrame
- SQL-ready Spark view
- S3 CSV export
- visualization outputs

## Repo Structure

```
traffic-collisions-etl/
├── assets/
├── notebooks/
├── scripts/
├── data/
└── README.md
```

## Author

Piyush Sharma
PGD Data Science & AI — IIITB
