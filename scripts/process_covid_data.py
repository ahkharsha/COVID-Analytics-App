import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Define base project path relative to this script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "pipeline_output")

def main():
    spark = SparkSession.builder.appName("COVID_Analytics_Pipeline").getOrCreate()

    print("Loading data...")
    dfs = {
        "full_grouped": spark.read.csv(os.path.join(DATA_DIR, "full_grouped.csv"), header=True, inferSchema=True),
        "clean_complete": spark.read.csv(os.path.join(DATA_DIR, "covid_19_clean_complete.csv"), header=True, inferSchema=True),
        "country_latest": spark.read.csv(os.path.join(DATA_DIR, "country_wise_latest.csv"), header=True, inferSchema=True),
        "day_wise": spark.read.csv(os.path.join(DATA_DIR, "day_wise.csv"), header=True, inferSchema=True),
        "usa_county": spark.read.csv(os.path.join(DATA_DIR, "usa_county_wise.csv"), header=True, inferSchema=True),
        "worldometer": spark.read.csv(os.path.join(DATA_DIR, "worldometer_data.csv"), header=True, inferSchema=True)
    }

    print("Cleaning data...")
    dfs["clean_complete"] = dfs["clean_complete"].fillna({"Province/State": "Unknown"})

    for k in ["full_grouped", "country_latest", "worldometer"]:
        dfs[k] = dfs[k].withColumn("Country/Region", F.regexp_replace(F.regexp_replace(F.col("Country/Region"), "^US$", "USA"), "^Korea$", "South Korea"))

    dfs["full_grouped"] = dfs["full_grouped"].dropDuplicates(["Country/Region", "Date"])

    print("Aggregating basic views...")
    top_cases = dfs["country_latest"].select("Country/Region", "Confirmed").orderBy(F.col("Confirmed").desc()).limit(10)
    top_deaths = dfs["country_latest"].select("Country/Region", "Deaths / 100 Cases").orderBy(F.col("Deaths / 100 Cases").desc()).limit(10)
    who_summary = dfs["full_grouped"].groupBy("WHO Region").agg(
        F.sum("Confirmed").alias("Total_Cases"),
        F.sum("Deaths").alias("Total_Deaths"),
        F.sum("Recovered").alias("Total_Recovered")
    )

    daily = dfs["day_wise"].orderBy("Date")
    death_growth = dfs["day_wise"].withColumn("Prev_Deaths", F.lag("New deaths").over(Window.orderBy("Date"))).withColumn("Death_Growth", F.when((F.col("Prev_Deaths").isNull()) | (F.col("Prev_Deaths") == 0), 0).otherwise((F.col("New deaths") / F.col("Prev_Deaths")) * 100))
    monthly = dfs["full_grouped"].withColumn("Month", F.month("Date")).groupBy("Month").agg(F.sum("Confirmed").alias("Monthly_Cases")).orderBy("Month")

    top_5_region = dfs["country_latest"].withColumn("Rank", F.dense_rank().over(Window.partitionBy("WHO Region").orderBy(F.col("Confirmed").desc()))).filter(F.col("Rank") <= 5)
    
    country_increase = dfs["full_grouped"].withColumn("Daily_Increase", F.col("Confirmed") - F.lag("Confirmed").over(Window.partitionBy("Country/Region").orderBy("Date")))
    usa_trend = country_increase.filter(F.col("Country/Region") == "USA")

    mismatches = dfs["country_latest"].alias("c").join(dfs["worldometer"].alias("w"), "Country/Region").select(
        "Country/Region", 
        F.abs(F.col("c.Confirmed") - F.col("w.TotalCases")).alias("Confirmed_Diff"), 
        F.abs(F.col("c.Deaths") - F.col("w.TotalDeaths")).alias("Death_Diff"), 
        F.abs(F.col("c.Recovered") - F.col("w.TotalRecovered")).alias("Recovery_Diff")
    ).filter(F.col("Confirmed_Diff") > 10000)

    infection_rates = dfs["worldometer"].filter(F.col("Population") > 0).withColumn("Infection_Rate", (F.col("TotalCases") / F.col("Population")) * 100).orderBy(F.col("Infection_Rate").desc()).limit(20)

    usa_states = dfs["usa_county"].groupBy("Province_State").count().orderBy(F.col("count").desc()).limit(20)
    geo_data = dfs["clean_complete"].select("Lat", "Long", "Confirmed").filter(F.col("Lat").isNotNull() & F.col("Long").isNotNull() & (F.col("Confirmed") > 0))

    recovery = dfs["country_latest"].filter(F.col("Confirmed") > 1000).withColumn("Rec_Rate", (F.col("Recovered") / F.col("Confirmed")) * 100)
    best_rec = recovery.orderBy(F.col("Rec_Rate").desc()).limit(10)
    worst_rec = recovery.orderBy(F.col("Rec_Rate").asc()).limit(10)
    
    peaks = dfs["day_wise"].agg(F.max("New cases").alias("Max_Cases"), F.max("New deaths").alias("Max_Deaths")).collect()[0]
    peak_dates = dfs["day_wise"].filter((F.col("New cases") == peaks["Max_Cases"]) | (F.col("New deaths") == peaks["Max_Deaths"]))

    severity = dfs["country_latest"].withColumn("Severity_Category", F.when(F.col("Confirmed") < 10000, "Low").when((F.col("Confirmed") >= 10000) & (F.col("Confirmed") <= 100000), "Medium").when((F.col("Confirmed") > 100000) & (F.col("Confirmed") <= 1000000), "High").otherwise("Critical"))
    sev_counts = severity.groupBy("Severity_Category").count()

    print("Writing files to pipeline_output...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Export to CSV/Parquet using pandas conversion for local environments without hadoop/winutils
    top_cases.toPandas().to_csv(os.path.join(OUTPUT_DIR, "top_cases.csv"), index=False)
    top_deaths.toPandas().to_csv(os.path.join(OUTPUT_DIR, "top_deaths.csv"), index=False)
    who_summary.toPandas().to_csv(os.path.join(OUTPUT_DIR, "who_summary.csv"), index=False)
    
    daily.toPandas().to_csv(os.path.join(OUTPUT_DIR, "daily.csv"), index=False)
    death_growth.toPandas().to_csv(os.path.join(OUTPUT_DIR, "death_growth.csv"), index=False)
    monthly.toPandas().to_csv(os.path.join(OUTPUT_DIR, "monthly_growth.csv"), index=False)
    
    top_5_region.toPandas().to_csv(os.path.join(OUTPUT_DIR, "top_5_region.csv"), index=False)
    usa_trend.toPandas().to_csv(os.path.join(OUTPUT_DIR, "usa_trend.csv"), index=False)
    
    mismatches.toPandas().to_csv(os.path.join(OUTPUT_DIR, "mismatches.csv"), index=False)
    infection_rates.toPandas().to_csv(os.path.join(OUTPUT_DIR, "infection_rates.csv"), index=False)
    
    usa_states.toPandas().to_csv(os.path.join(OUTPUT_DIR, "usa_states.csv"), index=False)
    geo_data.toPandas().to_csv(os.path.join(OUTPUT_DIR, "geo_data.csv"), index=False)
    
    best_rec.toPandas().to_csv(os.path.join(OUTPUT_DIR, "best_rec.csv"), index=False)
    worst_rec.toPandas().to_csv(os.path.join(OUTPUT_DIR, "worst_rec.csv"), index=False)
    peak_dates.toPandas().to_csv(os.path.join(OUTPUT_DIR, "peak_dates.csv"), index=False)
    
    sev_counts.toPandas().to_csv(os.path.join(OUTPUT_DIR, "severity_counts.csv"), index=False)
    severity.toPandas().to_parquet(os.path.join(OUTPUT_DIR, "severity_categories.parquet"), index=False)

    print("Pipeline run successfully.")
    spark.stop()

if __name__ == "__main__":
    main()