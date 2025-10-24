import os
import argparse
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T

LOG_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

def build_spark(master: str | None) -> SparkSession:
    b = (SparkSession.builder
         .appName("DSAN6000_Problem1_LogLevels"))
    if master:
        b = b.master(master)

    b = b.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def resolve_input_path(cli_input: str | None) -> str:
    if cli_input:
        return cli_input
    bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if bucket:
        return f"{bucket}/data/*/*"
    if os.path.exists("data/sample"):
        return "data/sample/*/*"
    return "data/raw/*/*"

def write_csv_small(df, out_path: str):
    import csv
    rows = df.collect()
    if not rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([x.name for x in df.schema])
        return
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([x.name for x in df.schema])
        for r in rows:
            w.writerow([r[c] for c in df.columns])

def main():
    ap = argparse.ArgumentParser(description="Problem 1: Log level distribution")
    ap.add_argument("master", nargs="?", default=None,
                    help="Spark master URL, e.g., spark://10.0.0.10:7077; omit for local")
    ap.add_argument("--input", type=str, default=None,
                    help="Input path. Examples: s3://<bucket>/data/*/*  OR  data/sample/*/*")
    ap.add_argument("--sample-n", type=int, default=10, help="Random sample size")
    args = ap.parse_args()

    input_path = resolve_input_path(args.input)
    spark = build_spark(args.master)

    lines = spark.read.text(input_path).withColumnRenamed("value", "log_entry")

    level_re = r"\b(INFO|WARN|ERROR|DEBUG)\b"
    df = (lines
          .withColumn("log_level", F.regexp_extract(F.col("log_entry"), level_re, 1))
          .withColumn("log_level", F.when(F.col("log_level") == "", None).otherwise(F.col("log_level")))
          ).cache()

    total_lines = df.count()
    with_levels = df.filter(F.col("log_level").isNotNull()).cache()
    total_with_levels = with_levels.count()
    unique_levels = with_levels.select("log_level").distinct().count()
    counts = (with_levels
              .filter(F.col("log_level").isin(LOG_LEVELS))
              .groupBy("log_level")
              .agg(F.count(F.lit(1)).alias("count"))
              .orderBy(F.desc("count"), F.asc("log_level")))

    sample_n = max(0, int(args.sample_n))
    sample_df = (with_levels
                 .filter(F.col("log_level").isin(LOG_LEVELS))
                 .orderBy(F.rand())
                 .limit(sample_n)
                 .select("log_entry", "log_level"))

    write_csv_small(counts, "problem1_counts.csv")
    write_csv_small(sample_df, "problem1_sample.csv")

    counts_local = {r["log_level"]: r["count"] for r in counts.collect()}
    def pct(c):
        return (c / total_with_levels * 100.0) if total_with_levels else 0.0

    lines_summary = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_with_levels:,}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:"
    ]
    for lvl in LOG_LEVELS:
        c = counts_local.get(lvl, 0)
        lines_summary.append(f"  {lvl:<5}: {c:>10,} ({pct(c):5.2f}%)")

    with open("problem1_summary.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines_summary) + "\n")

    print("[SUCCESS] Wrote outputs:")
    print(" - problem1_counts.csv")
    print(" - problem1_sample.csv")
    print(" - problem1_summary.txt")

    spark.stop()

if __name__ == "__main__":
    main()