import argparse
import os
from datetime import datetime

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession, functions as F


def run_spark_job(master_url: str, net_id: str):
    """Run Spark job to analyze cluster usage patterns from S3 logs."""
    spark = (
        SparkSession.builder
        .appName(f"problem2_{net_id}")
        .master(master_url)
        # S3A 文件系统与凭证（使用 EC2 Instance Profile）
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
        .getOrCreate()
    )

    # 读取所有日志（S3A）
    input_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/*/*"
    lines = (
        spark.read.text(input_path)
        .withColumnRenamed("value", "log_entry")
        .withColumn("src_file", F.input_file_name())
    )

    # 过滤掉不是以数字开头的无效行（例如 "Picked up _JAVA_OPTIONS"）
    lines = lines.filter(F.col("log_entry").rlike(r"^[0-9]"))

    # 从文件名中提取 application_id / cluster_id / app_number
    lines = (
        lines
        .withColumn(
            "application_id",
            F.regexp_extract(F.col("src_file"), r"(application_\d+_\d+)", 1)
        )
        .withColumn(
            "cluster_id",
            F.regexp_extract(F.col("application_id"), r"application_(\d+)_\d+", 1)
        )
        .withColumn(
            "app_number",
            F.regexp_extract(F.col("application_id"), r"_(\d+)$", 1)
        )
    )

    # 兼容两种时间格式：
    # 1) yy/MM/dd HH:mm:ss 开头
    ts1 = F.to_timestamp(F.substring(F.col("log_entry"), 1, 17), "yy/MM/dd HH:mm:ss")
    # 2) yyyy-MM-dd HH:mm:ss[,SSS] 内嵌
    ts2_raw = F.regexp_extract(
        F.col("log_entry"),
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{1,3})?)",
        1
    )
    ts2_clean = F.regexp_replace(ts2_raw, r",\d{1,3}$", "")
    ts2 = F.to_timestamp(ts2_clean, "yyyy-MM-dd HH:mm:ss")

    lines = (
        lines
        .withColumn("ts", F.coalesce(ts1, ts2))
        .where((F.col("application_id") != "") & F.col("ts").isNotNull())
    )

    # 汇总每个 application 的开始/结束时间
    timeline_df = (
        lines.groupBy("cluster_id", "application_id", "app_number")
             .agg(
                 F.min("ts").alias("start_time"),
                 F.max("ts").alias("end_time")
             )
             .orderBy("cluster_id", "app_number")
    )

    # 每个 cluster 的应用数与时间段
    cluster_summary_df = (
        timeline_df.groupBy("cluster_id")
                   .agg(
                       F.count("*").alias("num_applications"),
                       F.min("start_time").alias("cluster_first_app"),
                       F.max("end_time").alias("cluster_last_app")
                   )
                   .orderBy(F.col("num_applications").desc())
    )

    os.makedirs("data/output", exist_ok=True)

    # 保存 CSV
    timeline_pdf = timeline_df.toPandas()
    timeline_pdf.to_csv("data/output/problem2_timeline.csv", index=False)

    summary_pdf = cluster_summary_df.toPandas()
    summary_pdf.to_csv("data/output/problem2_cluster_summary.csv", index=False)

    # 统计摘要
    total_clusters = int(len(summary_pdf))
    total_apps = int(len(timeline_pdf))
    avg_apps = float(summary_pdf["num_applications"].mean()) if total_clusters > 0 else 0.0

    busiest = (
        summary_pdf.sort_values("num_applications", ascending=False)
        .head(5)
        .to_dict(orient="records")
    )

    with open("data/output/problem2_stats.txt", "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for r in busiest:
            f.write(f"  Cluster {r['cluster_id']}: {r['num_applications']} applications\n")

    spark.stop()


def generate_visualizations():
    """Generate bar chart and density plot from CSVs with safe guards."""
    out_dir = "data/output"
    summary_csv = os.path.join(out_dir, "problem2_cluster_summary.csv")
    timeline_csv = os.path.join(out_dir, "problem2_timeline.csv")

    if not (os.path.exists(summary_csv) and os.path.exists(timeline_csv)):
        print("Missing CSV files for visualization. Please run Spark job first.")
        return

    summary_df = pd.read_csv(summary_csv)
    timeline_df = pd.read_csv(timeline_csv)

    os.makedirs(out_dir, exist_ok=True)

    # —— Bar Chart ——
    plt.figure(figsize=(10, 6))
    if len(summary_df) == 0:
        plt.title("Number of Applications per Cluster (no data)")
        plt.xlabel("Cluster ID")
        plt.ylabel("Number of Applications")
        plt.savefig(os.path.join(out_dir, "problem2_bar_chart.png"), dpi=300)
        plt.close()
    else:
        ax = sns.barplot(
            data=summary_df,
            x="cluster_id",
            y="num_applications",
            palette="viridis"
        )
        if ax.containers:
            try:
                ax.bar_label(ax.containers[0], fmt='%d', label_type='edge', padding=3)
            except Exception:
                pass
        plt.title("Number of Applications per Cluster")
        plt.xlabel("Cluster ID")
        plt.ylabel("Number of Applications")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "problem2_bar_chart.png"), dpi=300)
        plt.close()

    # —— Density / Histogram ——
    plt.figure(figsize=(10, 6))
    if len(timeline_df) == 0:
        plt.title("Job Duration Distribution (no data)")
        plt.xlabel("Duration (seconds, log scale)")
        plt.ylabel("Count")
        plt.savefig(os.path.join(out_dir, "problem2_density_plot.png"), dpi=300)
        plt.close()
    else:
        timeline_df["start_time"] = pd.to_datetime(timeline_df["start_time"])
        timeline_df["end_time"] = pd.to_datetime(timeline_df["end_time"])
        timeline_df["duration_sec"] = (
            timeline_df["end_time"] - timeline_df["start_time"]
        ).dt.total_seconds()

        if "cluster_id" in timeline_df.columns and timeline_df["cluster_id"].notna().any():
            largest_cluster = timeline_df["cluster_id"].value_counts().idxmax()
            cluster_data = timeline_df.query("cluster_id == @largest_cluster").copy()
        else:
            largest_cluster = "N/A"
            cluster_data = timeline_df.copy()

        if len(cluster_data) == 0 or cluster_data["duration_sec"].dropna().empty:
            plt.title("Job Duration Distribution (no data)")
            plt.xlabel("Duration (seconds, log scale)")
            plt.ylabel("Count")
        else:
            sns.histplot(cluster_data["duration_sec"].dropna(), kde=True, log_scale=True, color="skyblue")
            plt.title(f"Job Duration Distribution (Cluster {largest_cluster}, n={len(cluster_data)})")
            plt.xlabel("Duration (seconds, log scale)")
            plt.ylabel("Count")

        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "problem2_density_plot.png"), dpi=300)
        plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master", nargs="?", default=None, help="Spark master URL")
    parser.add_argument("--net-id", default=None, help="Your net ID, e.g., abc123")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark and only regenerate charts")
    args = parser.parse_args()

    os.makedirs("data/output", exist_ok=True)

    if not args.skip_spark:
        if not args.master or not args.net_id:
            raise ValueError("Must provide master URL and --net-id when running Spark job")
        run_spark_job(args.master, args.net_id)

    generate_visualizations()


if __name__ == "__main__":
    main()