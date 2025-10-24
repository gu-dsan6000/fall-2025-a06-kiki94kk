## Problem 1 Approach:
I used PySpark to process the raw log files from the cluster log directory stored in S3.
Each log line was read as a text record, from which the log level (INFO, WARN, ERROR, etc.) was extracted using regular expressions.
The job computed the frequency distribution of each log level and saved three files:
- problem1_counts.csv – total counts per log level
- problem1_sample.csv – sampled subset of log entries
- problem1_summary.txt – overall statistics summary

Key Findings:
- he dataset contains several million log lines across multiple worker and master nodes.
- The majority of logs were of level INFO, which aligns with expected Spark event logging behavior.
- Only a small fraction of ERROR entries were observed, mostly related to transient Hadoop I/O warnings.

Performance Observations:
- Execution time: 13s
- Optimizations: Used .select() and .groupBy() to reduce shuffles; cached intermediate results to avoid re-reading S3 data.

Screenshots of Spark Web UI showing job execution:
![problem1](../data/output/screenshoot_problem1.png)

## Problem 2 Approach:
This Spark job analyzed historical cluster usage logs from S3 to extract application_id, cluster_id, and timestamps (start_time, end_time).
Aggregations were performed to produce:
- problem2_timeline.csv – application start / end times
- problem2_cluster_summary.csv – usage summary per cluster
- problem2_stats.txt – aggregated statistics
Two visualizations were generated: a bar chart and a density plot.

Key Findings and Insights:
- Total unique clusters: 6
- Total applications: 194
- Average applications per cluster: 32.33
- Cluster 1485248649253 was the most active, running 181 applications between Jan 2017 and Jul 2017.
- Other clusters had far fewer applications (≤ 8), showing an uneven usage distribution.
- Timeline analysis shows parallel activity across clusters during peak periods.

Performance Observations:
- Execution time: 4 min 50 s
- Optimizations: Used regexp_extract and to_timestamp for efficient parsing; grouped data before joins to reduce shuffle size.

Screenshots of Spark Web UI showing job execution:
![problem2](../data/output/screenshoot_problem2.png)
Explanation of Visualizations:
- Bar Chart 
![bar_chart](../data/output/problem2_bar_chart.png) 
– Number of applications per cluster. The tall bar represents Cluster 1485248649253 as the dominant user.
- Density Plot 
![density](../data/output/problem2_density_plot.png)
– Distribution of application durations (log-scale). Most jobs finished within minutes, with a long-tail of long-running tasks.