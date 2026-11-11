# DAT535

## Spark Lab Pipelines

Spark pipelines for learning distributed data processing.

### Repo Structure

```text
.
├── README.md                    # This file
├── project_cicd_guide.md        # Dev/Prod CI/CD hands-on guide (GitHub Environments, branch strategy, exercise)
├── run_pipeline.py              # Pipeline orchestrator (lab2 -> lab3)
├── lab2_pipeline.py             # Lab 2: Fundamentals + MapReduce + Medallion Architecture
├── lab3_pipeline.py             # Lab 3: Window functions, joins, UDFs, streaming, production patterns
├── lab1_install-spark.sh        # Lab 1: Bash script to install Spark, Java, Python 3.11 & packages
├── lab2_spark_fundamentals.ipynb# Lab 2 notebook: Fundamentals + MapReduce + Medallion Architecture
├── lab3_advanced_spark.ipynb    # Lab 3 notebook: Window functions, joins, UDFs, streaming, production patterns
└── .github/
    └── workflows/
        ├── pipeline.yml         # Scheduled & dispatch pipeline workflow
        ├── dat535-dev.yml       # Dev branch CI pipeline workflow
        └── dat535-prod.yml      # Main branch production deployment workflow
```

### Overview

The Jupyter notebooks in this folder teach Apache Spark end-to-end across **two consolidated labs**
that share **one single e-commerce clickstream dataset** (generated once in Lab 2, reused by Lab 3):

| Lab   | Notebook                        | Key Concepts                                                                                                                                                                                                                                                 |
| ----- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Lab 2 | `lab2_spark_fundamentals.ipynb` | SparkSession & architecture, RDD/DataFrame/SQL/pandas conversions, column ops & type casting, filtering/sorting/aggregations, Parquet/CSV/JSON I/O, MapReduce with RDDs, the Medallion Architecture (Bronze/Silver/Gold)                                     |
| Lab 3 | `lab3_advanced_spark.ipynb`     | Window functions, partitioning strategies, caching/persistence, every join type + broadcast joins, query optimization, UDFs vs pandas UDFs vs built-ins, Structured Streaming basics, production patterns (incremental processing, data quality, SCD Type 2) |

The `.py` pipeline scripts (`lab2_pipeline.py`, `lab3_pipeline.py`, `run_pipeline.py`) mirror the same
two-lab structure as the notebooks: `lab2_pipeline.py` generates the shared dataset and builds the
Medallion pipeline, and `lab3_pipeline.py` loads Lab 2's Silver output and runs the advanced/production
patterns. Run Lab 2 before Lab 3 (or use `python run_pipeline.py all`).

### Quick Start

1. **Install Prerequisites (Lab 1)**:

    Complete the `lab1_install-spark.sh` script on Ubuntu VM for clearting Spark.

   ```bash
   chmod +x lab1_install-spark.sh
   ./lab1_install-spark.sh
   source ~/spark-env/bin/activate
   ```

2. Run Pipeline Scripts:

    ```bash
    # Run individual labs
    python run_pipeline.py lab2
    python run_pipeline.py lab3

    # Run all labs (Lab 2 then Lab 3)
    python run_pipeline.py all
    ```

### Lab Details

#### Lab 2: Spark Fundamentals & the Medallion Architecture

**Learning Objectives:**

- Create and configure a SparkSession; understand driver/executor/task/partition
- Convert data between RDD, DataFrame, SQL views and pandas
- Perform DataFrame operations (select, withColumn, cast, filter, sort)
- Run aggregations and groupBy operations
- Read/write data in multiple formats (Parquet, CSV, JSON, partitioned)
- Apply MapReduce patterns directly with RDDs (map, flatMap, reduceByKey)
- Build a Bronze -> Silver -> Gold pipeline with data-quality quarantining

**Output (shared with Lab 3):**

```text
~/spark-lab-data/shared/
├── bronze/events/
├── silver/
│   ├── events/
│   └── quarantine/
└── gold/
    ├── daily_metrics/
    ├── user_metrics/
    ├── product_metrics/
    └── category_metrics/

~/spark-lab-data/lab2/            # scratch I/O examples (parquet/csv/json/partitioned)
```

#### Lab 3: Advanced Spark & Production Patterns

**Learning Objectives:**

- Master window functions (ranking, lag/lead, running totals, moving averages)
- Understand partitioning strategies and partition pruning
- Implement caching/persistence and compare storage levels
- Perform every join type (inner/left/right/full/semi/anti) and optimize with broadcast joins
- Apply query optimization techniques (filter/column pushdown, execution plans)
- Compare UDFs, pandas UDFs and built-in functions
- Build a minimal Structured Streaming pipeline with a windowed aggregation
- Learn production patterns: incremental processing, data quality checks, SCD Type 2, safe aggregation

**Input:** loads the Silver-layer dataset produced by Lab 2 from `~/spark-lab-data/shared/silver/events`
(run Lab 2 first).

**Output:**

```text
~/spark-lab-data/lab3/
└── partitioned_data/
    ├── no_partition/
    ├── date_partition/
    └── multi_partition/
```

## GitHub Actions CI/CD Guide

The repo contains the automated workflows for the DAT535 project. These workflows use GitHub Actions to schedule data pipelines, execute Spark jobs, and deploy code updates.

**Important**: These pipelines are designed to execute on **self-hosted runners** configured with Apache Spark and Java.

**For the full dev/prod concepts, GitHub Environments setup, and hands-on exercise, see [project_cicd_guide.md](project_cicd_guide.md).**

### Available Workflows

We have configured the following automated processes:

#### Pipeline ([pipeline.yml](.github/workflows/pipeline.yml))

- **Triggers**:
  - Scheduled cron job (monthly, 1st of the month at 00:00 UTC: `0 0 1 * *` which can be changed).
  - Manual dispatch (workflow_dispatch).
  - Push to `main` is defined in the file but currently commented out.

- **Key Tasks**:
  - Checks out the repository code.
  - Activates the Python environment (`source ~/spark-env/bin/activate`).
  - Sets environment variables (`SPARK_HOME=/opt/spark`, `JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64`).
  - Executes all pipelines via `python $GITHUB_WORKSPACE/run_pipeline.py all`.

### Infrastructure & Environment Setup

These workflows use `runs-on: self-hosted` targeting self-hosted infrastructure equipped with PySpark.

#### Self-Hosted Runner Requirements

The runner environment requires the following predefined paths and dependencies:

- **Python Virtual Environment**: `~/spark-env/`
- **Spark Home**: `/opt/spark`
- **Java Home**: `/usr/lib/jvm/java-8-openjdk-amd64`

**⚠️ Note for Personal Forks:**
If you fork this repository to your personal GitHub account, you will need to set up a self-hosted runner with Spark and Java installed.

### Setting up a Self-Hosted Runner

To execute these pipelines on your own infrastructure (e.g., your VM), follow these steps:

#### Step 1: Register and Start Runner

1. Go to your repository on GitHub.
2. Navigate to **Settings** > **Actions** > **Runners** > **New self-hosted runner**.
3. Select the operating system (linux) and architecture matching (x64) your runner host.
4. Execute the configuration commands provided by GitHub on the VM:

```bash
# Create a folder
mkdir actions-runner && cd actions-runner

# Download the runner package (Get the specific link for your OS from GitHub UI > Settings > Actions > Runners)
curl -o actions-runner-osx-x64-2.337.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.337.0/actions-runner-osx-x64-2.337.0.tar.gz

# Extract
tar xzf ./actions-runner-osx-x64-2.337.0.tar.gz

# Configure (You will need the token from the GitHub UI)
./config.sh --url https://github.com/OWNER/REPO --token YOUR_TOKEN

# Start the runner
# To keep the runner alive after closing the terminal, install as a service:
sudo ./svc.sh install 
sudo ./svc.sh start
```

#### Step 2: Triggering Manually

You can test a pipeline without waiting for the schedule:

1. Go to the **Actions** tab.
2. Select a workflow (e.g., "Pipeline") from the left sidebar.
3. Click the **Run workflow** dropdown button on the right.
4. Click **Run workflow**.

### CI/CD Troubleshooting

- **Virtual Environment Not Found**: Ensure `~/spark-env/bin/activate` exists on the self-hosted runner.
- **Java / Spark Path Mismatch**: Verify `JAVA_HOME` and `SPARK_HOME` paths on your runner match the environment settings.
- **Permission Denied**: If `run_pipeline.py` fails to run, make it executable via `git update-index --chmod=+x run_pipeline.py`.
