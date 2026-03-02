# 2027 Presidential Election Prediction Project (Lyon Scope)

This project aims to build a predictive model for the 2027 French presidential elections using historical data from the city of Lyon (INSEE Code: 69123). It relies on a medallion data architecture (Bronze, Silver, Gold) and a containerized development environment.

## 🛠 Prerequisites

To ensure reproducibility, the project uses Docker and VS Code:

- **Docker Desktop** (or Engine on Linux).
- **VS Code**.
- **Dev Containers** extension by Microsoft.

## 🚀 Quick Start

1.  **Open the project**: Launch VS Code and open the project root folder.
2.  **Start the container**:
    - A notification should prompt you to "Reopen in Container".
    - Alternatively, press `F1` (or `Ctrl+Shift+P`) and type: `Dev Containers: Reopen in Container`.
3.  **Automatic Setup**: The container will build the image (Spark 3.5, Java 17, Python 3.12) and install dependencies defined in `requirements.txt`.
4.  **Ready to code**: Once the build is finished, you are inside the container in the `/app` directory.

## 📂 Project Structure

The architecture follows Data Engineering industry standards:

```text
.
├── data-raw/           # Immutable source data (CSV).
├── bronze/             # Bronze Layer: Raw data in Parquet format + lineage.
├── silver/             # Silver Layer: Cleaned and normalized data (WIP).
├── gold/               # Gold Layer: ML-ready tables and Databricks integration.
├── notebooks/          # Jupyter exploration lab.
├── src/
│   ├── common/         # Shared utilities (SparkSession, Logging).
│   ├── etl/            # Production scripts (Ingestion, Transformation).
│   │   └── bronze/     # Bronze layer specific logic.
│   └── config.py       # Centralized configuration (Paths, Constants).
└── .devcontainer/      # VS Code Docker environment configuration.
```

## ⚙️ How to work on a dataset?

### 1. ETL Script Development

Transformation scripts should be located in `src/etl/`. To run a script while respecting module imports:

```bash
python -m src.etl.bronze.bronze_presidentielle
```

### 2. Interactive Exploration

The `notebooks/` directory is dedicated to exploratory analysis. The Jupyter extension is pre-configured to use the container's kernel with Spark.

- **Pro Tip**: DataFrame display is optimized (Eager Evaluation) for a "Databricks-like" visual rendering.

### 3. Debugging

Open any Python script and press **F5**. The configuration in `.vscode/launch.json` is set up to launch the current script with the interactive debugger and the `PYTHONPATH` correctly configured.

## 💡 Best Practices & Standards

- **Path Centralization**: Always use `src.config` to access data directories. Never hardcode relative paths.
- **Spark Session Management**: Import `get_spark_session` from `src.common.spark_session_manager`.
- **Data Lineage**: Every generated table must include `source_file` and `processing_timestamp` metadata columns.
- **Storage Format**: **Parquet** format is mandatory between layers to ensure performance and compatibility with the future Gold layer on Databricks.
- **Logging**: Use the `logging` module (configured in scripts) instead of `print` to ensure traceability in production environments.

---
*Project carried out as part of the MSPR Bloc 3 - Expert Informatique et Science de l'Information.*
