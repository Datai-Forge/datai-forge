import re

from pyspark.sql import DataFrame


def clean_column_name(name):
    if not name:
        return "unnamed_column"

    name = name.lower().strip()

    name = (
        name.replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("à", "a")
        .replace("â", "a")
        .replace("î", "i")
        .replace("ï", "i")
        .replace("ô", "o")
        .replace("û", "u")
        .replace("ù", "u")
        .replace("ç", "c")
    )

    name = re.sub(r"[^a-z0-9]", "_", name)

    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def rename_dataframe_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Renames columns in a PySpark DataFrame based on a provided mapping.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        column_mapping (dict): A dictionary where keys are current column names
                               and values are the new column names.

    Returns:
        DataFrame: A new PySpark DataFrame with the specified columns renamed.
    """
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        else:
            print(f"Warning: Column '{old_name}' not found in DataFrame. Skipping rename.")
    return df
