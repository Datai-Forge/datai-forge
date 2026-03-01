from pyspark.sql import SparkSession

def get_spark_session(app_name="DefaultApp"):
    """
    Crée ou récupère une session Spark centralisée.
    Configuration spécifique pour désactiver le cache HDFS afin de garantir 
    la stabilité sur les systèmes de fichiers locaux (Docker/Hôte).
    """
    spark = (SparkSession.builder 
        .appName(app_name) 
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") 
        .getOrCreate())
    
    return spark
