def test_spark_session_is_active(spark):
    count = spark.range(1).count()
    assert count == 1, "La session Spark devrait retourner exactement 1 ligne pour range(1)"

def test_spark_version(spark):
    assert spark.version == "3.5.0", f"Mauvaise version Spark : {spark.version}"
