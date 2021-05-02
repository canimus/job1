df = spark.read.csv('test.csv')
( 
    df
        .withColumn('timestamp', F.from_unixtime('sqltimestamp'))
        .drop('sqltimestamp')
        .withWatermark('timestamp', '5 seconds')
        .groupby(F.window(F.col('timestamp'), '5 minute'))
        .agg(
            F.sum(F.when(F.col('col1') == 'cat1', 1).otherwise(0)).alias('count_cat1'), 
            F.count(F.col('col1')).alias('total')
        )
        .withColumn('avg_cat1', F.expr('count_cat1 / total'))
).show(truncate=False)