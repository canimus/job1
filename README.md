Please remember that it is streaming data. Here is an example which estimates the count of cat1 in col1 for every 5 minutes. You need to add the code need to estimate the average. If you know the spark, it should take 5-10 minutes.

```scala
val mavgDF = timedDF
.withWatermark("sqltimestamp", "5 seconds")
.groupBy(window(col("sqltimestamp"), "5 minute").as("time_frame"))
.agg(
count( when( col("col1") === "cat1", 1)).as("count")
)
.withColumn("window_start", col("time_frame")("start").cast(TimestampType))
.drop("time_frame")
.orderBy("window_start")
```