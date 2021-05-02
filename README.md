# Job 1

## Introduction

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

## Implementation

- Consists in using the socket stream for testing purposes
- `nc -lk 9999` command to start serving socket input stream
- Use of the `DataFrame API` from Apache Spark to unify batch and stream options
- Stream arrives unstructured in `value` column. Is required to break it via `split`
- Then columns are renamed according to the example with prefix `col`
- Casting is performed assuming the input is in `unix` millisecond format
- Aggregation account the total numbers of occurrences in ratio to the total
- Final computation reports the `mean` number of `cat1` values in window