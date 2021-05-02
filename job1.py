from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from itertools import product

def main():
    spark = (
        SparkSession
            .builder
            .appName('Streaming - Python')
            .master('local[*]')
            .getOrCreate()
    )

    spark.sparkContext.setJobGroup('ReadSocket', 'Reading input from nc command')
    spark.sparkContext.setLogLevel('ERROR')

    df = (
        spark.readStream.format('socket')
        .option('host', '127.0.0.1')
        .option('port', 9999)
        .load()
    )

    # Dynamic
    # ===================
    NUMBER_OF_COLUMNS = 5
    COLUMN_PREFIX = 'col'

    _c = lambda x: str(x[0])+str(x[1])
    cartesian_result = map(_c, product([COLUMN_PREFIX], range(1,NUMBER_OF_COLUMNS+1)))
    expected_columns = ['sqltimestamp'] + list(cartesian_result)

    # Static
    # ===================
    #expected_columns = ['sqltimestamp'] + 'col1,col2,col3,col4,col5'.split(',')

    transformation_1 = (
        df
            # Column structure
            # ================================================
            .withColumn('raw_columns', F.split('value', ','))
            .drop('value')
            .select(*[F.col('raw_columns')[k].alias(v) for k,v in enumerate(expected_columns)])
            # Casting
            .withColumn('timestamp', F.from_unixtime('sqltimestamp').cast('timestamp'))
            .drop('sqltimestamp')
            # Watermarking
            .withWatermark('timestamp', '5 seconds')
            # Aggregation
            .groupby(F.window(F.col('timestamp'), '5 minute'))
            .agg(
                F.sum(F.when(F.col('col1') == 'cat1', 1).otherwise(0)).alias('count_cat1'), 
                F.count(F.col('col1')).alias('total')
            )
            .withColumn('mean_cat1', F.expr('count_cat1 / total'))
    )
    
    run_query = (
        transformation_1
            .writeStream
            .queryName('SocketStream')
            .format('console')
            .option('truncate', 'false')
            .outputMode('update')
    ).start()
    run_query.awaitTermination()
    

if __name__ == "__main__":
    main()