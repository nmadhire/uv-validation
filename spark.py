from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("CountNumbers") \
    .master("local[*]") \
    .getOrCreate()

# Create a list of numbers from 1 to 10
numbers = list(range(1, 11))

# Create a Spark DataFrame from the list of numbers
df = spark.createDataFrame([(num,) for num in numbers], ["number"])

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
