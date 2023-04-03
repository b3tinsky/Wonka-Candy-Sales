import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (SparkSession
  .builder
  .appName("CandySalesCount")
  .getOrCreate())

candy_sales_file = "./candy_sales.csv"

candy_sales_df = (spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(candy_sales_file))

count_candy_sales_df = (candy_sales_df
  .select("country", "candy", "sales")
  .groupBy("country", "candy")
  .agg(count("sales").alias("Total"))
  .orderBy("Total", ascending=False))

count_candy_sales_df.show(n=60, truncate=False)
print(f"Total Rows = {count_candy_sales_df.count()}")

ita_count_candy_sales_df = (candy_sales_df
  .select("country", "candy", "sales")
  .where(candy_sales_df.country == "ITA")
  .groupBy("country", "candy")
  .agg(count("sales").alias("Total"))
  .orderBy("Total", ascending=False))

ita_count_candy_sales_df.show(n=10, truncate=False)

spark.stop()