# # **Importing modules and configuring PySpark Session**
# importing module
import pyspark
 
# import pyspark sql functions
import pyspark.sql.functions as f 

# configure PySpark session
spark.conf.set('sprk.sql.parquet.vorder.enabled', 'true');
spark.conf.set('spark.microsoft.delta.optimizeWrite.enabled', 'true');

# # **Retrieving Historical Data**
historicalData = spark.read.format("csv").option("header","true").load("HistoricalData.csv")
# historicalData now is a Spark DataFrame containing CSV data from "HistoricalData.csv".

# append isDuplicate and isNull columns to historicalData frame
historicalData = historicalData.withColumn("isDuplicate", f.lit(False)).withColumn("isNull", f.lit(False))

# display historicalData
historicalData.show()


# # **Retrieving New Data to load into the database**
newData = spark.read.format("csv").option("header","true").load("NewData.csv")
# newData now is a Spark DataFrame containing CSV data from "NewData.csv".

# append isDuplicate and isNull columns to newData frame
newData = newData.withColumn("isDuplicate", f.lit(False)).withColumn("isNull", f.lit(False))

# display newData
newData.show()


# # **Tagging rows with duplicate or NULL values before storing new data to the database**
# define columns to check for duplicate values at
headers = ["serialNumber", "partNumber"]

# select and pool values for specific columns from the historicalData frame
mask = historicalData.select(*[f.col(h).alias("historical" + h[0].upper() + h[1:]) for h in headers]).distinct()

# generate condition for comparing newData to historicalData
condition = None
for h in headers:
    if condition is None:
        condition = (newData[h] == mask["historical" + h[0].upper() + h[1:]])
    else:
        condition = condition | (newData[h] == mask["historical" + h[0].upper() + h[1:]])

# generate joinedData for comparison
joined = newData.join(mask, (condition), "left")

# generate condition to tag duplicate values
condition = None
for h in headers:
    if condition is None:
        condition = (f.col("historical" + h[0].upper() + h[1:]).isNotNull())
    else:
        condition = condition | (f.col("historical" + h[0].upper() + h[1:]).isNotNull())

# tag rows with any duplicate column value in the data set
newData = joined.withColumn('isDuplicate', f.col('isDuplicate') | condition)

# drop the 'historical' columns from the newData frame
for h in headers:
    newData = newData.drop("historical" + h[0].upper() + h[1:])

# tag rows with any null value in the data set
newData = newData.withColumn("isNull", f.coalesce(f.greatest(*[f.col(c).isNull() for c in newData.columns])))

# merge tagged new data into historical data
merged = historicalData.union(newData)
display(merged)

