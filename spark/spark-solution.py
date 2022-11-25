# Create SparkSession from builder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType,IntegerType
import sys

spark = SparkSession.builder.master("yarn") \
                    .appName('car') \
                    .getOrCreate()
                    

# read csv file into RDD
df=spark.read.option("header",True).csv(sys.argv[1])

# Clean the dataset
df=df.withColumn('Fine amount',col('Fine amount').cast(DoubleType()))
df=df.withColumn('Ticket number',col('Ticket number').cast(IntegerType()))
# calculate avg for each state
avg_state=df.groupBy("RP State Plate").avg("Fine amount")
avg_state=avg_state.withColumnRenamed("avg(Fine amount)","avg amount")
df=df.na.drop("any")
distinctDF=df.distinct()





# Try joining
joinedDF=distinctDF.join(avg_state,avg_state["RP State Plate"]==distinctDF["RP State Plate"],"inner")
#joinedDF=df.join(avg_state,avg_state["RP State Plate"]==df["RP State Plate"],"inner")

# find cars>avg && Color=WH
output=joinedDF.filter((joinedDF.Color =="WH") & (joinedDF['Fine amount'] > joinedDF["avg amount"]))
tickets=output.select(col("Ticket number")).sort("Ticket number")

# Write to file on sys.argv[2] sorted 
tickets.write.options(header='False',delimiter=',').csv(sys.argv[2])


