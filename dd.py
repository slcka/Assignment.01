from pyspark.sql import SparkSession

# Create a SparkSession
spark = (SparkSession
        .builder
        .appName("SparkSQLExampleApp")
        .getOrCreate()
        )

# Path to data set

csv_file = "C:\AJAN\ebook\Spark\spark-3.0.0-bin-hadoop2.7\data\departuredelays.csv"


# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)

customSchema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"


df = (
    spark.read.format("csv")
        #.option("inferSchema", "true")
        .schema(customSchema)
        .option("header", "true")
        .load(csv_file)
    )
    
    
df.createOrReplaceTempView("us_delay_flights_tbl")

df.printSchema()

spark.sql("""
SELECT distinct
    distance, origin, destination
FROM 
    us_delay_flights_tbl 
WHERE distance > 1000
ORDER BY distance DESC
""").show(10)


(
    df
    .select("distance", "origin", "destination")
    .where(col("distance") > 1000 )
    .orderBy(desc("distance"))
    .show(10)
)



spark.sql("""
SELECT 
    *
FROM 
    us_delay_flights_tbl 
WHERE distance > 1000
ORDER BY distance DESC
""").show(10)


print("\n\n\*************************************************************************************************\n")
print("All flights between San Francisco (SFO) and Chicago (ORD) with at least a two-hour delay:")

spark.sql("""
SELECT 
    AA.* , 
    AA.distance * 1.6 DISTANCE_IN_KM ,
    TO_TIMESTAMP(AA.DATE , "MMddHHmm") DATE_TS ,
    CASE 
        WHEN AA.DELAY > 360 THEN 'VERY LONG'
        WHEN AA.DELAY > 120 THEN 'LONG'
        WHEN AA.DELAY > 60  THEN 'SHORT'
        WHEN AA.DELAY > 0 THEN 'TOLERABLE'
        WHEN AA.DELAY = 0 THEN 'NO DELAY'
        ELSE 'EARLY FLIGHT'
    END AS FLIGHT_DELAYS
FROM 
    us_delay_flights_tbl  AA
WHERE
    1 = 1 
    AND ORIGIN IN( 'SFO' , 'ORD' )
    AND DESTINATION IN( 'SFO' , 'ORD' )
    --AND delay > 120     
ORDER BY DATE DESC
""").show(100)




print("\n\n\*************************************************************************************************")