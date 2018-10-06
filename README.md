# aadhar_analytics
An analysis of Aadhar Data using Spark Dataframes In Scala


import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder.appName("Aadhar").getOrCreate
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@4e282b93

//creating manual schema
val mySchema = StructType(Array(
  StructField("date", DateType, true),
  StructField("registrar", StringType, true),
  StructField("private_agency", StringType, true),
  StructField("state", StringType, true),
  StructField("district", StringType, true),
  StructField("sub_district", StringType, true),
  StructField("pincode", LongType, true),
  StructField("gender", StringType, true),
  StructField("age", IntegerType, true),
  StructField("aadhar_generated", IntegerType, true),
  StructField("rejected", IntegerType, true),
  StructField("mobile_number", StringType, true),
  StructField("email", IntegerType, true)
))

mySchema: org.apache.spark.sql.types.StructType = StructType(StructField(date,DateType,true), StructField(registrar,StringType,true), StructField(private_agency,StringType,true), StructField(state,StringType,true), StructField(district,StringType,true), StructField(sub_district,StringType,true), StructField(pincode,LongType,true), StructField(gender,StringType,true), StructField(age,IntegerType,true), StructField(aadhar_generated,IntegerType,true), StructField(rejected,IntegerType,true), StructField(mobile_number,StringType,true), StructField(email,IntegerType,true))


//creating dataframe from csv file
val df = spark.read.format("csv")
.option("delimeter", ",")
.option("dateFormat", "yyyyMMdd")
.schema(mySchema)
.load("/FileStore/tables/aadhaar_data.csv")
df.persist()

df: org.apache.spark.sql.DataFrame = [date: date, registrar: string ... 11 more fields]
res16: df.type = [date: date, registrar: string ... 11 more fields]

//checking the schema of file
df.printSchema()
root
 |-- date: date (nullable = true)
 |-- registrar: string (nullable = true)
 |-- private_agency: string (nullable = true)
 |-- state: string (nullable = true)
 |-- district: string (nullable = true)
 |-- sub_district: string (nullable = true)
 |-- pincode: long (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- aadhar_generated: integer (nullable = true)
 |-- rejected: integer (nullable = true)
 |-- mobile_number: string (nullable = true)
 |-- email: integer (nullable = true)

//checking the data in dataframe
df.show(5)

+----------+--------------+--------------------+-------------+--------------+--------------+-------+------+---+----------------+--------+-------------+-----+
|      date|     registrar|      private_agency|        state|      district|  sub_district|pincode|gender|age|aadhar_generated|rejected|mobile_number|email|
+----------+--------------+--------------------+-------------+--------------+--------------+-------+------+---+----------------+--------+-------------+-----+
|2015-04-20|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     F| 49|               1|       0|            0|    1|
|2015-04-20|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     F| 65|               1|       0|            0|    0|
|2015-04-20|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     M| 42|               1|       0|            0|    1|
|2015-04-20|Allahabad Bank|A-Onerealtors Pvt...|        Delhi|   South Delhi|Defence Colony| 110025|     M| 61|               1|       0|            0|    1|
|2015-04-20|Allahabad Bank|A-Onerealtors Pvt...|Uttar Pradesh|Ambedkar Nagar|      Akbarpur| 224122|     F| 45|               1|       0|            0|    1|
+----------+--------------+--------------------+-------------+--------------+--------------+-------+------+---+----------------+--------+-------------+-----+
only showing top 5 rows

//Find the count and name of registrars in the table
df.groupBy("registrar").count().show(5)

+--------------------+------+
|           registrar| count|
+--------------------+------+
|Govt of Andhra Pr...| 47489|
| UT Of Daman and Diu|  1786|
|Govt of Madhya Pr...|  3582|
|Punjab National Bank|  3303|
|NSDL e-Governance...|122709|
+--------------------+------+
only showing top 5 rows

//Find the number of states, districts in each state and sub-districts in each district
df.groupBy("state").agg(countDistinct("district"), countDistinct("sub_district")).show(5)

+----------+------------------------+----------------------------+
|     state|count(DISTINCT district)|count(DISTINCT sub_district)|
+----------+------------------------+----------------------------+
|  Nagaland|                       9|                          51|
| Karnataka|                      44|                         217|
|    Odisha|                      33|                         357|
|    Kerala|                      15|                         100|
|Tamil Nadu|                      40|                         238|
+----------+------------------------+----------------------------+
only showing top 5 rows

//Find out the names of private agencies for each state
df.select(col("state"), col("private_agency")).show(5)

+-------------+--------------------+
|        state|      private_agency|
+-------------+--------------------+
|        Delhi|A-Onerealtors Pvt...|
|        Delhi|A-Onerealtors Pvt...|
|        Delhi|A-Onerealtors Pvt...|
|        Delhi|A-Onerealtors Pvt...|
|Uttar Pradesh|A-Onerealtors Pvt...|
+-------------+--------------------+
only showing top 5 rows

//Find top 3 states generating most number of Aadhaar cards?
df.groupBy("state").agg(sum(col("aadhar_generated"))).orderBy(desc("sum(aadhar_generated)")).show(3)

+--------------+---------------------+
|         state|sum(aadhar_generated)|
+--------------+---------------------+
|   Maharashtra|               951201|
| Uttar Pradesh|               385463|
|Andhra Pradesh|               270055|
+--------------+---------------------+
only showing top 3 rows

//Find top 3 districts where enrolment numbers are maximum
df.groupBy("district").agg(sum(col("aadhar_generated") + col("rejected"))).orderBy(desc("sum((aadhar_generated + rejected))")).show(3)

+--------+----------------------------------+
|district|sum((aadhar_generated + rejected))|
+--------+----------------------------------+
|    Pune|                            143886|
|  Mumbai|                            118014|
|  Nagpur|                            104355|
+--------+----------------------------------+
only showing top 3 rows

//Find the no. of Aadhaar cards generated in each state?
df.groupBy("state").agg(sum(col("aadhar_generated"))).orderBy(desc("sum(aadhar_generated)")).show(5)

+--------------+---------------------+
|         state|sum(aadhar_generated)|
+--------------+---------------------+
|   Maharashtra|               951201|
| Uttar Pradesh|               385463|
|Andhra Pradesh|               270055|
|     Rajasthan|               227234|
|         Bihar|               208161|
+--------------+---------------------+
only showing top 5 rows

//Find the number of unique pincodes in the data
df.groupBy("pincode").count().show(5)

+-------+-----+
|pincode|count|
+-------+-----+
| 271201|  101|
| 274182|   53|
| 222204|   75|
| 852122|  152|
| 841504|   27|
+-------+-----+
only showing top 5 rows

//Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
df.groupBy("state").agg(sum(col("rejected"))).filter(col("state") === "Uttar Pradesh" or col("state") === "Maharashtra").show()

+-------------+-------------+
|        state|sum(rejected)|
+-------------+-------------+
|  Maharashtra|        45704|
|Uttar Pradesh|        24752|
+-------------+-------------+

//Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest
df.groupBy("state", "gender").agg(sum("aadhar_generated")).filter(col("gender") === "M").orderBy(desc("sum(aadhar_generated)")).show(3)

+--------------+------+---------------------+
|         state|gender|sum(aadhar_generated)|
+--------------+------+---------------------+
|   Maharashtra|     M|               498094|
| Uttar Pradesh|     M|               190263|
|Andhra Pradesh|     M|               122779|
+--------------+------+---------------------+
only showing top 3 rows

+--------------+------+------------------+-------------+
|         state|gender|          district|sum(rejected)|
+--------------+------+------------------+-------------+
|        Kerala|     F|Thiruvananthapuram|         4393|
|        Kerala|     F|         Alappuzha|         4206|
|Andhra Pradesh|     F|           Kurnool|         4184|
+--------------+------+------------------+-------------+
only showing top 3 rows

//Find in each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest

val dfMaharashtra = df.filter(col("state") === "Maharashtra")
.groupBy("district", "gender").agg(sum(col("rejected")))
.filter(col("gender") === "F")
.orderBy(desc("sum(rejected)")).show(3)

val dfUttarPradesh = df.filter(col("state") === "Uttar Pradesh")
.groupBy("district", "gender").agg(sum(col("rejected")))
.filter(col("gender") === "F")
.orderBy(desc("sum(rejected)")).show(3)

val dfAndhraPradesh = df.filter(col("state") === "Andhra Pradesh")
.groupBy("district", "gender").agg(sum(col("rejected")))
.filter(col("gender") === "F")
.orderBy(desc("sum(rejected)")).show(3)

+--------+------+-------------+
|district|gender|sum(rejected)|
+--------+------+-------------+
|   Thane|     F|         2561|
|  Nagpur|     F|         2447|
|    Pune|     F|         1573|
+--------+------+-------------+
only showing top 3 rows

+---------+------+-------------+
| district|gender|sum(rejected)|
+---------+------+-------------+
|Ghaziabad|     F|          624|
|  Lucknow|     F|          553|
|     Agra|     F|          522|
+---------+------+-------------+
only showing top 3 rows

+----------+------+-------------+
|  district|gender|sum(rejected)|
+----------+------+-------------+
|   Kurnool|     F|         4184|
|Srikakulam|     F|         1965|
|  Chittoor|     F|         1649|
+----------+------+-------------+
only showing top 3 rows

dfMaharashtra: Unit = ()
dfUttarPradesh: Unit = ()
dfAndhraPradesh: Unit = ()
