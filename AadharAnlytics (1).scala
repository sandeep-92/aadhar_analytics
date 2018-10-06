// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder.appName("Aadhar").getOrCreate

// COMMAND ----------

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

// COMMAND ----------

//creating dataframe from csv file
val df = spark.read.format("csv")
.option("delimeter", ",")
.option("dateFormat", "yyyyMMdd")
.schema(mySchema)
.load("/FileStore/tables/aadhaar_data.csv")
df.persist()

// COMMAND ----------

//checking the schema of file
df.printSchema()

// COMMAND ----------

//checking the data in dataframe
df.show(5)

// COMMAND ----------

//Find the count and name of registrars in the table
df.groupBy("registrar").count().show(5)

// COMMAND ----------

//Find the number of states, districts in each state and sub-districts in each district
df.groupBy("state").agg(countDistinct("district"), countDistinct("sub_district")).show(5)

// COMMAND ----------

//Find out the names of private agencies for each state
df.select(col("state"), col("private_agency")).show(5)

// COMMAND ----------

//Find top 3 states generating most number of Aadhaar cards?
df.groupBy("state").agg(sum(col("aadhar_generated"))).orderBy(desc("sum(aadhar_generated)")).show(3)

// COMMAND ----------

//Find top 3 districts where enrolment numbers are maximum
df.groupBy("district").agg(sum(col("aadhar_generated") + col("rejected"))).orderBy(desc("sum((aadhar_generated + rejected))")).show(3)

// COMMAND ----------

//Find the no. of Aadhaar cards generated in each state?
df.groupBy("state").agg(sum(col("aadhar_generated"))).orderBy(desc("sum(aadhar_generated)")).show(5)

// COMMAND ----------

//Find the number of unique pincodes in the data
df.groupBy("pincode").count().show(5)

// COMMAND ----------

//Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
df.groupBy("state").agg(sum(col("rejected"))).filter(col("state") === "Uttar Pradesh" or col("state") === "Maharashtra").show()

// COMMAND ----------

//Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest
df.groupBy("state", "gender").agg(sum("aadhar_generated")).filter(col("gender") === "M").orderBy(desc("sum(aadhar_generated)")).show(3)

// COMMAND ----------

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
