// Databricks notebook source
// DBTITLE 1,csv file
val df = spark.read.option("header",true).option("inferSchema",true).format("csv").load("dbfs:/FileStore/shared_uploads/akshitnand@gmail.com/owid_covid_data.csv")

// COMMAND ----------

// DBTITLE 1,filter first 100 rows 
df.show(100)

// COMMAND ----------

// DBTITLE 1,Json file
val df1 = spark.read.option("header",true).format("json").load("dbfs:/FileStore/shared_uploads/akshitnand@gmail.com/owid_covid_data.json")

// COMMAND ----------

df1.show(100)

// COMMAND ----------

// DBTITLE 1,Show continent wise case count
df.groupBy("continent").sum("total_cases").alias("total").show()

// COMMAND ----------

df.printSchema

// COMMAND ----------

// DBTITLE 1,Show location wise case count 
df.groupBy("location").sum("total_cases").alias("total").show()

// COMMAND ----------

// DBTITLE 1,Check Year wise which year has more cases in which location
import org.apache.spark.sql.functions.{year, to_date}
val df1=df.withColumn("year",to_date($"date", "dd/MM/yyyy"))

// COMMAND ----------

import org.apache.spark.sql.functions.sum
val df2=df1.groupBy("year","location").agg(sum("total_cases"))
display(df2)

// COMMAND ----------

// DBTITLE 1,check which location is having good handwashing_facilities
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.col

val df2=df1.groupBy("location").agg(sum("handwashing_facilities").alias("hand")).orderBy(col("hand").desc).limit(1)



// COMMAND ----------

display(df2)
