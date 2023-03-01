// Databricks notebook source
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import *

// COMMAND ----------

// DBTITLE 1,LOAN DATASET


// COMMAND ----------

val df1 = spark.read.option("header",true).format("csv").load("dbfs:/FileStore/shared_uploads/akshitnand@gmail.com/loan.csv")

// COMMAND ----------

df1.printSchema()

// COMMAND ----------

df1.show(5)

// COMMAND ----------

df1.columns.length

// COMMAND ----------

df1.count()

// COMMAND ----------

df1.distinct().count()

// COMMAND ----------

// DBTITLE 1,number of loans in each category
df1.groupBy("Loan Category").count().orderBy($"count".desc).show()
// orderBy($"Date".desc)

// COMMAND ----------

// DBTITLE 1,number of people who have taken more than 1 lack loan

df1.filter(df1("Loan Amount")>"1,00,000").count()

// COMMAND ----------

// DBTITLE 1,number of people with income greater than 60000 rupees

df1.filter(df1("Income")>"60000").count()

// COMMAND ----------

// DBTITLE 1,number of people with 2 or more returned cheques and income less than 50000

df1.filter((df1(" Returned Cheque")>"1") &&(df1("Income")<"50000")).count()

// COMMAND ----------

// DBTITLE 1,number of people with 2 or more returned cheques and are single

df1.filter((df1(" Returned Cheque")>"1") &&(df1("Marital Status")<"SINGLE")).count()

// COMMAND ----------

// DBTITLE 1,number of people with expenditure over 50000 a month 
 
df1.filter((df1("Expenditure")>"50000")).show()

// COMMAND ----------

// DBTITLE 1,CREDIT CARD DATASET
val df2 = spark.read.option("header",true).format("csv").load("dbfs:/FileStore/shared_uploads/akshitnand@gmail.com/credit_card.csv")

// COMMAND ----------

df2.printSchema()

// COMMAND ----------

df2.columns.length

// COMMAND ----------

df2.distinct().count()

// COMMAND ----------

df2.show(5)

// COMMAND ----------

// DBTITLE 1,number of members who are elgible for credit card

df2.filter(df2("CreditScore")>700).count()

// COMMAND ----------

// DBTITLE 1,number of members who are  elgible and active in the bank

df2.filter((df2("IsActiveMember")===1) &&(df2("CreditScore")>700)).count()

// COMMAND ----------

// DBTITLE 1,credit card users in Spain 
 
df2.filter(df2("Geography")==="Spain").show()

// COMMAND ----------

df2.filter((df2("EstimatedSalary")>100000) &&(df2("Exited")==1)).count()

// COMMAND ----------

df2.filter((df2("EstimatedSalary")<100000) &&(df2("NumOfProducts")>1)).count()

// COMMAND ----------

// DBTITLE 1,TRANSACTION DATASET
val df3 = spark.read.option("header",true).schema(simpleSchema).format("csv").load("dbfs:/FileStore/shared_uploads/akshitnand@gmail.com/txn-1.csv")

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType,DoubleType,StringType,StructType,StructField}
val simpleSchema = StructType(Array(
    StructField("Account No",StringType,true),
    StructField("TRANSACTION DETAILS",StringType,true),
    StructField("VALUE DATE",StringType,true),
    StructField("WITHDRAWAL AMT", DoubleType, true),
    StructField("DEPOSIT AMT", DoubleType, true),
    StructField("BALANCE AMT", DoubleType, true)
  ))

// COMMAND ----------

df3.printSchema()

// COMMAND ----------

// DBTITLE 1,COUNT OF TRANSACTION ON EVERY ACCOUNT

df3.groupBy("Account No").count().show()

// COMMAND ----------

// DBTITLE 1,MINIMUM WITHDRAWAL AMOUNT OF AN ACCOUNT

df3.groupBy("Account No").min("WITHDRAWAL AMT").orderBy("min(WITHDRAWAL AMT)").show()

// COMMAND ----------

// DBTITLE 1,Maximum withdrawal amount

df3.groupBy("Account No").max("DEPOSIT AMT").orderBy($"max(DEPOSIT AMT)").show()
// orderBy($"count".desc).show()

// COMMAND ----------

// DBTITLE 1,MINIMUM DEPOSIT AMOUNT OF AN ACCOUNT

df3.groupBy("Account No").min("DEPOSIT AMT").orderBy("min(DEPOSIT AMT)").show()

// COMMAND ----------

// DBTITLE 1,sum of balance in every bank account

df3.groupBy("Account No").sum("BALANCE AMT").show()

// COMMAND ----------

// DBTITLE 1,Number of transaction on each date

df3.groupBy("VALUE DATE").count().orderBy($"count".desc).show()

// COMMAND ----------

// DBTITLE 1,List of customers with withdrawal amount more than 1 lakh

df3.select("Account No","TRANSACTION DETAILS"," WITHDRAWAL AMT ").filter(df3(" WITHDRAWAL AMT ")>100000).show()
