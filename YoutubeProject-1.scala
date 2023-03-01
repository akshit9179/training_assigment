// Databricks notebook source
// DBTITLE 1,Importing the packages

import org.apache.spark.sql
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{col,desc,coalesce}
import org.apache.spark.sql.functions.{when}


// COMMAND ----------

val df1 = spark.read.option("header",true).option("inferSchema", "true").format("csv").load("dbfs:/FileStore/shared_uploads/akshitnand@gmail.com/LoanStats_2018Q4.csv")

// COMMAND ----------

// DBTITLE 1,Displaying the dataframe
//var df1 = importBigDataFile(path1,format)
display(df1)

// COMMAND ----------

// DBTITLE 1,Total no of rows
df1.count()

// COMMAND ----------

// DBTITLE 1,Schema of the dataframe
df1.printSchema()

// COMMAND ----------

// DBTITLE 1,Create view of schema
df1.createOrReplaceTempView(temp_table)

// COMMAND ----------

// DBTITLE 1,Diplay complete data
var show = spark.sql("select * from loanstats")
display(show)

// COMMAND ----------

// DBTITLE 1,Count the total rows
var count = spark.sql("select count(*) from loanstats")
display(count)

// COMMAND ----------

// DBTITLE 1,Describe the table
val desc = df1.describe()
display(desc)

// COMMAND ----------

// DBTITLE 1,Describe the column 
val df1_sel = df1.describe("loan_status","pymnt_plan","zip_code","earliest_cr_line","open_acc","revol_bal")
display(df1_sel)

// COMMAND ----------

// DBTITLE 1,Show the column
val df1_sel = df1.select("loan_status","pymnt_plan","zip_code","earliest_cr_line","open_acc","revol_bal")
df1_sel.show()

// COMMAND ----------

// DBTITLE 1,Dataframe cache
df1_sel.cache()

// COMMAND ----------

// DBTITLE 1,show only distinct emp length
var df1_dis = spark.sql("select distinct emp_length from loanstats limit 50")
display(df1_dis)

// COMMAND ----------

// DBTITLE 1,Get integer only from dataframe
val myColumnNumericDF1 = df1.select(regexp_extract(col("emp_length"), "\\d+", 0).alias("emp_length_clean"),col("emp_length"))
myColumnNumericDF1.show(10)

// COMMAND ----------

val def1_sel = show
  .withColumn("term_cleaned", regexp_replace(col("term"), "months", ""))
  .withColumn("emplen_cleaned", regexp_extract(col("emp_length"), "\\d+", 0))
display(def1_sel)

// COMMAND ----------

// DBTITLE 1,Show only formatted columns
def1_sel.select("term","term_cleaned","emp_length","emplen_cleaned").show(15)

// COMMAND ----------

// DBTITLE 1,Schema of dataframe
show.printSchema()

// COMMAND ----------

// DBTITLE 1,Create a view
def1_sel.createOrReplaceTempView(table_name)

// COMMAND ----------

// DBTITLE 1,Use SQL statement in view
var sql_show = spark.sql("select * from loanstatus_sel")
display(sql_show)

// COMMAND ----------

// DBTITLE 1,Correlation of term_cleaned and loan_amnt
var get = spark.sql("select corr(loan_amnt,term_cleaned) from loanstatus_sel")
get.show()

// COMMAND ----------

// DBTITLE 1,contingency of loan_status and grade
sql_show.stat.crosstab("loan_status","grade").show()

// COMMAND ----------

// DBTITLE 1,Frequency of purpose and grade
val freq = sql_show.stat.freqItems(Array("purpose", "grade"), 0.3)
freq.collect()

// COMMAND ----------

// DBTITLE 1,Output purpose by group 
sql_show.groupBy(col("purpose")).count.show()

// COMMAND ----------

// DBTITLE 1,Show loan status by desc
var status = spark.sql("select loan_status,count(*) from loanstats group by loan_status order by 2 desc")
status.show()

// COMMAND ----------

// DBTITLE 1,Total status
status.count()

// COMMAND ----------

// DBTITLE 1,Decribe dti and revol_util
def1_sel.describe("dti","revol_util").show()

// COMMAND ----------

// DBTITLE 1,Cleaning the data
val result = spark.sql("SELECT CEIL(REGEXP_REPLACE(revol_util, '%', '')), COUNT(*) FROM loanstatus_sel GROUP BY CEIL(REGEXP_REPLACE(revol_util, '%', ''))")
result.show()

// COMMAND ----------

// DBTITLE 1,Output where revol_util is null
val nullData = spark.sql("SELECT * FROM loanstatus_sel WHERE revol_util IS NULL")
display(nullData)


// COMMAND ----------

// DBTITLE 1,output where dti is null
val nullData = spark.sql("SELECT * FROM loanstatus_sel WHERE dti IS NULL")
display(nullData)

// COMMAND ----------

val nullData = spark.sql("SELECT application_type,dti,dti_joint FROM loanstatus_sel WHERE dti IS NULL")
nullData.show()

// COMMAND ----------

// DBTITLE 1,coalesce of two columns
var df1_sel = def1_sel.withColumn("dti_cleaned",coalesce(col("dti"),col("dti_joint")))
display(df1_sel)

// COMMAND ----------

// DBTITLE 1,group by loan status
def1_sel.groupBy("loan_status").count().show()

// COMMAND ----------

// DBTITLE 1,Show only selected loan status
display(def1_sel.where(col("loan_status").isin("Late (31-120 days)", "Charged Off", "In Grace Period", "Late (16-30 days)")))


// COMMAND ----------

// DBTITLE 1,Condition for loan status
var sel =def1_sel
  .withColumn("bad_loan", when(col("loan_status").isin("Late (31-120 days)", "Charged Off", "In Grace Period", "Late (16-30 days)"), "yes").otherwise("no"))
display(sel)

// COMMAND ----------

// DBTITLE 1,Total no of bad loans
sel.groupBy("bad_loan").count().show()

// COMMAND ----------

// DBTITLE 1,show only bad loans
display(sel.filter(col("bad_loan") === "yes"))

// COMMAND ----------

// DBTITLE 1,Schema of dataframe
sel.printSchema()

// COMMAND ----------

// DBTITLE 1,Drop multiple columns
var df1_srt_final = sel.drop("revol_util","dti","dti_joint")

// COMMAND ----------

// DBTITLE 1,Schema of dropped table
df1_srt_final.printSchema()

// COMMAND ----------

// DBTITLE 1,Stats of bad loan and grade
sel.stat.crosstab("bad_loan","grade").show()

// COMMAND ----------

// DBTITLE 1,save the dataframe as table
sel.write.format("parquet").saveAsTable(par_name)

// COMMAND ----------

// DBTITLE 1,Use SQL query in dataframe
var data = spark.sql("select * from lc_loan_data1")
display(data)
