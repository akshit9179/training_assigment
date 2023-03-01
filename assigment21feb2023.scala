// Databricks notebook source
// DBTITLE 1,program to check whether a number is Prime or not
// MAGIC %scala
// MAGIC def isPrime2(i :Int) : Boolean = {
// MAGIC      if (i <= 1)
// MAGIC        false
// MAGIC      else if (i == 2)
// MAGIC        true
// MAGIC      else
// MAGIC        !(2 to (i-1)).exists(x => i % x == 0)
// MAGIC   }

// COMMAND ----------

// MAGIC %scala
// MAGIC isPrime2(69)

// COMMAND ----------

// DBTITLE 1,Program for n-th Fibonacci number
def fib1(n: Long): Long = n match {
  case 0 | 1 => n
  case _ => fib1(n - 1) + fib1(n - 2)
}

// COMMAND ----------

fib1(9)

// COMMAND ----------

// DBTITLE 1,Program for How to check if a given number is Fibonacci number?
def check(w:Int)
{
var a=0
var b=1
if (w==a || w==b)
{
println("true")
}
var c=a+b
while (c<=w)
  {
	if (c==w)
	{
		println("true")
	}
	a=b
	b=c
	c=a+b
  }
  
}

// COMMAND ----------

check(9)

// COMMAND ----------

// DBTITLE 1,Program to print ASCII Value of a character

    def main(ch:Char){  
        
        
        println("Character value: "+ch)
      println("ASCII value    : "+ch.toInt)
    }

// COMMAND ----------

main('A')

// COMMAND ----------

// DBTITLE 1,Program for Sum of squares of first n natural numbers
def number(n:Int)
{
 var sum=0
  var w=0
for (w<- 1 to n)
{
sum+=(w*w)
}
println(sum)
}

// COMMAND ----------

number(7)

// COMMAND ----------

// DBTITLE 1,Program for cube sum of first n natural numbers
def number(n:Int)
{
 var sum=0
  var w=0
for (w<- 1 to n)
{
sum+=(w*w*w)
}
println(sum)
}

// COMMAND ----------

number(3)
