// Databricks notebook source
// DBTITLE 1,Write a Scala program to print "Hello, world" and version of the Scala language
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    println("Scala language: "+util.Properties.versionString)
  }
}

// COMMAND ----------

HelloWorld.main(Array(""))

// COMMAND ----------

// DBTITLE 1, Write a Scala program to compute the sum of the two given integer values. If the two values are the same, then return triples their sum
def sumOrTriple(x: Int, y: Int): Int = {
  if (x == y) 3 * (x + y)
  else x + y
}

val a = 5
val b = 10

val result = sumOrTriple(a, b)

println("The result is: " + result)


// COMMAND ----------

// DBTITLE 1,Write a Scala program to get the absolute difference between n and 51. If n is greater than 51 return triple the absolute difference.
def absDiff(n: Int): Int = {
  val diff = Math.abs(n - 51)
  if (n > 51) 3 * diff
  else diff
}

val num = 65

val result = absDiff(num)

println("The result is: " + result)


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check two given integers, and return true if one of them is 30 or if their sum is 30.
def checkThirty(a: Int, b: Int): Boolean = {
  (a == 30 || b == 30 || a + b == 30)
}

val num1 = 20
val num2 = 10

val result = checkThirty(num1, num2)

println("The result is: " + result)


// COMMAND ----------

// DBTITLE 1, Write a Scala program to check a given integer and return true if it is within 20 of 100 or 300.
def checkWithinRange(n: Int): Boolean = {
  Math.abs(100 - n) <= 20 || Math.abs(300 - n) <= 20
}

val num = 110

val result = checkWithinRange(num)

println("The result is: " + result)


// COMMAND ----------

// DBTITLE 1,Write a Scala program to create a new string where 'if' is added to the front of a given string. If the string already begins with 'if', return the string unchanged.
def addIf(s: String): String = {
  if (s.startsWith("if")) s
  else "if" + s
}

val str1 = "else"
val str2 = "if not"

val result1 = addIf(str1)
val result2 = addIf(str2)

println("The result for string 1 is: " + result1)
println("The result for string 2 is: " + result2)


// COMMAND ----------

// DBTITLE 1,Write a Scala program to remove the character in a given position of a given string. The given position will be in the range 0...string length -1 inclusive.
object RemoveChar {
  def main(args: Array[String]): Unit = {
    val str = "example"
    val pos = 2
    
    val result = removeCharAt(str, pos)
    
    println(s"The original string is: $str")
    println(s"The character at position $pos is removed, resulting in: $result")
  }
  
  def removeCharAt(str: String, pos: Int): String = {
    str.take(pos) + str.drop(pos + 1)
  }
}


// COMMAND ----------

RemoveChar.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to exchange the first and last characters in a given string and return the new string
object ExchangeFirstLast {
  def main(args: Array[String]): Unit = {
    val str = "example"
    
    val result = exchangeFirstLast(str)
    
    println(s"The original string is: $str")
    println(s"The first and last characters are exchanged, resulting in: $result")
  }
  
  def exchangeFirstLast(str: String): String = {
    if (str.length < 2) str
    else str.last + str.tail.dropRight(1) + str.head
  }
}


// COMMAND ----------

ExchangeFirstLast.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to create a new string which is 4 copies of the 2 front characters of a given string.If the given string length is less than 2 return the original string. 
object FourCopies {
  def main(args: Array[String]): Unit = {
    val str1 = "example"
    val str2 = "hi"
    
    val result1 = fourCopies(str1)
    val result2 = fourCopies(str2)
    
    println(s"The original string is: $str1")
    println(s"Four copies of the first 2 characters are: $result1")
    println(s"The original string is: $str2")
    println(s"Four copies of the first 2 characters are: $result2")
  }
  
  def fourCopies(str: String): String = {
    if (str.length < 2) str
    else str.take(2) * 4
  }
}


// COMMAND ----------

FourCopies.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to create a new string with the last char added at the front and back of a given string of length 1 or more.
object AddLastChar {
  def main(args: Array[String]): Unit = {
    val str1 = "example"
    val str2 = "h"
    
    val result1 = addLastChar(str1)
    val result2 = addLastChar(str2)
    
    println(s"The original string is: $str1")
    println(s"The last character added at the front and back is: $result1")
    println(s"The original string is: $str2")
    println(s"The last character added at the front and back is: $result2")
  }
  
  def addLastChar(str: String): String = {
    if (str.length < 1) str
    else str.last + str + str.last
  }
}


// COMMAND ----------

AddLastChar.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether a given positive number is a multiple of 3 or a multiple of 7.
object Multiples {
  def main(args: Array[String]): Unit = {
    val num1 = 21
    val num2 = 10
    
    val result1 = checkMultiples(num1)
    val result2 = checkMultiples(num2)
    
    println(s"The number $num1 is a multiple of 3 or 7: $result1")
    println(s"The number $num2 is a multiple of 3 or 7: $result2")
  }
  
  def checkMultiples(num: Int): Boolean = {
    num % 3 == 0 || num % 7 == 0
  }
}


// COMMAND ----------

Multiples.main(Array(""))

// COMMAND ----------

// DBTITLE 1, Write a Scala program to create a new string taking the first 3 characters of a given string and return the string with the 3 characters added at both the front and back. If the given string length is less than 3, use whatever characters are there.
object AddThreeChars {
  def main(args: Array[String]): Unit = {
    val str1 = "example"
    val str2 = "hi"
    val str3 = "a"
    
    val result1 = addThreeChars(str1)
    val result2 = addThreeChars(str2)
    val result3 = addThreeChars(str3)
    
    println(s"The original string is: $str1")
    println(s"The new string with the first 3 characters added at both the front and back is: $result1")
    println(s"The original string is: $str2")
    println(s"The new string with the first 3 characters added at both the front and back is: $result2")
    println(s"The original string is: $str3")
    println(s"The new string with the first 3 characters added at both the front and back is: $result3")
  }
  
  def addThreeChars(str: String): String = {
    if (str.length < 3) str + str + str
    else str.substring(0, 3) + str.substring(0, 3) + str.substring(0, 3)
  }
}


// COMMAND ----------

AddThreeChars.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether a given string starts with 'Sc' or not.
object CheckPrefix {
  def main(args: Array[String]): Unit = {
    val str1 = "Scala programming language"
    val str2 = "Programming in Java"
    
    val result1 = checkPrefix(str1)
    val result2 = checkPrefix(str2)
    
    println(s"The string '$str1' starts with 'Sc': $result1")
    println(s"The string '$str2' starts with 'Sc': $result2")
  }
  
  def checkPrefix(str: String): Boolean = {
    str.startsWith("Sc")
  }
}


// COMMAND ----------

CheckPrefix.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether one of the given temperatures is less than 0 and the other is greater than 100.
object CheckTemperature {
  def main(args: Array[String]): Unit = {
    val temp1 = -5
    val temp2 = 105
    val temp3 = 50
    val temp4 = 90
    
    val result1 = checkTemperature(temp1, temp2)
    val result2 = checkTemperature(temp1, temp3)
    val result3 = checkTemperature(temp3, temp4)
    
    println(s"Temperature 1: $temp1, Temperature 2: $temp2, Result: $result1")
    println(s"Temperature 1: $temp1, Temperature 2: $temp3, Result: $result2")
    println(s"Temperature 1: $temp3, Temperature 2: $temp4, Result: $result3")
  }
  
  def checkTemperature(temp1: Int, temp2: Int): Boolean = {
    (temp1 < 0 && temp2 > 100) || (temp1 > 100 && temp2 < 0)
  }
}


// COMMAND ----------

CheckTemperature.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to check two given integers whether either of them is in the range 100..200 inclusive.
object CheckRange {
  def main(args: Array[String]): Unit = {
    val num1 = 150
    val num2 = 250
    val num3 = 50
    val num4 = 200
    
    val result1 = checkRange(num1, num2)
    val result2 = checkRange(num1, num3)
    val result3 = checkRange(num3, num4)
    
    println(s"Number 1: $num1, Number 2: $num2, Result: $result1")
    println(s"Number 1: $num1, Number 2: $num3, Result: $result2")
    println(s"Number 1: $num3, Number 2: $num4, Result: $result3")
  }
  
  def checkRange(num1: Int, num2: Int): Boolean = {
    (num1 >= 100 && num1 <= 200) || (num2 >= 100 && num2 <= 200)
  }
}


// COMMAND ----------

CheckRange.main(Array(""))

// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether three given integer values are in the range 20..50 inclusive. Return true if 1 or more of them are in the said range otherwise false.
def checkInRange(num1: Int, num2: Int, num3: Int): Boolean = {
  val range = 20 to 50
  range.contains(num1) || range.contains(num2) || range.contains(num3)
}


println(checkInRange(15, 25, 35)) 
println(checkInRange(10, 15, 18)) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether two given integer values are in the range 20..50 inclusive. Return true if 1 or other is in the said range otherwise false.
def checkInRange(num1: Int, num2: Int): Boolean = {
  val range = 20 to 50
  range.contains(num1) || range.contains(num2)
}


println(checkInRange(15, 25)) 
println(checkInRange(10, 15)) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether a string 'yt' appears at index 1 in a given string. If it appears return a string without 'yt' otherwise return the original string.
def checkAndRemoveYT(str: String): String = {
  if (str.length() > 2 && str.substring(1, 3) == "yt") {
    str.substring(0, 1) + str.substring(3)
  } else {
    str
  }
}


println(checkAndRemoveYT("mythology")) 
println(checkAndRemoveYT("python")) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check the largest number among three given integers.
def findLargest(num1: Int, num2: Int, num3: Int): Int = {
  Math.max(Math.max(num1, num2), num3)
}


println(findLargest(10, 20, 30)) 
println(findLargest(5, 15, 5)) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check which number is nearest to the value 100 among two given integers. Return 0 if the two numbers are equal.
def findNearestTo100(num1: Int, num2: Int): Int = {
  val diff1 = Math.abs(num1 - 100)
  val diff2 = Math.abs(num2 - 100)

  if (diff1 < diff2) {
    num1
  } else if (diff2 < diff1) {
    num2
  } else {
    0
  }
}


println(findNearestTo100(90, 110)) 
println(findNearestTo100(90, 95))
println(findNearestTo100(100, 100)) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether two given integers are in the range 40..50 inclusive, or they are both in the range 50..60 inclusive.
def checkRange(num1: Int, num2: Int): Boolean = {
  val inRange1 = num1 >= 40 && num1 <= 50
  val inRange2 = num2 >= 40 && num2 <= 50
  val inRange3 = num1 >= 50 && num1 <= 60
  val inRange4 = num2 >= 50 && num2 <= 60

  (inRange1 && inRange2) || (inRange3 && inRange4)
}


println(checkRange(45, 50)) 
println(checkRange(55, 58)) 
println(checkRange(30, 55)) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to find the larger value from two positive integer values in the range 20..30 inclusive, or return 0 if neither is in that range.
def findLargerInRange(num1: Int, num2: Int): Int = {
  val inRange1 = num1 >= 20 && num1 <= 30
  val inRange2 = num2 >= 20 && num2 <= 30

  if (inRange1 && inRange2) {
    Math.max(num1, num2)
  } else {
    0
  }
}


println(findLargerInRange(25, 28)) 
println(findLargerInRange(18, 25)) 
println(findLargerInRange(30, 35)) 


// COMMAND ----------

// DBTITLE 1, Write a Scala program to check whether a given character presents in a string between 2 to 4 times.
def checkCharFrequency(str: String, ch: Char): Boolean = {
  val count = str.count(_ == ch)
  count >= 2 && count <= 4
}


println(checkCharFrequency("abacabadabacaba", 'a')) 
println(checkCharFrequency("Hello World", 'o'))
println(checkCharFrequency("abcbcb", 'b')) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to check whether two given positive integers have the same last digit.
def sameLastDigit(num1: Int, num2: Int): Boolean = {
  num1 % 10 == num2 % 10
}


println(sameLastDigit(23, 53)) 
println(sameLastDigit(101, 201)) 
println(sameLastDigit(12, 45))


// COMMAND ----------

// DBTITLE 1,Write a Scala program to convert the last 4 characters of a given string in upper case. If the length of the string has less than 4 then uppercase all the characters.
def convertLastFourToUpper(str: String): String = {
  val length = str.length
  if (length < 4) {
    str.toUpperCase()
  } else {
    str.substring(0, length - 4) + str.substring(length - 4).toUpperCase()
  }
}


println(convertLastFourToUpper("hello")) 
println(convertLastFourToUpper("world!")) 
println(convertLastFourToUpper("scala is fun")) 


// COMMAND ----------

// DBTITLE 1,Write a Scala program to create a new string which is n (non-negative integer ) copies of a given string.
def copyString(str: String, n: Int): String = {
  if (n <= 0) {
    ""
  } else {
    str * n
  }
}


println(copyString("hello", 3)) 
println(copyString("world", 0))  
println(copyString("scala", 5)) 

