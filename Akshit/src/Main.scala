import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Table to dataframes")
      .master("local[*]")
      .getOrCreate()

    val mysql_db_driver_class = "com.mysql.jdbc.Driver"
    val table_name = "corona_dataset_latest"
    val host_name = "localhost"
    val port_no = "3306"
    val user_name = "root"
    val password = "root"
    val database_name = "kpi_traning"

    val mysql_Select_query = "(select * from " + table_name + ") as users"
    println("printing mysql_select_query: ")
    println(mysql_Select_query)

    val mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name
    print("Printing JDBC Url: " + mysql_jdbc_url)

    val corona_data_df = spark.read.format("jdbc")
      .option("url", mysql_jdbc_url)
      .option("driver", mysql_db_driver_class)
      .option("dbtable", mysql_Select_query)
      .option("user", user_name)
      .option("password", password)
      .load()
    corona_data_df.show(10, false)
  }
}