import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameSQL extends App {
  val spark = SparkSession.builder().appName("test_app").master("local").getOrCreate()

  // dataFrame으로 CSV 파일 load
  val df = spark.read
    .format("csv")
    .option("header", "true") // CSV의 헤더 인식
    .option("inferSchema", "true") // 각 컬럼의 타입 인식
    .load("samples/SalesJan2009.csv")

  df.createOrReplaceTempView("sales")
  spark.sql("DESCRIBE sales").show()

  // SELECT *
  spark.sql("SELECT * FROM sales").show(10)

  // WHERE Price >= 2000
  spark.sql("SELECT * FROM sales WHERE Price >= 2000").show(10)

  // order by price desc
  spark.sql("SELECT * FROM sales ORDER BY Price DESC").show(10)

  // average price group by product
  spark.sql("SELECT Product, avg(Price) FROM sales GROUP BY Product").show()
}
