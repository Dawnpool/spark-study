import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame extends App {
  val spark = SparkSession.builder().appName("test_app").master("local").getOrCreate()

  // dataFrame으로 CSV 파일 load
  val df = spark.read
    .format("csv")
    .option("header", "true") // CSV의 헤더 인식
    .option("inferSchema", "true") // 각 컬럼의 타입 인식
    .load("samples/SalesJan2009.csv")
  df.printSchema()

  // SELECT ALL
  val selectAllDF = df.select("*")
  selectAllDF.show(10)

  // WHERE Price >= 2000
  val priceOver2000DF = df.where("Price >= 2000")
  priceOver2000DF.show(10)

  // order by price desc
  val priceDescOrderDF = df.orderBy(col("Price").desc)
  priceDescOrderDF.show(10)

  // average price group by product
  val groupByProductDF = df.groupBy("Product").avg("Price")
  groupByProductDF.show()
}
