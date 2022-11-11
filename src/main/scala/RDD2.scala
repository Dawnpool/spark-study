import org.apache.spark.{SparkConf, SparkContext}

object RDD2 extends App {
  val conf = new SparkConf().setAppName("test_app").setMaster("local")
  val sc = new SparkContext(conf)

  // CSV 파일로부터 RDD 생성
  val rddFromFile = sc.textFile("samples/SalesJan2009.csv")
    .mapPartitionsWithIndex({
      (idx, iter) => if (idx == 0) iter.drop(1) else iter // Skip header
  })

  // RDD 정보 파싱하여 새 RDD 생성
  val rdd = rddFromFile.map(record => {
    val splitRecord = record.split(",")
    val product = splitRecord(1)
    val price = splitRecord(2)
    val paymentType = splitRecord(3)
    val name = splitRecord(4)
    val country = splitRecord(7)
    (product, price, paymentType, name, country)
  })
  rdd.take(10).foreach(println)

  // RDD 요소를 (product, 1)로 가공
  val productAndOnePair = rdd.map(record => (record._1, 1))
  productAndOnePair.take(10).foreach(println)

  // RDD 요소 키 단위로 집약처리 및 정렬
  val countByProductRDD = productAndOnePair.reduceByKey((result, elem) => result + elem).sortByKey()
  countByProductRDD.foreach(println)
}
