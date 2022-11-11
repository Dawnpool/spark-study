import org.apache.spark.{SparkConf, SparkContext}

object RDD extends App {
  val conf = new SparkConf().setAppName("test_app").setMaster("local")
  val sc = new SparkContext(conf)

  // 기본 RDD 생성
  val textRDD = sc.textFile("samples/words.txt")
  val textArray = textRDD.collect() // 배열로 변환
  println("textArray 리스트:")
  textArray.foreach(println)

  // RDD 필터링
  val wordRDD = textRDD.filter(word => word.matches("""\p{Alnum}+"""))
//  val wordRDD = textRDD.filter(_.matches("""\p{Alnum}+""")) // 플레이서 홀더도 사용 가능
  val wordArray = wordRDD.collect()
  println("wordArray 리스트:")
  wordArray.foreach(println)

  // RDD 요소를 (단어, 1)과 같은 형태로 가공
  val wordAndOnePairRDD = wordRDD.map(word => (word, 1))
  val wordAndOnePairRDDArray = wordAndOnePairRDD.collect()
  wordAndOnePairRDDArray.foreach(println)

  // RDD 요소를 키 단위로 집약처리 (groupBy)
  val wordAndCountRDD = wordAndOnePairRDD.reduceByKey((result, elem) => result + elem)
  val wordAndCountArray = wordAndCountRDD.collect()
  wordAndCountArray.foreach(println)
}
