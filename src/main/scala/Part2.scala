import org.apache.spark.SparkContext


object HelloWorldSpark extends App {

  val sc = new SparkContext("local", "HelloWorld" )
  val stopWordsInput = sc.textFile("src/files/stopwords.txt").flatMap(line => line.split("\\W+")).map(_.trim)
  val broadcastStopWords = sc.broadcast(stopWordsInput.collect.toSet)
  val sourceRDD = sc.textFile("src/files/all-shakespeare.txt")

  val splitData = sourceRDD
    .map(_.toLowerCase)
    .flatMap(line => line.split("\\W+"))
    .filter(!broadcastStopWords.value.contains(_)).filter(!" ".contains(_))
    .map((_,1))
    .reduceByKey(_+_)
    .sortBy(_._2, ascending = false)
    .take(10)

  //Сделал вывод в формате:  слово, кол-во повторений. Затруднения для вывода в формате: число раз: список слов
  println("class: " + splitData.mkString("Array(", ", ", ")"))
}
