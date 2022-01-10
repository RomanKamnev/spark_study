import org.apache.spark.SparkContext

object Part1 extends App  {
  val sc = new SparkContext("local", "HelloWorld" )

  // aggregateByKey
  val pairs1 = sc.parallelize(Seq(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
  val resAgg1 = pairs1.aggregateByKey(0)(_+_,_+_)
  resAgg1.collect
  println("AggregateByKey: " + resAgg1.take(10).mkString("Array(", ", ", ")"))

  // reduceByKey
  val pairs2 = sc.parallelize(Seq(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
  val resAgg2 = pairs2.reduceByKey ( _+_ )
  resAgg2.collect
  println("ReduceByKey: " + resAgg2.take(10).mkString("Array(", ", ", ")"))

  // combineByKey
  //Не могу разобраться с синтаксисом
  val pairs3 = sc.parallelize(Seq(("a", 3), ("a", 1), ("b", 7), ("a", 5)))

  val createCombiner = (v:Int) => List(v)
  val mergeValue = (a:List[Int], b:Int) => a.::(b)
  val mergeCombiners = (a:List[Int],b:List[Int]) => a.++(b)

  pairs3.combineByKey(createCombiner,mergeValue, mergeCombiners).collect
}
