import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object RDDs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sesion2").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val PaRDD = sc.parallelize(data)
    PaRDD.collect().foreach(println)
    println(PaRDD.reduce((a, b) => a*b))
    println(PaRDD.reduce(_*_))

//    open folder and write it to folder
    val file = sc.textFile("./src/main/scala/test.txt")
    val length = file.map(s=> s.length)
    length.collect().foreach(println)

    val whole_files = sc.wholeTextFiles("./src/main/scala/datasets")
    whole_files.collect().foreach(println)


  }
}
