package mdzz1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object mdzz1 {
  case class Person(name: String, age: Long)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //    val data = sc.parallelize(List((0, 2), (0, 4), (1, 0), (1, 10), (1, 20)))
    //    data.map(r => (r._1, (r._2, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(r => (r._1, (r._2._1 / r._2._2))).foreach(x => println(x))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._


    val people = sc.textFile("C:/spark/spark-1.3.0-bin-hadoop2.4/examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()

    people.registerTempTable("people")
    people.show()
    people.printSchema()
//    people.col("name")
//    people.first()


    people.select("name").show()



    val teenagers = sqlContext.sql("SELECT name,age FROM people WhERE age >= 13 AND age <= 19")
    teenagers.show()
//    teenagers.map(t => "Name: " + t(0)).collect().foreach(x => println(x))

  }
}
