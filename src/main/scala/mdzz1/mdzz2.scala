package mdzz1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


case class Person(name: String, age: Long)
object mdzz2 {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

//    val df = sqlContext.read.json("C:/spark/spark-1.3.0-bin-hadoop2.4/examples/src/main/resources/people.json")


    //    df.show()
    //    df.printSchema()
    //    df.select("name").show()
    //    df.select(df("name"), df("age") + 1).show()
    //    df.filter(df("age") > 21).show()
    //    df.groupBy("age").count().show()

//    val ds = Seq(1, 2, 3).toDS()
//    ds.map(_ + 1).collect().foreach(println)

//    val dt = Seq(Persons("Andy", 32)).toDS()
//    dt.collect().foreach(println)
//    println(dt.collect().length)

//    val path = "C:/Users/northgrass/Desktop/1.json"
//    val people = sqlContext.read.json(path).as[Persons]
//    people.collect().foreach(println)

//    val path = "examples/src/main/resources/people.json"
//    val people = sqlContext.read.json(path).as[Person]
val people = sc.textFile("C:/spark/spark-1.3.0-bin-hadoop2.4/examples/src/main/resources/people.txt")
    val schemaString = "name age"

    import org.apache.spark.sql.Row;

    import org.apache.spark.sql.types.{StructType,StructField,StringType};

    val schema =
    StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    peopleDataFrame.registerTempTable("people")

    val results = sqlContext.sql("SELECT name FROM people")

    results.map(t => "Name: " + t(0)).collect().foreach(println)
//  .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//    people.registerTempTable("people")
//
//    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
//    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }
}
