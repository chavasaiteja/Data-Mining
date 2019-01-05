import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql._

object Task1 {
  def main(args: Array[String]) {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Word Count")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    // Load our input data.
    var df = spark.read.format("csv").option("header", "true").load(args(0))

    df = df.select("Country", "Salary", "SalaryType")

    /*
    println()
    println("***** Schema  ******")
    print(df.printSchema())
    printf("Count = %d", df.count())
    println()
    */

    val df2 = df.filter($"Salary" !== "NA").filter($"Salary" !== "0")
    val df3 = df2.select("Country")

    /*
    println()
    println()
    printf("Count = %d", df2.count())

    println()
    println()
    print(df3.printSchema())
    */

    //val x = df3.take(5)
    //print(x(0))

    val df4 = df3.rdd
    val df5 = df4.map(x => (x.getString(0), 1))

    val df6 = df5.reduceByKey((x, y) => x + y)

    val z = df6.take(5)
    val df7 = df6.sortByKey()

    val df8 = df7.collect()


    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(1))))
    val csvWriter = new CSVWriter(writer)

    csvWriter.writeNext(Array("Total",df2.count().toString))

    for(line <- df8){
      //println(line._1)
      csvWriter.writeNext(Array(line._1.toString,line._2.toString))
    }
    //df7.toDF().repartition(1).write.format("csv").save(args(1))
    csvWriter.close()

  }
}