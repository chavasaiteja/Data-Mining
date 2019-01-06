import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import au.com.bytecode.opencsv.CSVWriter
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._



object Task2 {
  def main(args: Array[String]) {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Word Count")
      //.config("spark.master", "local")
      .master("local")
      .config("spark.sql.shuffle.partition",2)
      .getOrCreate()

    //val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._

    // Load our input data.
    var df = spark.read.format("csv").option("header", "true").load(args(0))

    df = df.select("Country", "Salary", "SalaryType")


    val df2 = df.filter($"Salary" !== "NA").filter($"Salary" !== "0")
    val df3 = df2.select("Country")

    val df4 = df3.rdd
    var df11 = df4.repartition(2)
    var df5 = df4.map(x => (x.getString(0), 1))

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(1))))
    val csvWriter = new CSVWriter(writer)


    var par1 = df5.toDF().rdd.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.toDF("partition_number","number_of_records")
  
    var start_without_partition = System.currentTimeMillis()
    var df6 = df5.reduceByKey((x, y) => x + y)

    var df7 = df6.sortByKey()

    var end_without_partition = System.currentTimeMillis()

    var s3 = par1.toDF()
    var k2 = par1.select("number_of_records").rdd.map(r => r(0)).collect()
    csvWriter.writeNext(Array("standard", k2(0).toString, k2(1).toString, (end_without_partition-start_without_partition).toString))
    
    var df12 = df11.map(x => (x.getString(0), 1))

    var par2 = df11.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.toDF("partition_number","number_of_records")
    var start_with_partition = System.currentTimeMillis()


    var df13 = df12.reduceByKey((x, y) => x + y)
    val df14 = df13.sortByKey()
    var end_with_partition = System.currentTimeMillis()
    
    var s4 = par2.toDF()
    var k4 = par2.select("number_of_records").rdd.map(r => r(0)).collect()
    csvWriter.writeNext(Array("partition", k4(0).toString, k4(1).toString, (end_with_partition-start_with_partition).toString))
    csvWriter.close()

  }
}