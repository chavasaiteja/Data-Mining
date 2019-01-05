import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.round

import scala.math.BigDecimal


object Task3 {
  def main(args: Array[String]) {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("Word Count")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // Load our input data.
    var df = spark.read.format("csv").option("header", "true").load(args(0)).toDF()

    df = df.select("Country","Salary" ,"SalaryType")

    val replace = udf((data : String)=>data.replaceAll(",",""))
    var df2 = df.withColumn("Salary",replace($"Salary"))

    /*
    println()
    println("***** Schema  ******")
    print(df.printSchema())
    printf("Count = %d",df.count())
    println()
    */

    df2 = df2.filter($"Salary"!=="NA").filter($"Salary"!=="0")

    val distinctvaluesdf = df.select("SalaryType").distinct()

    //println()

    //println("Distinct values of the SalaryType Column are as follows")
    //distinctvaluesdf.collect.foreach(println)

    //println()

    val aftermonthDF = df2.withColumn("SalaryNew", when($"SalaryType" === "Monthly", $"Salary"*12).otherwise($"Salary"))
    var afterweekDF = aftermonthDF.withColumn("SalaryFinal",when($"SalaryType" === "Weekly",$"Salary"*52).otherwise($"SalaryNew"))
    val b = afterweekDF.take(20)
    //println()

    //afterweekDF.limit(70).show(70,false)
    //afterweekDF.repartition(1).write.format("csv").save("/Users/sai/Downloads/spark-2.3.1-bin-hadoop2.7/bin/check.csv")
    afterweekDF = afterweekDF.select("Country","SalaryFinal")
    //afterweekDF.limit(70).show(70,false)
    /*
    println()
    println("***** Schema  ******")
    print(afterweekDF.printSchema())
    printf("Count = %d",df.count())
    println()
    */

    var count = afterweekDF.groupBy("Country").count().orderBy($"Country".asc)

    //count.repartition(1).write.format("csv").save("/Users/sai/Downloads/spark-2.3.1-bin-hadoop2.7/bin/count.csv")

    val avg = afterweekDF.groupBy("Country").agg(mean("SalaryFinal")).orderBy($"Country".asc)

    var avg1 = avg.withColumn("Avg", round($"avg(SalaryFinal)", 2))
    var avg2 = avg1.select("Country","Avg")

    afterweekDF = afterweekDF.withColumn("Salary", 'SalaryFinal.cast("Double")).select( 'Country,'Salary as 'SalaryFinal)
    var ma1 = afterweekDF.groupBy("Country").agg(collect_list("SalaryFinal")).orderBy($"Country").toDF().rdd.collect()

    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(1))))
    val csvWriter = new CSVWriter(writer)

    for (line <- ma1){
      //println()
      //print(line)
      var seq = line.getSeq[Double](1).toArray
      //var seq = asd.toSeq
      var max_country = (seq.max).toInt
      var min_country = (seq.min).toInt
      var sum_country = seq.sum
      var len_country = seq.length
      var avg_country = BigDecimal(sum_country/len_country).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      csvWriter.writeNext(Array(line(0).toString(), len_country.toString, min_country.toString, max_country.toString, avg_country.toString))
    }
    csvWriter.close()



    var ma = afterweekDF.groupBy($"Country").agg(max($"SalaryFinal")).orderBy($"Country".asc)

  }
}

