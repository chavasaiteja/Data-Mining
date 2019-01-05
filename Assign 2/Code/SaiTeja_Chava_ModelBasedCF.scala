import java.io._

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

import scala.collection.mutable.Map


object SaiTeja_Chava_ModelBasedCF {
  def main(args: Array[String]) {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Sai").setMaster("local")
    val sc = new SparkContext(conf)


    val  trainRDD = sc.textFile(args(0))
    val  testRDD = sc.textFile(args(1))


    // Load our input data.
    //val trainRDD = sc.textFile("/Users/sai/Documents/Data Mining INF 553/Assignments/hw2/Data/train_review.csv")
    //val testRDD  = sc.textFile("/Users/sai/Documents/Data Mining INF 553/Assignments/hw2/Data/test_review.csv")

    var user_map = Map[String,Int]()
    var item_map = Map[String,Int]()
    var reverse_user_map = Map[Int,String]()
    var reverse_item_map = Map[Int,String]()
    var index1 = 0
    var index2 = 0
    testRDD.collect().foreach{ line =>
      var fgh = line.split(",")(0)
      var lkj = line.split(",")(1)
      if(!user_map.contains(fgh)){
        user_map.put(fgh,index1)
        reverse_user_map.put(index1,fgh)
        index1=index1+1
      }
      if(!item_map.contains(lkj))
      {
        item_map.put(lkj,index2)
        reverse_item_map.put(index2,lkj)
        index2=index2+1
      }

      //item_map.put(BigInt(line.split(',')(0).getBytes()).toInt,line.split(",")(0))
      //println(user_map.get(BigInt(line.split(",")(0).getBytes()).toInt))
      //println(line.split(',')(0))
      //user_map += ()
    }
    trainRDD.collect().foreach{ line =>
      var tyu = line.split(",")(0)
      var ghj = line.split(",")(1)
      if(!user_map.contains(tyu)){
        user_map.put(tyu,index1)
        reverse_user_map.put(index1,tyu)
        index1=index1+1
      }
      if(!item_map.contains(ghj))
      {
        item_map.put(ghj,index2)
        reverse_item_map.put(index2,ghj)
        index2=index2+1
      }

      //item_map.put(BigInt(line.split(',')(0).getBytes()).toInt,line.split(",")(0))
      //println(user_map.get(BigInt(line.split(",")(0).getBytes()).toInt))
      //println(line.split(',')(0))
      //user_map += ()
    }

    val train_header = trainRDD.first()
    val trainRDD1 = trainRDD.filter{ x => x!=train_header}.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}

    //println()
    //println("The attributes of the training set are :")
    //println(train_header)

    val test_header = testRDD.first()
    val testRDD1  = testRDD.filter{ x => x!=test_header}.map(line => line.split(",")).map{x => ((x(0),x(1)),1)}

    //println()
    //println("The testRDD1 count is")
    //println(testRDD1.count())
    //println()

    //println()
    //println("The first 5 values of the train data are: ")
    //println()
    //trainRDD1.take(5).foreach(println)

    //println()
    //println("The first 5 values of the test data are: ")
    //println()
    //testRDD1.take(5).foreach(println)
    //xprintln()

    //val data = trainRDD1.map{ case ((user, product), rate) => (user, product, rate)}
    //data.take(5).foreach(println)

    val testRDD10 = testRDD.filter{ x => x!=test_header}.map(line => line.split(",")).map{x => (x(0),x(1),x(2))}
    val testRDD11 = testRDD10.map(r => Rating(user_map(r._1), item_map(r._2), r._3.toDouble))

    val trainRDD2 = trainRDD1.map{ case ((user_id, business_id), star) => (user_id, business_id, star)}
    val testRDD5  = testRDD1.map{ case ((user_id, business_id), star) => (user_id, business_id, star)}

    /*
    val user_ids     = trainRDD2.map(_._1).distinct.sortBy(x => x).zipWithIndex.collectAsMap
    val business_ids = trainRDD2.map(_._2).distinct.sortBy(x => x).zipWithIndex.collectAsMap

    val user_ids1     = testRDD5.map(_._1).distinct.sortBy(x => x).zipWithIndex.collectAsMap
    val business_ids1 = testRDD5.map(_._2).distinct.sortBy(x => x).zipWithIndex.collectAsMap

    */

    //convert to Rating format
    //val trainRDD5 = trainRDD1.map{ case ((user_id, business_id), star) => ((user_ids(user_id).toInt, business_ids(business_id).toInt), star.toDouble)}
    //val testRDD25 = testRDD1.map{ case ((user_id, business_id), star) => ((user_ids1(user_id).toInt, business_ids1(business_id).toInt), star.toDouble)}

    val trainRDD5 = trainRDD1.map{ case ((user_id, business_id), star) => ((user_map(user_id),item_map(business_id)), star.toDouble)}
    val testRDD25 = testRDD1.map{ case ((user_id, business_id), star) => ((user_map(user_id),item_map(business_id)), star.toDouble)}

    //println()
    //println("The testRDD25 count is")
    //println(testRDD25.count())
    //println()

    //val ratings_data = trainRDD2.map(r => Rating(user_ids(r._1).toInt, business_ids(r._2).toInt, r._3.toDouble))
    val ratings_data = trainRDD2.map(r => Rating(user_map(r._1), item_map(r._2), r._3.toDouble))

    //println()
    //println("The first 5 values of the ratings_data are: ")
    //println()
    //ratings_data.take(5).foreach(println)
    //println()

    val r = 20
    val i = 21

    val outfile = new PrintWriter(new File("SaiTeja_Chava_ModelBasedCF.txt"))
    //val outfile1 = new PrintWriter(new File("train_predictions.txt"))
    outfile.println("UserId, BusinessId, Pred Star")

    val model = ALS.train(ratings_data, r, i, 0.3)

    //val train_data = trainRDD1.map{case ((user_id, business_id), star) => (user_ids(user_id).toInt, business_ids(business_id).toInt)}
    val train_data = trainRDD1.map{case ((user_id, business_id), star) => (user_map(user_id),item_map(business_id))}

    val train_predictions = model.predict(train_data).map { case Rating(user_id, business_id, star) => (user_id, business_id, star)}

    /*
    val iter1 = train_predictions.map(elem => elem._1 + "," + elem._2 + "," + elem._3).toLocalIterator
    while(iter1.hasNext) {
      outfile1.print(iter1.next() + "\n")
    }
    outfile1.close()
    */
    val test_data =  testRDD1.map{ case ((user_id, business_id), 1) => (user_map(user_id),item_map(business_id))}

    val pred = model.predict(test_data).map { case Rating(user_id, business_id, star) => (user_id, business_id, star)}

    //println()
    //println("The pred count is")
    //println(pred.count())
    //println()

    val pred1 = pred.map { case (user_id, business_id, star) =>((user_id.toInt, business_id.toInt), star.toDouble)}


    val business_length =  trainRDD5.map{case((user_id, business_id),star) => (business_id, star)}.groupByKey().map(elem => (elem._1, elem._2.size))
    val business_sum =  trainRDD5.map{case((user_id, business_id),star) => (business_id, star)}.reduceByKey(_ + _)
    val business_avg = business_sum.join(business_length).map{case(business_id, (sum, len)) => (business_id, sum/len)}

    val user_length =  trainRDD5.map{case((user_id, business_id),star) => (user_id, star)}.groupByKey().map(elem => (elem._1, elem._2.size))
    val user_sum =  trainRDD5.map{case((user_id, business_id),star) => (user_id, star)}.reduceByKey(_ + _)
    val user_avg = user_sum.join(user_length).map{case(user_id, (sum, len)) => (user_id, sum/len)}



    //println()
    //println("The first 5 values of the user_avg data are: ")
    //println()
    //user_avg.take(5).foreach(println)

    val mapright = pred1.collect().toMap

    /*
    println()
    println("The pred1 count is")
    println(pred1.count())
    println()
    */

    val rest = testRDD25.filter(elem => !mapright.keySet.contains(elem._1))

    /*
    println()
    println("The rest count is")
    println(rest.count())
    println()
    */
    //(user_ids1(elem._._1).toInt,business_ids1(elem._._2).toInt)))
    val restrate = rest.map{case((user_id, business_id), star) => (user_id, (business_id))}.join(user_avg).map{case(user_id,(business_id, avg)) => ((user_id,business_id), avg)}

    //println()
    //println("The restrate count is")
    //println(restrate.count())
    //println()

    val mapright1 = restrate.collect().toMap
    val restrest = rest.filter(elem => !mapright1.keySet.contains(elem._1))

    /*
    println()
    println("The restrest count is")
    println(restrest.count())
    println()
  */
    val restrestrate = restrest.map{case((user_id, business_id), star) => (business_id, (user_id))}.join(business_avg).map{case(business_id,(user_id, avg)) => ((user_id,business_id), avg)}

    /*
    println()
    println("The restrestrate count is")
    println(restrestrate.count())
    println()
     */

    val mapright2 = restrestrate.collect().toMap
    val restrestrest = restrest.filter(elem => !mapright2.keySet.contains(elem._1))

    /*
    println()
    println("The restrestrest count is")
    println(restrestrest.count())
    println()
    */

    val restrestrestrate = restrestrest.map{case((user_id, business_id), star) => ((user_id,business_id),3.3)}

    /*
    println()
    println("The restrestrestrate count is")
    println(restrestrestrate.count())
    println()
    */

    //val predicttotal = pred1.++(restrate)
    val predicttotal = pred1.union(restrate).union(restrestrate)

    /*
    println()
    println("The predicttotal count is")
    println(predicttotal.count())
    println()
    */

    val predicttotal1 = predicttotal.union(restrestrestrate)

    /*
    println()
    println("The predicttotal count is")
    println(predicttotal1.count())
    println()
    */

    val preductNomal = predicttotal1.map{case((user, product), pred) =>
      if (pred <= 0) ((user, product), 3.0)
      else if (pred >= 5) ((user, product), 5.0)
      else ((user, product), pred)
    }

    /*
    println()
    println("The preductNomal count is")
    println(preductNomal.count())
    println()
    */

    val ratesAndPreds = testRDD11.map { case Rating(user, product, rate_text) => ((user, product), rate_text)}.join(preductNomal)

    /*
    println()
    println(ratesAndPreds.count())
    println()
    */

    val count0 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 0 && elem._2 < 1).count()
    val count1 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 1 && elem._2 < 2).count()
    val count2 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 2 && elem._2 < 3).count()
    val count3 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 3 && elem._2 < 4).count()
    val count4 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 4).count()


    val preductNomal1 = preductNomal.map{case((user, business), pred) => ((reverse_user_map(user),reverse_item_map(business)),pred)}
    //preduct
    val iter = preductNomal1.sortByKey().map(elem => elem._1._1 + "," + elem._1._2 + "," + elem._2).toLocalIterator
    while(iter.hasNext) {
      outfile.print(iter.next() + "\n")
    }

    val RMSE = ratesAndPreds.map{case((user, product),(t, predic)) =>
      val err = t - predic
      err * err
    }.mean()

    outfile.close()
    print(">= 0 and < 1: " + count0 + "\n")
    print(">= 1 and < 2: " + count1 + "\n")
    print(">= 2 and < 3: " + count2 + "\n")
    print(">= 3 and < 4: " + count3 + "\n")
    print(">= 4: " + count4 + "\n")
    println()
    print("RMSE = " + Math.sqrt(RMSE) + "\n")

  }
}