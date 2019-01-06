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
    }

    val train_header = trainRDD.first()
    val trainRDD1 = trainRDD.filter{ x => x!=train_header}.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}

    val test_header = testRDD.first()
    val testRDD1  = testRDD.filter{ x => x!=test_header}.map(line => line.split(",")).map{x => ((x(0),x(1)),1)}

    val testRDD10 = testRDD.filter{ x => x!=test_header}.map(line => line.split(",")).map{x => (x(0),x(1),x(2))}
    val testRDD11 = testRDD10.map(r => Rating(user_map(r._1), item_map(r._2), r._3.toDouble))

    val trainRDD2 = trainRDD1.map{ case ((user_id, business_id), star) => (user_id, business_id, star)}
    val testRDD5  = testRDD1.map{ case ((user_id, business_id), star) => (user_id, business_id, star)}

    val trainRDD5 = trainRDD1.map{ case ((user_id, business_id), star) => ((user_map(user_id),item_map(business_id)), star.toDouble)}
    val testRDD25 = testRDD1.map{ case ((user_id, business_id), star) => ((user_map(user_id),item_map(business_id)), star.toDouble)}

    val ratings_data = trainRDD2.map(r => Rating(user_map(r._1), item_map(r._2), r._3.toDouble))

    val r = 20
    val i = 21

    val outfile = new PrintWriter(new File("SaiTeja_Chava_ModelBasedCF.txt"))

    outfile.println("UserId, BusinessId, Pred Star")

    val model = ALS.train(ratings_data, r, i, 0.3)

    val train_data = trainRDD1.map{case ((user_id, business_id), star) => (user_map(user_id),item_map(business_id))}
    val train_predictions = model.predict(train_data).map { case Rating(user_id, business_id, star) => (user_id, business_id, star)}
    val test_data =  testRDD1.map{ case ((user_id, business_id), 1) => (user_map(user_id),item_map(business_id))}

    val pred = model.predict(test_data).map { case Rating(user_id, business_id, star) => (user_id, business_id, star)}
    val pred1 = pred.map { case (user_id, business_id, star) =>((user_id.toInt, business_id.toInt), star.toDouble)}

    val business_length =  trainRDD5.map{case((user_id, business_id),star) => (business_id, star)}.groupByKey().map(elem => (elem._1, elem._2.size))
    val business_sum =  trainRDD5.map{case((user_id, business_id),star) => (business_id, star)}.reduceByKey(_ + _)
    val business_avg = business_sum.join(business_length).map{case(business_id, (sum, len)) => (business_id, sum/len)}

    val user_length =  trainRDD5.map{case((user_id, business_id),star) => (user_id, star)}.groupByKey().map(elem => (elem._1, elem._2.size))
    val user_sum =  trainRDD5.map{case((user_id, business_id),star) => (user_id, star)}.reduceByKey(_ + _)
    val user_avg = user_sum.join(user_length).map{case(user_id, (sum, len)) => (user_id, sum/len)}

    val mapright = pred1.collect().toMap

    val rest = testRDD25.filter(elem => !mapright.keySet.contains(elem._1))

    val restrate = rest.map{case((user_id, business_id), star) => (user_id, (business_id))}.join(user_avg).map{case(user_id,(business_id, avg)) => ((user_id,business_id), avg)}

    val mapright1 = restrate.collect().toMap
    val restrest = rest.filter(elem => !mapright1.keySet.contains(elem._1))

    val restrestrate = restrest.map{case((user_id, business_id), star) => (business_id, (user_id))}.join(business_avg).map{case(business_id,(user_id, avg)) => ((user_id,business_id), avg)}

    val mapright2 = restrestrate.collect().toMap
    val restrestrest = restrest.filter(elem => !mapright2.keySet.contains(elem._1))
    val restrestrestrate = restrestrest.map{case((user_id, business_id), star) => ((user_id,business_id),3.3)}

    val predicttotal = pred1.union(restrate).union(restrestrate)
    val predicttotal1 = predicttotal.union(restrestrestrate)
    
    val preductNomal = predicttotal1.map{case((user, product), pred) =>
      if (pred <= 0) ((user, product), 3.0)
      else if (pred >= 5) ((user, product), 5.0)
      else ((user, product), pred)
    }

    val ratesAndPreds = testRDD11.map { case Rating(user, product, rate_text) => ((user, product), rate_text)}.join(preductNomal)

    val count0 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 0 && elem._2 < 1).count()
    val count1 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 1 && elem._2 < 2).count()
    val count2 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 2 && elem._2 < 3).count()
    val count3 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 3 && elem._2 < 4).count()
    val count4 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 4).count()

    val preductNomal1 = preductNomal.map{case((user, business), pred) => ((reverse_user_map(user),reverse_item_map(business)),pred)}
    
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