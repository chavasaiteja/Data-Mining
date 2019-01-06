import java.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.Map
object SaiTeja_Chava_UserBasedCF {
  def main(args: Array[String]) {
    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Sai").setMaster("local[*]").set("spark.shuffle.spill","false").set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val  trainRDD = sc.textFile(args(0))
    val  testRDD = sc.textFile(args(1))

    val start_time = System.nanoTime()
    val output = new PrintWriter(new File("SaiTeja_Chava_UserBasedCF.txt"))

    // Load our input data.
    //val trainRDD = sc.textFile("/Users/sai/Documents/Data Mining INF 553/Assignments/hw2/Data/train_review.csv")
    val train_header = trainRDD.first()

    //val testRDD  = sc.textFile("/Users/sai/Documents/Data Mining INF 553/Assignments/hw2/Data/test_review.csv")
    val test_header  = testRDD.first()
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

    val trainRDD1 = trainRDD.filter{x => x!=train_header}.map(line => line.split(",")).map{x => (x(0),x(1),x(2))}
    val testRDD1  = testRDD.filter{x => x!=test_header}.map(line => line.split(",")).map{x => (x(0),x(1),1)}
    val sai = testRDD.filter{x => x!=test_header}.map(line => line.split(",")).map{x => ((user_map(x(0)),item_map(x(1))),x(2).toDouble)}

    val test_rdd = testRDD1.map(r => ((user_map(r._1), item_map(r._2)),r._3.toDouble)

    val train_rdd = trainRDD1.map(r => ((user_map(r._1), item_map(r._2)), r._3.toDouble))
    val train_part = train_rdd.filter(elem => elem._1._1<14000)

    val train12  = train_rdd.map{case((user,business),stars) => (business, (user,stars))}
    val train_part2 = sai.filter(elem => elem._1._1>5800)

    var train13 = train_part.union(train_part2)
    var train1 = train13.map{case((user,business),stars) => (business, (user,stars))}

    val testdata = test_rdd.map{case((user, business), num) => (business,(user))}

    val user_stars   =  train_rdd.map{case((user, business),stars) => (user, stars)}
    val user_grouped =  user_stars.groupByKey()
    val user_length  = user_grouped.map(x => (x._1, x._2.size))
    val user_sum     =  user_stars.reduceByKey(_ + _)
    val user_sum_join_length    =  user_sum.join(user_length)
    val user_avg                =  user_sum_join_length.map{case(user, (sum, len)) => (user, sum / len)}
    val train1_join_train1 = train1.join(train1)

    val grid1 = train1_join_train1.map{case(business,((useri, ratei),(userj, ratej))) => ((useri, userj),business,(ratei, ratej))}
    val grid = grid1.filter(elem => elem._1._1<elem._1._2).cache()
    
    val i = grid.map{case((useri, userj),business,(ratei, ratej)) => ((useri, userj),ratei)}
    val j = grid.map{case((useri, userj),business,(ratei, ratej)) => ((useri, userj),ratej)}

    val jj = j.reduceByKey(_ + _)
    val ii = i.reduceByKey(_ + _)

    val matrix_group1 = grid.map{case((useri, userj),business,(ratei, ratej)) => ((useri, userj), (ratei, ratej))}
    val matrix_group = matrix_group1.groupByKey()

    val avg_group1 = ii.join(jj)
    val length = matrix_group.map(x => (x._1, x._2.size))
    val avg_group2 = avg_group1.join(length)
    val avg_group  = avg_group2.map{case((useri, userj),((ratei,ratej),len)) => ((useri, userj),(ratei / len, ratej/len))}

    val map_group1 = grid.map{case((useri, userj),product,(ratei, ratej)) => ((useri, userj),(ratei, ratej))}

    val mean = map_group1.join(avg_group)
    val mean1 = mean.map{case((useri, userj),((ratei, ratej),(meani, meanj))) => ((useri, userj),((ratei - meani),(ratej - meanj)))}

    val abc = mean1.map{case((useri, userj),(meani,meanj)) => ((useri, userj),(meani * meanj))}
    val numerator = abc.reduceByKey(_ + _)

    val bcd1 = mean1.map{case((useri, userj),(meani,meanj)) => ((useri, userj), meani * meani, meanj * meanj)}

    val bcd2 = bcd1.map{case((useri, userj), meanii,  meanjj) => ((useri, userj),meanii)}
    val a = bcd2.reduceByKey(_ + _)

    val bcd3 = bcd1.map{case((useri, userj), meanii, meanjj) => ((useri, userj),meanjj)}
    val b = bcd3.reduceByKey(_ + _)

    val bcd4 = a.join(b)

    val denominator = bcd4.map{case((useri, userj),(meanii, meanjj)) => ((useri, userj), Math.sqrt(meanii) * Math.sqrt(meanjj))}

    val w = numerator.join(denominator)
    val w1 = w.map{case((useri, userj), (num, denom)) =>
      if (denom == 0)
        ((useri, userj), 0.0)
      else ((useri, userj), (num / denom))
    }.cache()

    val item_stars = train_rdd.map { case ((user, item), stars) => (item, stars) }
    val item_grouped = item_stars.groupByKey()
    val item_length = item_grouped.map(x => (x._1, x._2.size))
    val item_sum = item_stars.reduceByKey(_ + _)
    val item_sum_join_length = item_sum.join(item_length)
    val item_avg = item_sum_join_length.map { case (item, (sum, len)) => (item, (sum / len)) }

    val testtrain = testdata.join(train1)
    val testtrain1 = testtrain.map{case(business, (useri,(userj, rj))) => ((useri, userj),(business, rj))}

    val testmean = testtrain1.join(avg_group)
    val testmean1 = testmean.map{case((useri, userj),((business, rj),(mi, mj))) => ((useri, userj),(business, rj - mj))}

    val testratewight = testmean1.join(w1)

    val predictnom = testratewight.map{case((useri, userj),((business, rmean), w)) => ((useri,business), rmean * w)}
    val predictnom1 = predictnom.reduceByKey(_ + _)

    val predictdenom = testratewight.map{case((useri, userj),((business, rmean), w)) => ((useri,business), Math.abs(w))}
    val predictdenom1 = predictdenom.reduceByKey(_ + _)

    val predicright = predictnom1.join(predictdenom1).map{case((user, product), (n, d)) =>
      if (d == 0) (user, (product, 3.0))
      else (user, (product, n / d))
    }

    val predict = user_avg.join(predicright).map{case(user,((left), (business, right))) => ((user, business), left + right)}
    val maprightpart = predict.collect().toMap

    val remaining = test_rdd.filter(elem => !maprightpart.keySet.contains(elem._1))
    val rating_for_remaining = remaining.map{case((user, business), num) => (user, (business))}.join(user_avg).map{case(user,(business, avg)) => ((user,business), avg)}

    val maprightpart1 = rating_for_remaining.collectAsMap()

    val remaining_remaining = remaining.filter(elem => !maprightpart1.keySet.contains(elem._1))
    val rating_for_remaining_reamining = remaining_remaining.map{case((user, business), num) => (business,(user))}.join(item_avg).map{case(business,(user, avg)) => ((user,business),avg)}

    val maprightpart2 = rating_for_remaining_reamining.collectAsMap()

    val remaining_remaining_remaining = remaining_remaining.filter(elem => !maprightpart2.keySet.contains(elem._1))
    val rating_for_reamining_remaining_remaining = remaining_remaining_remaining.map{case((user, business), num) => ((user,business),3.0)}

    val predicttotal = predict.union(rating_for_remaining).union(rating_for_remaining_reamining).union(rating_for_reamining_remaining_remaining)

    val preductNomal = predicttotal.map{case((user, business), pred) =>
      if (pred <= 0) ((user, business), 0.3)
      else if (pred >= 5) ((user, business), 5.0)
      else ((user, business), pred)
    }

    val xyz  = sai.join(preductNomal)

    val n0 = xyz.map{case((user, product),(t, predic)) => ((user, product), Math.abs(t - predic))}.filter(x => x._2 >= 0 && x._2 < 1).count()
    val n1 = xyz.map{case((user, product),(t, predic)) => ((user, product), Math.abs(t - predic))}.filter(x => x._2 >= 1 && x._2 < 2).count()
    val n2 = xyz.map{case((user, product),(t, predic)) => ((user, product), Math.abs(t - predic))}.filter(x => x._2 >= 2 && x._2 < 3).count()
    val n3 = xyz.map{case((user, product),(t, predic)) => ((user, product), Math.abs(t - predic))}.filter(x => x._2 >= 3 && x._2 < 4).count()
    val n4 = xyz.map{case((user, product),(t, predic)) => ((user, product), Math.abs(t - predic))}.filter(x => x._2 >= 4).count()

    val RMSE = xyz.map{case((user, product),(t, predic)) =>
      val err = t - predic
      err * err
    }.mean()
    output.print("UserId,BusinessId,Pred_rating" + "\n")

    val RMSE1 = Math.sqrt(RMSE)

    val preductNomal1 = preductNomal.map{case((user, business), pred) => ((reverse_user_map(user),reverse_item_map(business)),pred)}
    val iter = preductNomal1.sortByKey()
    val iter1 = iter.map(elem => elem._1._1.toString + "," + elem._1._2.toString + "," + elem._2).toLocalIterator
    while(iter1.hasNext) {
      output.print(iter1.next() + "\n")
    }

    print(">= 0 and < 1: " + n0 )
    print("\n>= 1 and < 2: " + n1 )
    print("\n>= 2 and < 3: " + n2)
    print("\n>= 3 and < 4: " + n3 )
    print("\n>= 4: " + n4 )

    print("\nRMSE = " + RMSE1 + "\n")

    output.close()
    val end_time = System.nanoTime()

    print("The total execution time taken is " +  ((end_time - start_time) / 1000000000) + " sec.")

  }
}