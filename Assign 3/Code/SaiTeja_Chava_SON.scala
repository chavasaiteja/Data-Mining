import java.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import util.control.Breaks._

object SaiTeja_Chava_SON {

  def runapriori(basket: Iterator[Set[String]], threshold:Int): Iterator[Set[String]] = {

    var cl = basket.toList
    var tuple_size = 2
    var candidate_set_size = 0

    val items_of_size_one = cl.flatten.groupBy(identity)
    val items_of_size_one1 = items_of_size_one.mapValues(_.size)
    val items_of_size_one2 = items_of_size_one1.filter(x => x._2 >= threshold).map(x => x._1).toSet
    var candidate_set = Set.empty[Set[String]]

    var freq_items = items_of_size_one2

    for (i <- freq_items){
      candidate_set = candidate_set + Set(i)
    }

    candidate_set_size = candidate_set.size

    var result = Set.empty[String]

    tuple_size = 2

    var candidate = freq_items.subsets(tuple_size)

    var flag = 0

    while(freq_items.size > tuple_size - 1){
      for(x <- candidate){
        breakable{
          var count = 0
          flag = 0
          for (z <- cl){
            if(x.subsetOf(z)){
              if(flag==1){
                flag = 0
                break
              }
              count = count + 1
              if(count >= threshold){
                result = result ++ x
                candidate_set+=x
                flag = 1
              }
            }
          }
        }
      }

      if(candidate_set.size > candidate_set_size){
        freq_items = result
        result = Set.empty[String]
        candidate_set_size = candidate_set.size
        tuple_size = tuple_size + 1
      }
      else{
        tuple_size = 1 + freq_items.size
      }
      candidate = freq_items.subsets(tuple_size)
    }
    candidate_set.iterator
  }
  
  def sort[A : Ordering](coll: Seq[Iterable[A]]) = coll.sorted

  def main(args: Array[String]) {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //val outfile = new PrintWriter(new File("SaiTeja_Chava_SON_yelp_reviews_test_30.txt"))
    val outfile = new PrintWriter(new File(args(2)))
    val start_time = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("Sai").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))
    val support = args(1).toInt //.toDouble
    var tdp = ""

    val baskets = input.map(line => (line.split(",")(0),line.split(",")(1))).groupByKey().map(_._2.toSet)//sortByKey()
    val no_of_baskets = baskets.count()
    var no_of_partitions = input.getNumPartitions

    var thresold = 0
    var sus = support/no_of_partitions
    if(sus > 1) {
      thresold = sus
    }
    else{
      thresold = 1
    }
    var first = baskets.mapPartitions(chunk => {
      runapriori(chunk, thresold)
    }).map(x=>(x,1))

    var first1 = first.reduceByKey((v1,v2)=>1).map(_._1).collect()
    
    val broadcasted = sc.broadcast(first1)
    
    var secondmap = baskets.mapPartitions(chunk => {
      var chunklist = chunk.toList
      var count10 = 0
      var count01 = 0
      var out = List[(Set[String],Int)]()
      for (i<- chunklist){
        count01 = count01 + 1
        for (j<- broadcasted.value){
          if (j.forall(i.contains)){
            count10 = count10 + 1
            out = Tuple2(j,1) :: out
          }
        }
      }
      out.iterator
    })

    var last = secondmap.reduceByKey(_+_)
    var last1 = last.filter(_._2 >= support)
    var last2 = last1.map(_._1).map(x => (x.size,x)).collect()
    val max = last2.maxBy(_._1)._1

    var tosort = sort(last2.filter(_._1 == 1).map(_._2))

    for (i<- tosort)
    {
      outfile.write(i.mkString("(","', '",")"))
      if (i==tosort.last){
        outfile.write("\n")
        outfile.write("\n")
      }
      else {
        outfile.write(", ")
      }
    }

    for (i <- 2.to(max)){
      var x = sort(last2.filter(_._1 == i) map(_._2))
      var temp = x.map(k => k.toList.sorted)
      var temp1 = sort(temp)
      for (k<- temp1)
      {   tdp = tdp + "("
        var s = k
        for (j<- s){
          if (j!=s.last){
            tdp =tdp + j +", "
          }
          else {
            tdp = tdp + j +"), "
          }
        }
      }
      tdp = tdp.dropRight(2)
      if(i!=2.to(max).last) {
        tdp = tdp + "\n"
        tdp = tdp + "\n"
      }
    }
    outfile.write(tdp)
    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time)/1000 + " secs")
    outfile.close()
  }
}
