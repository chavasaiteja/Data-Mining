import java.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.{HashMap, Set}
import scala.collection.immutable.ListMap
import scala.util.control._
import scala.collection.mutable.ListBuffer

object Task1 {

  def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Sai").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))
    val feature = args(1)
    val no_of_clusters = args(2).toInt
    val no_of_iterations = args(3).toInt

    var no_of_documents = 0
    var unique_words =  Set[String]()

    input.collect().foreach { line =>
      no_of_documents += 1
      for(word <- line.split(" "))
      {
        unique_words+=word
      }
    }

    var unique_words_with_index = unique_words.toList.zipWithIndex
    val no_of_unique_words = unique_words.size
    println()
    println("No of documents are : "+no_of_documents)
    println("Unique words count is : "+no_of_unique_words)

    //################## TF - IDF ########################

    var tfs = new ListBuffer[(String,Double)]()
    var document_no = 0
    input.collect().foreach{line =>
      var term_freq = HashMap.empty[String, Double]
      for(word <- line.split(" ")){
        if(term_freq.contains(word))
          term_freq.put(word,term_freq(word)+1)
        else
          term_freq.put(word,1)
      }
      var document_length = line.split(" ").length
      for((i,j) <- term_freq){
        term_freq.put(i,j/document_length)
      }
      for((word,term_f) <- term_freq){
        var i = (word,term_f)
        tfs += i
      }
      term_freq = HashMap.empty[String, Double]
    }

    var tfs_final = tfs.toMap

    var tfs1 = tfs.toArray
    var tfs2 = sc.parallelize(tfs1)
    var tfs_grouped = tfs2.groupByKey()
    val tfs_length  = tfs_grouped.map(x => (x._1, x._2.size))
    val tfs_sum     = tfs2.reduceByKey(_ + _)
    val tfs_sum_join_length = tfs_sum.join(tfs_length)
    val tfs_avg = tfs_sum_join_length.map{case(cluster, (sum, len)) => (cluster, sum / len)}


    val idfs = HashMap.empty[String, Double]
    tfs_length.collect().foreach{ x =>
      idfs(x._1) = scala.math.log(no_of_documents/x._2)
    }

    val tf_idfs = HashMap.empty[String, Double]
    for(x <- idfs) {
      tf_idfs(x._1) = x._2 * tfs_final(x._1)
    }

    //#################### Word count ##########################

    var word_count = HashMap.empty[String, Double]

    input.collect().foreach{line =>
      for(word <- line.split(" "))
      {
        if (word_count.contains(word))
          word_count.put(word, word_count(word) + 1)
        else
          word_count.put(word, 1)

      }
    }

    println()
    println("Word count Hash map size is : "+word_count.size)

    var sorted_word_count = ListMap[String,Double]()
    var sorted_word_count1 = List[(String,Double)]()

    if(feature == "W") {
      sorted_word_count = ListMap(word_count.toSeq.sortWith(_._2 > _._2): _*)
      sorted_word_count1 = sorted_word_count.toList

      println()
      println("sorted word count size is : " + sorted_word_count.size)
      println("sorted word count is as follows :")
      println(sorted_word_count)
    }

    else if(feature == "T") {

    sorted_word_count = ListMap(tf_idfs.toSeq.sortWith(_._2 > _._2): _*)
    sorted_word_count1 = sorted_word_count.toList

    println()
    println("sorted word count size is : " + sorted_word_count.size)
    println("sorted word count is as follows :")
    println(sorted_word_count)

    }

    val r = new scala.util.Random(20181031)

    var centroids = new Array[Double](no_of_clusters)
    var centroid_words = new Array[String](no_of_clusters)

    val centroid_word_centroid = new HashMap[String, Double]()
    var centroid_word_centroid1 = new HashMap[Double, Double]()

    for (j <- 1 until no_of_clusters + 1){
      println(j)
      var temp = j-1
      var random_num = r.nextInt(no_of_unique_words)
      centroids(temp) = sorted_word_count1(random_num)._2
      centroid_words(temp) = sorted_word_count1(random_num)._1
      centroid_word_centroid(sorted_word_count1(random_num)._1) = temp
    }

    println()
    println("Centroid after random assignment are :")
    for(x<-centroids){
      println(x)
    }

    println()
    println("Centroid count and word are :")

    for((centroid, centroid_word) <- centroids zip centroid_words){
      println(centroid +" : "+centroid_word)
    }

    //val assignments = new HashMap[String, (Int,Int)]()
    val assignments = new HashMap[String, (Double,Double)]()
    val loop = new Breaks;

    var iteration_num = 0

    while(iteration_num<no_of_iterations) {
      for ((i, j) <- unique_words_with_index) {
        var temp1 = sorted_word_count(i)
        var centroid_num = 0
        var min_val = 1000000.0
        var assignment_no = 0
        var flag = 0
        loop.breakable {
          for (k <- centroids) {
            if (iteration_num == 0 && (centroid_words contains i )) {
              assignments(i) = (temp1, centroid_word_centroid(i))
              flag = 1
              loop.break()
            }
            else if(iteration_num!=0 && (centroids contains temp1)){
              assignments(i) = (temp1,centroid_word_centroid1(temp1))
              flag = 1
              loop.break()
            }
            if (Math.abs(k - temp1) < min_val) {
              min_val = Math.abs(k - temp1)
              assignment_no = centroid_num
            }
            centroid_num += 1
          }
        }
        if (flag == 0) {
          assignments(i) = (temp1, assignment_no)
        }
      }

      var new_centers_1 = assignments.toArray
      var new_centers = sc.parallelize(new_centers_1)
      var cluster_assignment = new_centers.map { case (a, (b, c)) => (c, b) }
      var cluster_grouped = cluster_assignment.groupByKey()
      val cluster_length  = cluster_grouped.map(x => (x._1, x._2.size))
      val cluster_sum = cluster_assignment.reduceByKey(_ + _)
      val cluster_sum_join_length = cluster_sum.join(cluster_length)
      val cluster_avg = cluster_sum_join_length.map{case(cluster, (sum, len)) => (cluster, sum / len)}

      println()
      println("Cluster lenghts are as follows :")
      for (centroid_and_its_count <- cluster_length) {
        println(centroid_and_its_count)
      }
      println()

      println()
      println("Cluster avg are as follows :")
      for (centroid_and_its_count <- cluster_avg) {
        println(centroid_and_its_count)
      }
      println()

      println()

      var cluster_nums =  Set[Int]()
      centroid_word_centroid1 = new HashMap[Double, Double]()
      cluster_avg.collect().foreach {case (cluster_num, cluster_count) =>
        centroids(cluster_num.toInt) = cluster_count
        cluster_nums+=cluster_num.toInt
        centroid_word_centroid1(cluster_count) = cluster_num
      }

      if(centroid_word_centroid1.size!=5){
        for(j <- 0 until no_of_clusters){
          if(cluster_nums contains(j)){

          }
          else{
            centroid_word_centroid1(1.0) = j
          }
        }
      }

      println()
      println("Centroids are as follows :")
      for (centroid <- centroids){
        println(centroid)
      }

      println(centroid_word_centroid1)
      iteration_num+=1
    }

    var final_assignments = new HashMap[String, Double]()


    for ((i, j) <- unique_words_with_index){
      var temp1 = sorted_word_count(i)
      var centroid_num = 0
      var min_val = 1000000.0
      var assignment_no = 0
      var flag = 0
      loop.breakable {
        for (k <- centroids) {
          if (iteration_num == 0 && (centroid_words contains i )) {
            final_assignments(i) = centroid_word_centroid(i)
            flag = 1
            loop.break()
          }
          else if(iteration_num!=0 && (centroids contains temp1)){
            final_assignments(i) = centroid_word_centroid1(temp1)
            flag = 1
            loop.break()
          }
          if (Math.abs(k - temp1) < min_val) {
            min_val = Math.abs(k - temp1)
            assignment_no = centroid_num
          }
          centroid_num += 1
        }
      }
      if (flag == 0) {
        final_assignments(i) = assignment_no
      }
    }

    var cluster1 = new HashMap[String, Double]()
    var cluster2 = new HashMap[String, Double]()
    var cluster3 = new HashMap[String, Double]()
    var cluster4 = new HashMap[String, Double]()
    var cluster5 = new HashMap[String, Double]()

    var cluster1_error = 0.0
    var cluster2_error = 0.0
    var cluster3_error = 0.0
    var cluster4_error = 0.0
    var cluster5_error = 0.0


    for ((i, j) <- unique_words_with_index){
      if(final_assignments(i)==0){
        cluster1(i) = sorted_word_count(i)
        cluster1_error += (centroids(0)-sorted_word_count(i))*(centroids(0)-sorted_word_count(i))
      }
      else if(final_assignments(i)==1){
        cluster2(i) = sorted_word_count(i)
        cluster2_error += (centroids(1)-sorted_word_count(i))*(centroids(1)-sorted_word_count(i))
      }
      else if(final_assignments(i)==2){
        cluster3(i) = sorted_word_count(i)
        cluster3_error += (centroids(2)-sorted_word_count(i))*(centroids(2)-sorted_word_count(i))
      }
      else if(final_assignments(i)==3){
        cluster4(i) = sorted_word_count(i)
        cluster4_error += (centroids(3)-sorted_word_count(i))*(centroids(3)-sorted_word_count(i))
      }
      else if(final_assignments(i)==4){
        cluster5(i) = sorted_word_count(i)
        cluster5_error += (centroids(4)-sorted_word_count(i))*(centroids(4)-sorted_word_count(i))
      }
    }

    var WSSE = cluster1_error + cluster2_error + cluster3_error + cluster4_error + cluster5_error

    cluster1_error = math.sqrt(cluster1_error)
    cluster2_error = math.sqrt(cluster2_error)
    cluster3_error = math.sqrt(cluster3_error)
    cluster4_error = math.sqrt(cluster4_error)
    cluster5_error = math.sqrt(cluster5_error)


    println()
    println("Size of cluster1 is : "+cluster1.size)
    println("Size of cluster2 is : "+cluster2.size)
    println("Size of cluster3 is : "+cluster3.size)
    println("Size of cluster4 is : "+cluster4.size)
    println("Size of cluster5 is : "+cluster5.size)

    var cluster1_map = ListMap(cluster1.toSeq.sortWith(_._2 > _._2):_*)
    var cluster2_map = ListMap(cluster2.toSeq.sortWith(_._2 > _._2):_*)
    var cluster3_map = ListMap(cluster3.toSeq.sortWith(_._2 > _._2):_*)
    var cluster4_map = ListMap(cluster4.toSeq.sortWith(_._2 > _._2):_*)
    var cluster5_map = ListMap(cluster5.toSeq.sortWith(_._2 > _._2):_*)

    var cluster1_list = cluster1_map.toList
    var cluster2_list = cluster2_map.toList
    var cluster3_list = cluster3_map.toList
    var cluster4_list = cluster4_map.toList
    var cluster5_list = cluster5_map.toList


    var words_in_cluster = new HashMap[Int, List[String]]()
    for(i <- 1 to no_of_clusters+1){
      if(i==1){
        var temp = ListBuffer[String]()
        for(j <- 0 to 9){
          temp+= cluster1_list(j)._1
        }
        words_in_cluster(i) = temp.toList
      }

      else if(i==2){
        var temp = ListBuffer[String]()
        for(j <- 0 to 9){
          temp+= cluster2_list(j)._1
        }
        words_in_cluster(i) = temp.toList
      }

      else if(i==3){
        var temp = ListBuffer[String]()
        for(j <- 0 to 9){
          temp+= cluster3_list(j)._1
        }
        words_in_cluster(i) = temp.toList
      }

      else if(i==4){
        var temp = ListBuffer[String]()
        for(j <- 0 to 9){
          temp+= cluster4_list(j)._1
        }
        words_in_cluster(i) = temp.toList
      }

      else if(i==5){
        var temp = ListBuffer[String]()
        for(j <- 0 to 9){
          temp+= cluster5_list(j)._1
        }
        words_in_cluster(i) = temp.toList
      }


    }

    println()
    println(cluster1_map)
    println(cluster2_map)
    println(cluster3_map)
    println(cluster4_map)
    println(cluster5_map)

    println()
    println("Cluster 1 error is : "+cluster1_error)
    println("Cluster 2 error is : "+cluster2_error)
    println("Cluster 3 error is : "+cluster3_error)
    println("Cluster 4 error is : "+cluster4_error)
    println("Cluster 5 error is : "+cluster5_error)

    if(feature=="W") {

      val file = new File("SaiTeja_Chava_KMeans_small_W_5_20.json")
      val bw = new BufferedWriter(new FileWriter(file))
      var cluster_size = 0
      var cluster_error = 0.0
      bw.write("{ \n")
      bw.write("\t \"algorithm\": \"K-Means\", \n")
      bw.write("\t \"WSSE\": " + WSSE + ", \n")
      bw.write("\t \"clusters\": [ { \n")
      for (i <- 1 to no_of_clusters) {
        if (i != 1) {
          bw.write("\t { \n")
        }
        bw.write("\t \t \"id\": " + i + ", \n")
        if(i==1){
          cluster_size = cluster1.size
          cluster_error = cluster1_error
        }
        else if(i==2){
          cluster_size = cluster2.size
          cluster_error = cluster2_error
        }
        else if(i==3){
          cluster_size = cluster3.size
          cluster_error = cluster3_error
        }
        else if(i==4){
          cluster_size = cluster4.size
          cluster_error = cluster4_error
        }
        else if(i==5){
          cluster_size = cluster5.size
          cluster_error = cluster5_error
        }

        bw.write("\t \t \"size\": " + cluster_size + ", \n")
        bw.write("\t \t \"error\": " + cluster_error + ",\n")
        bw.write("\t \t \"terms\": [") //+ words_in_cluster(i).mkString(", ") + "]  \n")
        var temp = words_in_cluster(i)
        for(k<- 0 to 9){
          bw.write("\"" + temp(k) + "\"")
          if(k!=9){
            bw.write(",")
          }
        }
        bw.write("]\n")
        if (i == no_of_clusters) {
          bw.write("\t } ] \n")
        }
        else
          bw.write("\t }, \n")
      }
      bw.write("} \n")
      bw.close()

    }

    else if(feature=="T"){

      val file = new File("SaiTeja_Chava_KMeans_small_T_5_20.json")
      val bw = new BufferedWriter(new FileWriter(file))
      var cluster_size = 0
      var cluster_error = 0.0
      bw.write("{ \n")
      bw.write("\t \"algorithm\": \"K-Means\", \n")
      bw.write("\t \"WSSE\": " + WSSE + ", \n")
      bw.write("\t \"clusters\": [ { \n")
      for (i <- 1 to no_of_clusters) {
        if (i != 1) {
          bw.write("\t { \n")
        }
        bw.write("\t \t \"id\": " + i + ", \n")
        if(i==1){
          cluster_size = cluster1.size
          cluster_error = cluster1_error
        }
        else if(i==2){
          cluster_size = cluster2.size
          cluster_error = cluster2_error
        }
        else if(i==3){
          cluster_size = cluster3.size
          cluster_error = cluster3_error
        }
        else if(i==4){
          cluster_size = cluster4.size
          cluster_error = cluster4_error
        }
        else if(i==5){
          cluster_size = cluster5.size
          cluster_error = cluster5_error
        }

        bw.write("\t \t \"size\": " + cluster_size + ", \n")
        bw.write("\t \t \"error\": " + cluster_error + ",\n")
        bw.write("\t \t \"terms\": [")
        var temp = words_in_cluster(i)
        for(k<- 0 to 9){
          bw.write("\"" + temp(k) + "\"")
          if(k!=9){
            bw.write(",")
          }
        }
        bw.write("]\n")
        if (i == no_of_clusters) {
          bw.write("\t } ] \n")
        }
        else
          bw.write("\t }, \n")
      }
      bw.write("} \n")
      bw.close()

    }


  }
}

