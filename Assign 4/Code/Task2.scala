import java.io.{BufferedWriter, File, FileWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

object Task2 {

  def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Sai").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))
    val input2 =  sc.textFile(args(0)).zipWithIndex()
    val input1 = sc.textFile(args(0)).zipWithIndex().collect()

    val algorithm = args(1)
    val no_of_clusters = args(2).toInt
    val no_of_iterations = args(3).toInt
    val random_seed = 42

    // Load documents (one per line).
    val documents: RDD[Seq[String]] = input
      .map(_.split(" ").toSeq)

    val hashingTF = new HashingTF(15000)
    val tf: RDD[Vector] = hashingTF.transform(documents)

    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    val idfIgnore = new IDF(minDocFreq = 0).fit(tf)
    val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)

    var tfidfIgnore1 :RDD[(Vector,Long)] = tfidfIgnore.zipWithIndex()

    println("tf idf count is : "+tfidfIgnore.count())
    tfidfIgnore.take(1).foreach(println)

    val numClusters = no_of_clusters
    val numIterations = no_of_iterations


    if(algorithm=="K") {
      val clusters = KMeans.train(tfidfIgnore, numClusters, numIterations, initializationMode = "k-means||", 42)

      val predictions = clusters.predict(tfidfIgnore).zipWithIndex()

      println()
      for(x <- predictions){
        print(x+",")
      }

      val cluster_to_words = new HashMap[Int, ArrayBuffer[String]]()
      predictions.collect().foreach { case (clusterid, doc_id) =>
        if (cluster_to_words contains clusterid) {
          var temp = cluster_to_words(clusterid)
          var doc = input1(doc_id.toInt)._1
          for (word <- doc.split(" ")) {
            temp += word
          }
          cluster_to_words(clusterid) = temp
        }
        else {
          var temp = ArrayBuffer[String]()
          var doc = input1(doc_id.toInt)._1
          for (word <- doc.split(" ")) {
            temp += word
          }
          cluster_to_words(clusterid) = temp
        }

      }

      val cluster_to_words_count = new HashMap[Int, List[(String, Int)]]()
      for (cluster_to_word <- cluster_to_words) {
        val words = cluster_to_word._2.map(word => (word, 1)).groupBy(_._1)
        val reduce = words.map(word => (word._1, word._2.size)).toList.sortWith((_._2 > _._2))
        cluster_to_words_count(cluster_to_word._1) = reduce
        //println(reduce)
      }

      //println(cluster_to_words_count)


      val cluster_map = new HashMap[Int, Int]()

      predictions.collect().foreach { case (cluster_id, doc_id) =>
        if (cluster_map contains cluster_id) {
          var temp = cluster_map(cluster_id)
          temp = temp + 1
          cluster_map(cluster_id) = temp
        }
        else {
          cluster_map(cluster_id) = 1
        }
      }

      var sorted_cluster_map = ListMap(cluster_map.toSeq.sortBy(_._1): _*)
      println(sorted_cluster_map)

      println("Prediction count is : " + predictions.count())
      val cluster_centroids = clusters.clusterCenters


      // Evaluate clustering by computing Within Set Sum of Squared Errors
      println("Kmeans")

      val WSSSE = clusters.computeCost(tfidfIgnore)
      println(s"Within Set Sum of Squared Errors = $WSSSE")

      println("Centroid of Kmeans are as follows")

      for(centroid <- cluster_centroids) {
        println(centroid)
      }

      val docno_to_cluster = HashMap.empty[Int,Int]
      predictions.collect().foreach{line =>
        docno_to_cluster.put(line._2.toInt+1,line._1)
      }

      println()
      println(docno_to_cluster)

      var cluster_vectors  = new HashMap[Int, Array[Vector]]()

      tfidfIgnore1.collect().foreach{ d_c =>
        if(cluster_vectors.contains(docno_to_cluster(d_c._2.toInt+1))){
          cluster_vectors(docno_to_cluster(d_c._2.toInt+1)) = cluster_vectors(docno_to_cluster(d_c._2.toInt+1)) :+ d_c._1
        }
        else{
          cluster_vectors.put(docno_to_cluster(d_c._2.toInt+1),Array(d_c._1))
        }

      }

      println()
      println(cluster_vectors)

      var cluster_vec_rdd = HashMap.empty[Int,RDD[Vector]]

      for(cluster_vector <- cluster_vectors){
        cluster_vec_rdd(cluster_vector._1) = sc.parallelize((cluster_vector._2))
      }


      var cluster_errors = HashMap.empty[Int,Double]

      for(l <- cluster_vectors){
        var cent = clusters.clusterCenters(l._1)
        for(vec <- l._2){
          if(cluster_errors contains(l._1)) {
            cluster_errors(l._1) = cluster_errors(l._1) + Vectors.sqdist(cent, vec)
          }
          else{
            cluster_errors(l._1) = Vectors.sqdist(cent, vec)
          }
        }
        cluster_errors(l._1) = Math.sqrt(cluster_errors(l._1))
      }


      println("Cluster errors")
      println(cluster_errors)


      val file = new File("SaiTeja_Chava_KMeans_small_K_8_20.json")
      val bw = new BufferedWriter(new FileWriter(file))
      //var cluster_error = 0.0
      bw.write("{ \n")
      bw.write("\t \"algorithm\": \"K-Means\", \n")
      bw.write("\t \"WSSE\":" + WSSSE + ", \n")
      bw.write("\t \"clusters\": [ { \n")
      for (i <- 1 to no_of_clusters) {
        if (i != 1) {
          bw.write("\t { \n")
        }
        bw.write("\t \t \"id\": " + i + ", \n")
        bw.write("\t \t \"size\": " + sorted_cluster_map(i-1) + ", \n")
        bw.write("\t \t \"error\": " + cluster_errors(i-1) + ",\n")
        bw.write("\t \t \"terms\": [")
        var temp = cluster_to_words_count(i-1)
        for(k<- 0 to 9){
          bw.write("\"" + temp(k)._1 + "\"")
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

    else if(algorithm=="B") {

      val bkm = new BisectingKMeans().setK(numClusters).setSeed(random_seed).setMaxIterations(numIterations)
      val model = bkm.run(tfidfIgnore)


      println()
      println("Bisecting K means")
      // Show the compute cost and the cluster centers
      println(s"Compute Cost: ${model.computeCost(tfidfIgnore)}")
      model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
        //println(s"Cluster Center ${idx}: ${center}")
      }

      val predictions_bkm = model.predict(tfidfIgnore).zipWithIndex()
      println("Predictions bkm count is : " + predictions_bkm.count())

      println()
      for(x <- predictions_bkm){
        print(x+",")
      }

      val cluster_to_words_bkm = new HashMap[Int, ArrayBuffer[String]]()

      predictions_bkm.collect().foreach { case (clusterid, doc_id) =>
        if (cluster_to_words_bkm contains clusterid) {
          var temp = cluster_to_words_bkm(clusterid)
          var doc = input1(doc_id.toInt)._1
          for (word <- doc.split(" ")) {
            temp += word
          }
          cluster_to_words_bkm(clusterid) = temp
        }
        else {
          var temp = ArrayBuffer[String]()
          var doc = input1(doc_id.toInt)._1
          for (word <- doc.split(" ")) {
            temp += word
          }
          cluster_to_words_bkm(clusterid) = temp
        }

      }

      val cluster_to_words_count_bkm = new HashMap[Int, List[(String, Int)]]()
      for (cluster_to_word_bkm <- cluster_to_words_bkm) {
        val words = cluster_to_word_bkm._2.map(word => (word, 1)).groupBy(_._1)
        val reduce = words.map(word => (word._1, word._2.size)).toList.sortWith((_._2 > _._2))
        cluster_to_words_count_bkm(cluster_to_word_bkm._1) = reduce
        println(reduce)
      }

      val cluster_map_bkm = new HashMap[Int, Int]()
      predictions_bkm.collect().foreach { case (cluster_id, doc_id) =>
        if (cluster_map_bkm contains cluster_id) {
          var temp = cluster_map_bkm(cluster_id)
          temp = temp + 1
          cluster_map_bkm(cluster_id) = temp
        }
        else {
          cluster_map_bkm(cluster_id) = 1
        }
      }

      var sorted_cluster_map_bkm = ListMap(cluster_map_bkm.toSeq.sortBy(_._1): _*)
      println(sorted_cluster_map_bkm)

      val docno_to_cluster = HashMap.empty[Int,Int]
      predictions_bkm.collect().foreach{line =>
        docno_to_cluster.put(line._2.toInt+1,line._1)
      }

      println()
      println(docno_to_cluster)

      var cluster_vectors  = new HashMap[Int, Array[Vector]]()

      tfidfIgnore1.collect().foreach{ d_c =>
        if(cluster_vectors.contains(docno_to_cluster(d_c._2.toInt+1))){
          cluster_vectors(docno_to_cluster(d_c._2.toInt+1)) = cluster_vectors(docno_to_cluster(d_c._2.toInt+1)) :+ d_c._1
        }
        else{
          cluster_vectors.put(docno_to_cluster(d_c._2.toInt+1),Array(d_c._1))
        }

      }

      println()
      println(cluster_vectors)

      var cluster_vec_rdd = HashMap.empty[Int,RDD[Vector]]

      for(cluster_vector <- cluster_vectors){
        cluster_vec_rdd(cluster_vector._1) = sc.parallelize((cluster_vector._2))
      }


      var cluster_errors = HashMap.empty[Int,Double]

      for(l <- cluster_vectors){
        var cent = model.clusterCenters(l._1)
        for(vec <- l._2){
          if(cluster_errors contains(l._1)) {
            cluster_errors(l._1) = cluster_errors(l._1) + Vectors.sqdist(cent, vec)
          }
          else{
            cluster_errors(l._1) = Vectors.sqdist(cent, vec)
          }
        }
        cluster_errors(l._1) = Math.sqrt(cluster_errors(l._1))
      }

      val file = new File("SaiTeja_Chava_KMeans_small_B_8_20.json")
      val bw = new BufferedWriter(new FileWriter(file))
      //var cluster_size = 0
      //var cluster_error = 0.0
      bw.write("{ \n")
      bw.write("\t \"algorithm\": \"Bisecting K-Means\", \n")
      bw.write("\t \"WSSE\":" + model.computeCost(tfidfIgnore) + ", \n")
      bw.write("\t \"clusters\": [ { \n")
      for (i <- 1 to no_of_clusters) {
        if (i != 1) {
          bw.write("\t { \n")
        }
        bw.write("\t \t \"id\": " + i + ", \n")
        bw.write("\t \t \"size\": " + sorted_cluster_map_bkm(i-1) + ", \n")
        bw.write("\t \t \"error\" : " + cluster_errors(i-1) + ",\n")
        bw.write("\t \t \"terms\": [")
        var temp = cluster_to_words_count_bkm(i-1)
        for(k<- 0 to 9){
          bw.write("\"" + temp(k)._1 + "\"")
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

    else if(algorithm=="G") {
      // Cluster the data into two classes using GaussianMixture
      val gmm = new GaussianMixture().setK(numClusters).setSeed(random_seed).setMaxIterations(numIterations).run(tfidfIgnore)

      // Save and load model
      gmm.save(sc, "/Users/sai/IdeaProjects/untitled/src/main/scala/GaussianMixtureModel")
      val sameModel = GaussianMixtureModel.load(sc,
        "/Users/sai/IdeaProjects/untitled/src/main/scala/GaussianMixtureModel")

      println()
      println("Guassian Mixture")
      // output parameters of max-likelihood model
      for (i <- 0 until gmm.k) {
        println("weight=%f\nmu=%s\nsigma=\n%s\n" format
          (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
      }

      val predictions_gmm = gmm.predict(tfidfIgnore).zipWithIndex()
      println("Predictions gmm count is : " + predictions_gmm.count())

      val cluster_to_words_gmm = new HashMap[Int, ArrayBuffer[String]]()

      predictions_gmm.collect().foreach { case (clusterid, doc_id) =>
        if (cluster_to_words_gmm contains clusterid) {
          var temp = cluster_to_words_gmm(clusterid)
          var doc = input1(doc_id.toInt)._1
          for (word <- doc.split(" ")) {
            temp += word
          }
          cluster_to_words_gmm(clusterid) = temp
        }
        else {
          var temp = ArrayBuffer[String]()
          var doc = input1(doc_id.toInt)._1
          for (word <- doc.split(" ")) {
            temp += word
          }
          cluster_to_words_gmm(clusterid) = temp
        }

      }

      val cluster_to_words_count_gmm = new HashMap[Int, List[(String, Int)]]()
      for (cluster_to_word_gmm <- cluster_to_words_gmm) {
        val words = cluster_to_word_gmm._2.map(word => (word, 1)).groupBy(_._1)
        val reduce = words.map(word => (word._1, word._2.size)).toList.sortWith((_._2 > _._2))
        cluster_to_words_count_gmm(cluster_to_word_gmm._1) = reduce
        println(reduce)
      }

      val cluster_map_gmm = new HashMap[Int, Int]()
      predictions_gmm.collect().foreach { case (cluster_id, doc_id) =>
        if (cluster_map_gmm contains cluster_id) {
          var temp = cluster_map_gmm(cluster_id)
          temp = temp + 1
          cluster_map_gmm(cluster_id) = temp
        }
        else {
          cluster_map_gmm(cluster_id) = 1
        }
      }

      var sorted_cluster_map_gmm = ListMap(cluster_map_gmm.toSeq.sortBy(_._1): _*)
      println(sorted_cluster_map_gmm)
    }
    else{
      println("Enter one among the following for alogrithm")
      println("K for Kmeans")
      println("B for bisecting K means")
      println("G for Guassian Mixture")
    }
  }
}

