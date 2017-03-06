import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.{ Vectors, SparseVector }
import org.apache.spark.sql.Row

object TFIDF_News {
  
  def tokenize(content:String):Seq[String] = {
    
    content.split("\\s+").toSeq
  }
  
  def getTFIDF_vector(sourcefile: String, sc: SparkContext): org.apache.spark.rdd.RDD[(String , Seq[(String, Double)])] ={
        // (u_id, news)
    var srcRDD = sc.textFile(sourcefile).map { x =>
      val data = x.split(",")
      (data(0), data(1))
    }
    
    val srcrdd_news = srcRDD.map(_._2).map(tokenize)
    val srcrdd_index = srcRDD.map(_._1)
    
    //=========TF-IDF to compute the key words of news=============
    //hash-tf
    val hashingTF = new HashingTF()
    val mapWords = srcrdd_news.flatMap { x => x }.map ( w => (hashingTF.indexOf(w), w) ).collect.toMap
    val featurizedData = hashingTF.transform(srcrdd_news)
    featurizedData.cache()
    val bcWords = featurizedData.context.broadcast(mapWords)
    
    //idf
    val idf = new IDF(2)
    val idfmodel = idf.fit(featurizedData)
    
    val rescaledData = idfmodel.transform(featurizedData)
    
    val newsrdd = rescaledData.map{case SparseVector(size, indices, values)=> 
     val words = indices.map ( index => bcWords.value.getOrElse(index, "null")) 
       words.zip(values).sortBy(-_._2).take(10).toSeq
    }
    
//    srcrdd_index.zip(newsrdd).saveAsTextFile("/home/laura/Documents/TFIDF/tfidf.txt")
    val news_key_words = srcrdd_index.zip(newsrdd) //(nid,key_words)
    return news_key_words
  }
  
  def JacarrdSimilarity(uwords:List[String], nwords: List[String]): Double = {
    return uwords.intersect(nwords).length.toDouble/uwords.length.toDouble
  }
  
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("LDA").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sourcefile = "/home/laura/Documents/1training_data.txt"
    
    val training_news_key_words = getTFIDF_vector(sourcefile, sc)
    
    //===========Load user reading history=========================
    //reading record in the training data: (nid,uid)
    val reading_record = sc.textFile("/home/laura/Documents/uid_nid.txt").map { x =>
      var data = x.split(",")
      (data(1), data(0))
    }

    //user_vector : (uid, key words of a piece of news)
    val user_vector = reading_record.join(training_news_key_words).map{x=>x._2}.groupByKey().map{
      case (uid, words_v) =>
        (uid, words_v.flatten)
      }
    
    val user_keyword = user_vector.map{case (uid, words) => 
        (uid, words.toList.sortBy(-_._2).take(10))
    }
    
//    user_keyword.saveAsTextFile("/home/laura/Documents/TFIDF/User_keywords")
    
    val old_user_id = sc.textFile("/home/laura/Documents/old_user_test.txt")
    val old_user_id_bc = sc.broadcast(old_user_id.collect.toSet)
    val old_user_representation = user_keyword.filter { case (uid, representation) => old_user_id_bc.value.contains(uid) }
    
    val old_user_keywords = old_user_representation.map{case (uid, representation) => (uid, representation.map(x=>x._1))}.cache()
//    old_user_keywords.saveAsTextFile("/home/laura/Documents/TFIDF/user_keywords")
    val test_news_keywords = getTFIDF_vector("/home/laura/Documents/testing_data.txt", sc).map{case(nid, representation) => (nid, representation.map(x=>x._1))}.cache()
    
    //================================Begin Recommend==================================================
    val sim_result = old_user_keywords.cartesian(test_news_keywords).map { 
      case ((uid, uwords), (nid, nwords)) => 
        val sim = uwords.intersect(nwords).length.toDouble/uwords.length.toDouble
        (uid, (nid, sim))}     
//    sim_result.saveAsTextFile("/home/laura/Documents/TFIDF/sim_result")
    
    //(uid, List(nid, n_sim))
    val topK = 10
    val recommend_news = sim_result.groupByKey().map {
      case (uid, nid_sim) =>
        (uid, nid_sim.toList.sortWith(_._2 > _._2).take(topK).filter(x => x._2 > 0))
    }
    
    val recommend_news_flatten = recommend_news.flatMap({case (uid, n_set) => n_set.map(uid-> _._1)})
    val recommend_news_id = recommend_news_flatten.groupByKey()
//    recommend_news.saveAsTextFile("/home/laura/Documents/TFIDF/result")
    
    //(uid, Iterable[nid])
    val test_news_labels = sc.textFile("/home/laura/Documents/old_user_records.txt").map { x =>
      var data = x.split(",")
      (data(0), data(1))
    }.groupByKey()
    

    val recommend_result = recommend_news_id.join(test_news_labels).map{case (uid, (pred, labels))=> 
      val labels_ = labels.toArray
      val pred_ = pred.toArray
      val tp = pred_.intersect(labels_).length
      val min_v = Math.min(pred_.length, labels_.length)
      var correct = 0
//      if(tp >= min_v/2)
      if(tp > 0)
        correct = 1
      (uid, correct)
    }
   
    recommend_result.saveAsTextFile("/home/laura/Documents/TFIDF/resommend")
    val results_news = recommend_result.values.sum()
    println(results_news)
   
  }
}