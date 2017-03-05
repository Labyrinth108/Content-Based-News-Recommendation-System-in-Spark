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
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("LDA").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sourcefile = "/home/laura/Documents/1training_data.txt"
    
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
    
    //===========Load user reading history=========================
    //reading record in the training data: (nid,uid)
    val reading_record = sc.textFile("/home/laura/Documents/uid_nid.txt").map { x =>
      var data = x.split(",")
      (data(1), data(0))
    }

    //user_vector : (uid, key words of a piece of news)
    val user_vector = reading_record.join(news_key_words).map{x=>x._2}.groupByKey().map{
      case (uid, words_v) =>
        (uid, words_v.flatten)
      }
    
    val user_keyword = user_vector.map{case (uid, words) => 
        (uid, words.toList.sortBy(-_._2).take(10))
    }
    
    user_keyword.saveAsTextFile("/home/laura/Documents/TFIDF/User_keywords")
  }
}