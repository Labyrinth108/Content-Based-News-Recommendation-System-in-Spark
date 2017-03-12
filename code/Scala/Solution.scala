import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.{ Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering.{ LDA, LDAModel, OnlineLDAOptimizer, LocalLDAModel }
import breeze.linalg.DenseVector
import breeze.numerics._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.log4j.{ Level, Logger }
import java.util.Date
import java.text.SimpleDateFormat

object Solution {
  val dir = "/home/laura/Documents/"
  val news_training_file = dir + "data/training_data.txt"
  val news_testing_file = dir + "data/testing_data.txt"
  val record_training_file = dir + "data/training_uid_nid.txt"
  val record_testing_file = dir + "data/test_old_user_records.txt"
  val olduser_id_test_file = "/home/laura/Documents/data/test_old_user_id.txt"
  val stopwords_file = dir + "News_Stopwords.txt"
  
  val numTopics = 4
  val model_file = dir + s"LDA_Model/LDA_K$numTopics"
  
  def parse_News(corpus_rdd: org.apache.spark.rdd.RDD[ String], sc: SparkContext): org.apache.spark.sql.DataFrame = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val corpus_df = corpus_rdd.zipWithIndex.toDF("corpus", "id")

    //tokenize and remove stop words
    val tokenizer = new Tokenizer().setInputCol("corpus").setOutputCol("words")
    val wordsData = tokenizer.transform(corpus_df)

    val stopwords = sc.textFile(stopwords_file).collect()
    val remover = new StopWordsRemover().setStopWords(stopwords).setInputCol("words").setOutputCol("filtered")
    val filtered_df = remover.transform(wordsData)

    return filtered_df
  }

  def LDA_stuff(srcRDD: org.apache.spark.rdd.RDD[(String,  String)], sc: SparkContext): org.apache.spark.rdd.RDD[(String, Vector)] = {

    val news_index = srcRDD.map { x => x._1 }
    val corpus_rdd = srcRDD.map { x => x._2 }
    
    //preprocess(remove the stop words) in the training dataset
    val filtered_df = parse_News(corpus_rdd, sc)

    // Set parameters for CountVectorizer
    val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
    //Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")

    //Convert DF to RDD
    val countvector_rdd = countVectors.map { case Row(id: Long, countVector: Vector) => (id, countVector) }

    val computeLDAModel = false
    var localmodel: org.apache.spark.mllib.clustering.LocalLDAModel = null
    
    if (computeLDAModel) {

      //LDA begins

      val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
        .setK(numTopics).setMaxIterations(60).setDocConcentration(-1).setTopicConcentration(-1)

      val ldaModel = lda.run(countvector_rdd)

      //evaluate the model
      localmodel = ldaModel.asInstanceOf[LocalLDAModel]
      val ll = localmodel.logLikelihood(countvector_rdd)
      val lp = localmodel.logPerplexity(countvector_rdd)
      println(s"The lower bound on the log likelihood of the entire corpus: $ll")
      println(s"The upper bound on perplexity: $lp")

      //describe the model
      val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
      val vocabList = vectorizer.vocabulary
      val topics = topicIndices.map {
        case (terms, termWeights) =>
          terms.map(vocabList(_)).zip(termWeights)
      }

      topics.zipWithIndex.foreach {
        case (topic, i) =>
          println(s"TOPIC: $i")
          topic.foreach { case (term, weight) => println(s"$term\t$weight") }
          println(s"=====================")
      }
      
      ldaModel.save(sc, model_file)
    }
    else{
      
      localmodel = LocalLDAModel.load(sc, model_file)
    }
    
    //compute topic distributions of news in the training data
    //topicProb : (useless normal_news_index from 1 to n, topic representation of news)
    val topicProb = localmodel.topicDistributions(countvector_rdd)
    val representaion = topicProb.map(x => x._2)
    //    topicProb.saveAsTextFile(s"file:///home/laura/Documents/topicDistributions_K$numTopics")

    //nid_representation : (nid, topic representation of news)
    val nid_representation = news_index.zip(representaion)

    return nid_representation
  }

  def process_test_news(srcRDD: org.apache.spark.rdd.RDD[(String,  String, String)], sc: SparkContext): org.apache.spark.rdd.RDD[(String,( Vector, String))] = {

    val news_index = srcRDD.map { x => x._1 }
    val corpus_rdd = srcRDD.map { x => x._2 }
    val news_pub_time = srcRDD.map{ x => x._3 }

    // preprocess (remove the stop words) in the test data set
    val filtered_df = parse_News(corpus_rdd, sc)

    // Set parameters for CountVectorizer
    val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
    //Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")

    //Convert DF to RDD
    val countvector_rdd = countVectors.map { case Row(id: Long, countVector: Vector) => (id, countVector) }

    val localmodel = LocalLDAModel.load(sc, model_file)

    val topicProb = localmodel.topicDistributions(countvector_rdd)
    val representaion = topicProb.map(x => x._2)

    //nid_representation : (nid, topic representation of news)
    val nid_representation = news_index.zip(representaion.zip(news_pub_time))

    return nid_representation
  }

  def cosineSimilarity(v1: Vector, v2: Vector) = {

    var result = 0.0
    var norm1 = 0.0
    var norm2 = 0.0
    var index = v1.size - 1

    for (i <- 0 to index) {
      result += v1(i) * v2(i)
      norm1 += Math.pow(v1(i), 2)
      norm2 += Math.pow(v2(i), 2)
    }
    result / (Math.sqrt(norm1) * Math.sqrt(norm2))
  }

  def getNearDayTimestamp(ts: String): String ={
    
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.format(new Date(ts.toLong * 1000))
    
    val neardate = date.split(" ")(0) 
    val sdf_new = new SimpleDateFormat("yyyy-MM-dd")
    
    return String.valueOf(sdf_new.parse(neardate).getTime / 1000)
  }
  
  
  def getPreviousDayTimestamp(ts: String): String ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.format(new Date(ts.toLong * 1000))
    
    val neardate = date.split(" ")(0) 
    val sdf_new = new SimpleDateFormat("yyyy-MM-dd")
    
    return String.valueOf(sdf_new.parse(neardate).getTime / 1000 - 24 * 3600)
  }
  
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ON)

    val conf = new SparkConf().setMaster("local").setAppName("LDA")
    val sc = new SparkContext(conf)

    // News data set : (n_id, news, pub_time), but srcRDD(nid, news_content)
    val srcRDD = sc.textFile(news_training_file).map { x =>
      val data = x.split(",")
      (data(0), data(1))
    }.cache()
    
    //nid_representation : (nid, topic representation of news)
    val nid_representation = LDA_stuff(srcRDD, sc)

    //reading records in the training data: (nid,uid, read_time)
    val record_rdd = sc.textFile(record_training_file).map { x =>
      val data = x.split(",")
      (data(1), data(0), data(2))
    }
    
    //read_record: (nid, uid)
    val reading_record = record_rdd.map{case (x,y,z) => (x,y)}
    //user_vector : (uid, representation of a piece of news)
    val user_vector = reading_record.join(nid_representation).map(x => x._2)

    //compute user preference vector: (uid, preference vector)
    //compute the average of the representation of reading news as the preference vector
    val user_representation = user_vector.groupByKey().map{case (uid, vectors) => {
      var sum_v = Vectors.zeros(numTopics)
      var num = vectors.toArray.length
      
      //compute sum of vectors
      for (i <- vectors) {
        val v1 = new DenseVector(i.toArray)
        val v2 = new DenseVector(sum_v.toArray)
        sum_v = Vectors.dense((v1 + v2).toArray)
      }
      
      val sum_dv = new DenseVector(sum_v.toArray)
      
      val num_v = List.fill(numTopics)(num * 1.0).toVector
      val num_dv = new DenseVector(num_v.toArray)     
      
      (uid, Vectors.dense((sum_dv :/ num_dv).toArray))
    }}

    //Recommend news to users
    val test_rdd = sc.textFile(news_testing_file).map { x =>
      val data = x.split(",")
      (data(0), data(1), data(2))
    }.cache()

    //<nid, <vector, pub_time>>
    val test_lda_vector = process_test_news(test_rdd, sc).cache()
    //    test_lda_vector.saveAsTextFile("/home/laura/Documents/test_news_representation")

    //extract the representation of old users from all users 
    val old_user_id = sc.textFile(olduser_id_test_file)
    val old_user_id_bc = sc.broadcast(old_user_id.collect.toSet)
    val old_user_representation = user_representation.filter { case (uid, representation) => old_user_id_bc.value.contains(uid) }
    // old_user_representation.saveAsTextFile("/home/laura/Documents/old_user_representation")   
   
    //(uid, nid, read_time)
    val test_news_rdd = sc.textFile(record_testing_file).map { x =>
      val data = x.split(",")
      (data(0), data(1), data(2))
    }
    
    val test_news_labels = test_news_rdd.map{case (x,y,z) => ((x,z),y)} //(uid, (read_time, nid))
    val test_uid_time = test_news_rdd.map{x => (x._1, x._3)}  //(uid, read_time)
    
    //(read_time, Array(nid, num))
    val hot_news_rdd = test_news_rdd.map{x => (getNearDayTimestamp(x._3), x._2)}.map(x => (x._1, (x._2, 1))).groupByKey().map{ case (time, news)
      =>
        val news_count = news.groupBy(_._1).map{case (x,y) => y.reduce((a,b) =>(a._1, a._2 + b._2))}.toList.sortWith(_._2 > _._2).take(10)
        (time, news_count)
    }
   
//    hot_news_rdd.saveAsTextFile(dir + "hot_news")
    val hot_news_evaluation = test_news_rdd.map(x => (getPreviousDayTimestamp(x._3), (x._1, x._2))).leftOuterJoin(hot_news_rdd).map{
      case( read_time, ((uid, nid), Some(pred)))=>
        var correct = 0
        
        for (pair <- pred){
          if(pair._1 == nid)
            correct = 1
        }
        ((uid, read_time), correct)
        
      case( read_time, ((uid, nid), None)) =>
         ((uid, read_time), 0)
    }
    
    val num = hot_news_evaluation.count()
    val results = hot_news_evaluation.values.sum()
    println(results)
    println(num)
    
//    //compute score of every news according to the user's feature vector and read_time
//    val scorerdd = test_uid_time.join(old_user_representation).cartesian(test_lda_vector).filter{case(x,y)=> x._2._1 > y._2._2} //filter and remain the news which published before the time user reads
//    .map{
//      case( (uid, (r_time, u_vector)), (nid, (n_vector, pub_time))) // RDD[((String, (String, Vector)), (String, (Vector, String)))]
//      => 
//        val content_sim = cosineSimilarity(u_vector, n_vector)
//        val hours_passed = ( r_time.toFloat- pub_time.toFloat) / 3600
//        val day_passed = hours_passed / 24
//        var score = 0.0
//        val norm = 24 * 5
//        var time_diff = norm.toFloat
//        
//        if (day_passed < 5){
//          time_diff = hours_passed / norm
//          score =  content_sim - time_diff
//        }
//         ((uid, r_time), (nid,  score))
//         
//    }.filter{case ((uid, r_time), (nid,  score)) => score > 0}
//    
////    scorerdd.saveAsTextFile("/home/laura/Documents/Testcosine")
//    
//    val topK = 10
//    //sort to get the recommended news
//    val recommend_news_id = scorerdd.groupByKey().map{ case ((uid, rtime), candidates) // RDD[((String, (String, Vector)), (String, (Vector, String)))]
//      =>
//      ((uid,rtime), candidates.toList.sortWith(_._2 > _._2).take(topK))
//    }
////    recommend_news_id.saveAsTextFile("/home/laura/Documents/recommend")
//    
//    //evaluate the recommendation according to the labels
//    val recommend_evaluation = recommend_news_id.join(test_news_labels).map{case ((uid,rtime), (pred, label))=> 
//      val pred_ = pred.toArray
//      var correct = 0
//      
//      for (pair <- pred){
//        if(pair._1 == label)
//          correct = 1
//      }
//      ((uid, rtime), correct)
//    }
//    val num = recommend_evaluation.count()
//    val results = recommend_evaluation.values.sum()
//    println(results)
//    println(num)
  }
  
}