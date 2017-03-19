import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
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
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import breeze.linalg.DenseVector
import breeze.numerics._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.log4j.{ Level, Logger }
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import java.util.ArrayList
import scala.collection.JavaConverters
import scala.util.control.Breaks._

object LRRecommend {
  
  val dir = "/home/laura/Documents/"
  val news_training_file = dir + "data/training_data.txt"
  val news_testing_file = dir + "data/testing_data.txt"
  val negative_file = dir + "NegativeSamples.txt"
  val record_training_file = dir + "data/training_uid_nid.txt"
  val record_testing_file = dir + "data/test_old_user_records.txt"
  val olduser_id_test_file = "/home/laura/Documents/data/test_old_user_id.txt"
  val stopwords_file = dir + "News_Stopwords.txt"
  
  val numTopics = 4
  val topK = 5
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

    def EclSimilarity(v1: Vector, v2: Vector) = {
    
    var result = 0.0
    var norm1 = 0.0
    var norm2 = 0.0
    var index = v1.size - 1

    for (i <- 0 to index) {
      result += (v1(i) - v2(i))*(v1(i) - v2(i))
    }
    math.sqrt(result) 
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
  
   def getNextDayTimestamp(ts: String): String ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.format(new Date(ts.toLong * 1000))
    
    val neardate = date.split(" ")(0) 
    val sdf_new = new SimpleDateFormat("yyyy-MM-dd")
    
    return String.valueOf(sdf_new.parse(neardate).getTime / 1000 + 24 * 3600)
  }
  
   def getPrenewsIndex(ee: List[Object], target:Double): Int = {
        var prenews_index = -1
        var low = 0
        var high = ee.length - 1
        var done = false
        
        while(low <= high && !done){
          
          val mid = (high + low) / 2
          val pair = ee(mid).asInstanceOf[(String, String)]
          if(pair._2.toDouble == target){
            prenews_index = mid - 1
            done = true
          }
            
          if(pair._2.toDouble < target)
            low = mid + 1
          else
            high = mid - 1
        }
        
        if(prenews_index == -1){
          if(low == 0)
            prenews_index = low
          else
            prenews_index = low - 1
        }
        return prenews_index
   }
   
   def getFeatures(data: org.apache.spark.rdd.RDD[(String, String, String)], record_time_info:  scala.collection.Map[String, List[(String, String)]],
     pair_num_map:  scala.collection.Map[(String, String), Int],
     u_vector_map:  scala.collection.Map[String, Vector],
     n_vector_map:  scala.collection.Map[String, Vector],
     pubt_map :  scala.collection.Map[String, String],
     longPop :  scala.collection.Map[String, Int],
     shortPop:  scala.collection.Map[(String, String), Int],
     label:Int
   ) : org.apache.spark.rdd.RDD[LabeledPoint] = {
     
     val res = data.map{
      case(nid, uid, read_time)=>
        val default_v = List.fill(numTopics)(1.0 / numTopics).toVector
        val uvector = u_vector_map.getOrElse(uid, Vectors.dense(default_v.toArray))
        val nvector = n_vector_map.getOrElse(nid, Vectors.dense(default_v.toArray))
        val unsim= EclSimilarity(uvector, nvector)
        
        val tmp = new ArrayList[(String, String)]
        val reading_history = record_time_info.getOrElse(uid, tmp.toArray().toList)

        val prenews_index = getPrenewsIndex(reading_history, read_time.toDouble)
        var tran_prob = 0.0
//        var sim_pre_now = 0.0

        if(prenews_index < reading_history.length){
           val prenews = reading_history(prenews_index).asInstanceOf[(String, String)]._1
           val prenews_rtime = reading_history(prenews_index).asInstanceOf[(String, String)]._2
           val ntrans = pair_num_map.getOrElse((prenews, nid), 0) 
           val nd = longPop.getOrElse(prenews, 0)
           if(nd != 0)
             tran_prob = ntrans.toDouble / nd.toDouble
             
//           val n_pre_v = n_vector_map.getOrElse(prenews, Vectors.dense(default_v.toArray))
//           sim_pre_now = EclSimilarity(nvector, n_pre_v)
        }
        
        val news_freshness = (read_time.toDouble - pubt_map.getOrElse(nid, "0").toDouble) / 3600.0
        val long_popularity = longPop.getOrElse(nid, 0)
        val short_popularity = shortPop.getOrElse((nid, getPreviousDayTimestamp(read_time)), 0)
        
        LabeledPoint(label, Vectors.dense(long_popularity, short_popularity, unsim, news_freshness, tran_prob))
    }
     return res
   }
    
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ON)

    val conf = new SparkConf().setMaster("local").setAppName("LDA").set("spark.local.dir", "/tmp/spark-tmp")
    val sc = new SparkContext(conf)

    // News data set : (n_id, news, pub_time)
    val srcRDD = sc.textFile(news_training_file).map { x =>
      val data = x.split(",")
      (data(0), data(1), data(2))
    }.cache()
    
    val news_pubt_map = srcRDD.map(x=>(x._1, x._3)).collectAsMap()
    val news_content = srcRDD.map(x=>(x._1, x._2))
    
    //nid_representation : (nid, news_vectors)
    val nid_representation = LDA_stuff(news_content, sc)
    val n_vector_map = nid_representation.collectAsMap()
    
    //reading records in the training data: (nid, uid, read_time)
    val record_rdd = sc.textFile(record_training_file).map { x =>
      val data = x.split(",")
      (data(1), data(0), data(2))
    }
    
    val longPop_training = record_rdd.map{x=>(x._1, 1)}.reduceByKey(_+_).collectAsMap() // (nid, pop_value)
    val shortPop_training = record_rdd.map{x=>( (x._1, getPreviousDayTimestamp(x._3)),  1)}.reduceByKey(_+_).collectAsMap() //((nid, pre_time), pop_value)
    
    //read_record: (nid, uid)
    val reading_record = record_rdd.map{case (x,y,z) => (x,y)}
    //user_vector : (uid, representation of a piece of news)
    val user_vector = reading_record.join(nid_representation).map(x => x._2)

    //generate news context pairs
    val records = record_rdd.map(x=> (x._2, (x._1, x._3))).groupByKey()
    
    val records_pairs = records.map{
      case(uid, news) =>

        val news_sort = news.toList.sortWith(_._2.toFloat < _._2.toFloat)
        var pre = news_sort(0)._1
        var pairs = new ArrayList[String]
        val length = news_sort.length
        
        for(i <- 1 to length - 1){
                  pairs.add(i-1, pre + "_" + news_sort(i)._1)
                  pre = news_sort(i)._1
        }
        (uid, pairs.toArray().map { x => x.toString() })
    }
    
    val record_time_info = records.map{
      case(uid, news) =>
         (uid,  news.toList.sortWith(_._2.toFloat < _._2.toFloat))
    }.collectAsMap()
      
    val pair_num_map = records_pairs.flatMap { x => x._2 }.map { x => 
      val pair = x.toString().split("_")
      ( (pair(0), pair(1)),1)
    }.reduceByKey(_+_).collectAsMap()
    
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
    
    val user_vector_map = user_representation.collectAsMap()
    
    //Use logistic regression model
    //Training
    val training_pos = getFeatures(record_rdd, record_time_info, pair_num_map, user_vector_map, n_vector_map, 
        news_pubt_map,longPop_training, shortPop_training, 1)
 
    val negative_samples = sc.textFile(negative_file).map { x => 
      val data = x.split(",") 
      (data(1), data(0), data(2))  //(nid, uid, readtime)
    }
    val training_neg = getFeatures(negative_samples, record_time_info, pair_num_map, user_vector_map, n_vector_map, 
        news_pubt_map,longPop_training, shortPop_training, 0)
    val training_dataset = training_pos.union(training_neg)
    val normalizer = new Normalizer()
    val training_normalized = training_dataset.map { x => x.label }.zip(normalizer.transform(training_dataset.map { x => x.features })).
    map{
      case (label, features) => LabeledPoint(label, features)
    }

    //Test News modeling
    //(nid, content, pub_time)
    val test_rdd = sc.textFile(news_testing_file).map { x =>
      val data = x.split(",")
      (data(0), data(1), data(2))
    }.cache()
    
    //<nid, <vector, pub_time>>
    val test_lda_vector = process_test_news(test_rdd, sc).cache()
    val test_news_map = test_lda_vector.map(x=>(x._1, x._2._1)).collectAsMap()
    val test_news_pubt_map = test_lda_vector.map(x=>(x._1, x._2._2)).collectAsMap()
    
    // (uid, nid, read_time)
    val test_news_record = sc.textFile(record_testing_file).map { x =>
      val data = x.split(",")
      (data(0), data(1), data(2))
    }.cache()
    val test_labels = test_news_record.map(x=>((x._1, x._3), x._2))//)((nid, read_time), nid)
    val test_u_time = test_news_record.map(x => (x._1, x._3)) //(nid, read_time)
    
    val longPop_testing = test_news_record.map{x=>(x._2, 1)}.reduceByKey(_+_).collectAsMap() // (nid, pop_value)
    val shortPop_testing = test_news_record.map{x=>( (x._2, getPreviousDayTimestamp(x._3)),  1)}.reduceByKey(_+_).collectAsMap() //((nid, pre_time), pop_value)

    //Recommend
    val time_gap = 5*3600*24
    val recommend_index = test_u_time.cartesian(test_rdd.map(x => (x._1, x._3))).filter{case(x,y ) => x._2.toFloat > y._2.toFloat && x._2.toFloat - y._2.toFloat < time_gap}
    .map{case ((uid, read_time), (candi_nid, pub_time)) => ( candi_nid, uid, read_time)}.cache()
    
    val records_t = test_news_record.map(x=> (x._1, (x._2, x._3))).groupByKey()
    
    val records_pairs_t = records_t.map{
      case(uid, news) =>

        val news_sort = news.toList.sortWith(_._2.toFloat < _._2.toFloat)
        var pre = news_sort(0)._1
        var pairs = new ArrayList[String]
        val length = news_sort.length
        
        for(i <- 1 to length - 1){
                  pairs.add(i-1, pre + "_" + news_sort(i)._1)
                  pre = news_sort(i)._1
        }
        (uid, pairs.toArray().map { x => x.toString() })
    }
    
    val record_time_info_t = records_t.map{
      case(uid, news) =>
         (uid,  news.toList.sortWith(_._2.toFloat < _._2.toFloat))
    }.collectAsMap()
      
    val pair_num_map_t = records_pairs_t.flatMap { x => x._2 }.map { x => 
      val pair = x.toString().split("_")
      ( (pair(0), pair(1)),1)
    }.reduceByKey(_+_).collectAsMap()
    
    val test_data = getFeatures(recommend_index, record_time_info_t, pair_num_map_t, user_vector_map, test_news_map, 
        test_news_pubt_map,longPop_testing, shortPop_testing, 0)
    
    val test_normalized = test_data.map { x => x.label }.zip(normalizer.transform(test_data.map { x => x.features })).map{
      case (label, features) => LabeledPoint(label, features)
    }

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val training_df = training_normalized.map { case LabeledPoint(label, features) =>(label, features)}.toDF("label", "features")
    val test_df = test_normalized.map { case LabeledPoint(label, features) =>(label, features)}.toDF("label", "features")
    
    //    val model = LogisticRegressionWithSGD.train(dataset, 10000, 0.00000001)
    val model = new LogisticRegression().setMaxIter(10000).setRegParam(0.01).setElasticNetParam(0.00000001)
    val model2 = model.fit(training_df)
    
    val res = model2.transform(test_df)
    val res_prob = res.select("probability").map { case Row(x) => 
      val d = x.asInstanceOf[Vector] 
      d(1)   
   }
    val recommend_res = recommend_index.zip(res_prob).map{case ((candi_nid, uid, read_time), prob) => ((uid, read_time), (candi_nid, prob))}
    
    //sort to get the recommended news
    val recommend_news_id = recommend_res.groupByKey().map{ case ((uid, rtime), candidates) 
      =>
     ((uid,rtime), candidates.toList.sortWith(_._2 > _._2).take(topK))
    }.cache()

    
    val res_evaluation = recommend_news_id.join(test_labels).map{case ((uid, r_time), (candidates, label)) =>
      var correct = 0
      for (c <- candidates){
        if(c._1 == label)
          correct = 1
      }
      ((uid, r_time, label), correct)
    }
    
    val conum = res_evaluation.values.sum()
    val precision = conum / res_evaluation.count()
    println(conum)
    println(precision)
  }
  
}