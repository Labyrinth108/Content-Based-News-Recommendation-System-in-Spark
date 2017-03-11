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

object Solution_LDA {

  def parse_News(srcRDD: org.apache.spark.rdd.RDD[(String, String)], sc: SparkContext): org.apache.spark.sql.DataFrame = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var corpus_rdd = srcRDD.map { x => x._2 }
    val corpus_df = corpus_rdd.zipWithIndex.toDF("corpus", "id")

    //tokenize and remove stop words
    var tokenizer = new Tokenizer().setInputCol("corpus").setOutputCol("words")
    var wordsData = tokenizer.transform(corpus_df)

    val stopwords = sc.textFile("/home/laura/Documents/News_Stopwords.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopwords).setInputCol("words").setOutputCol("filtered")
    val filtered_df = remover.transform(wordsData)

    return filtered_df
  }

  def LDA_stuff(sourcefile: String, sc: SparkContext): org.apache.spark.rdd.RDD[(String, Vector)] = {

    // (u_id, news)
    var srcRDD = sc.textFile(sourcefile).map { x =>
      val data = x.split(",")
      (data(0), data(1))
    }
    var news_index = srcRDD.map { x => x._1 }

    //preprocess(remove the stop words) in the training dataset
    val filtered_df = parse_News(srcRDD, sc)

    // Set parameters for CountVectorizer
    val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
    //Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")

    //Convert DF to RDD
    val lda_countvector = countVectors.map { case Row(id: Long, countVector: Vector) => (id, countVector) }

    val computeLDAModel = false

    if (computeLDAModel) {

      //LDA begins
      val numTopics = 8

      val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
        .setK(numTopics).setMaxIterations(60).setDocConcentration(-1).setTopicConcentration(-1)

      val ldaModel = lda.run(lda_countvector)

      //evaluate the model
      val localmodel = ldaModel.asInstanceOf[LocalLDAModel]
      val ll = localmodel.logLikelihood(lda_countvector)
      val lp = localmodel.logPerplexity(lda_countvector)
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

      ldaModel.save(sc, s"file:///home/laura/Documents/LDA_Model/LDA_K$numTopics")
    }

    val localmodel = LocalLDAModel.load(sc, "file:///home/laura/Documents/LDA_Model/LDA_K8")

    //compute topic distributions of news in the training data
    //topicProb : (useless normal_news_index from 1 to n, topic representation of news)
    val topicProb = localmodel.topicDistributions(lda_countvector)
    val representaion = topicProb.map(x => x._2)
    //    topicProb.saveAsTextFile(s"file:///home/laura/Documents/topicDistributions_K$numTopics")

    //nid_representation : (nid, topic representation of news)
    val nid_representation = news_index.zip(representaion)

    return nid_representation
  }

  def process_test_news(sourcefile: String, sc: SparkContext, modelfile: String): org.apache.spark.rdd.RDD[(String, Vector)] = {

    var srcRDD = sc.textFile(sourcefile).map { x =>
      val data = x.split(",")
      (data(0), data(1))
    }.cache()

    var news_index = srcRDD.map { x => x._1 }

    //preprocess(remove the stop words) in the test dataset
    val filtered_df = parse_News(srcRDD, sc)

    // Set parameters for CountVectorizer
    val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
    //Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")

    //Convert DF to RDD
    val lda_countvector = countVectors.map { case Row(id: Long, countVector: Vector) => (id, countVector) }

    val localmodel = LocalLDAModel.load(sc, modelfile)

    val topicProb = localmodel.topicDistributions(lda_countvector)
    val representaion = topicProb.map(x => x._2)

    //nid_representation : (nid, topic representation of news)
    val nid_representation = news_index.zip(representaion)

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

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ON)

    val conf = new SparkConf().setMaster("local").setAppName("LDA")
    val sc = new SparkContext(conf)

    //========Compute News representation===================================
    //nid_representation : (nid, topic representation of news)
    val nid_representation = LDA_stuff("/home/laura/Documents/training_data.txt", sc)

    //=========Compute User profile========================================
    //reading record in the training data: (nid,uid)
    val reading_record = sc.textFile("/home/laura/Documents/uid_nid.txt").map { x =>
      var data = x.split(",")
      (data(1), data(0))
    }

    //user_vector : (uid, representation of a piece of news)
    val user_vector = reading_record.join(nid_representation).map(x => x._2)

    //compute user preference vector: (uid, preference vector)
    //compute the average of the representation of reading news as the preference vector
    val user_representation = user_vector.groupByKey().map(f => {
      var avg = Vectors.zeros(8)
      var num = f._2.toArray.length
      for (i <- f._2) {
        val v1 = new DenseVector(i.toArray)
        val v2 = new DenseVector(avg.toArray)
        avg = Vectors.dense((v1 + v2).toArray)
      }
      val v = new DenseVector(avg.toArray)
      val num_list = List.fill(8)(num * 1.0)
      val num_v = new DenseVector(num_list.toVector.toArray)     
      (f._1, Vectors.dense((v :/ num_v).toArray))
    })

    //=============Recommendation===========================================
    //Recommend news to users
    val modelfile = "/home/laura/Documents/LDA_Model/LDA_K8"
    val test_lda_vector = process_test_news("/home/laura/Documents/testing_data.txt", sc, modelfile).cache()
    
    //    test_lda_vector.saveAsTextFile("/home/laura/Documents/test_news_representation")

    //extract the representation of old users from all users 
    val old_user_id = sc.textFile("/home/laura/Documents/old_user_test.txt")
    val old_user_id_bc = sc.broadcast(old_user_id.collect.toSet)
    val old_user_representation = user_representation.filter { case (uid, representation) => old_user_id_bc.value.contains(uid) }
    //    old_user_representation.saveAsTextFile("/home/laura/Documents/old_user_representation")   

    //(user, (n_id, similarity))
    //Wrong implementation which causes an error 
    //named "RDD transformation and actions can only be invoked by the driver, not inside of other transformation"
    //    val user_recommend = old_user_representation.map(x=>{
    //      val user_vector = x._2
    //      val u_n_sim = test_lda_vector.map(x => (x._1, cosineSimilarity(user_vector, x._2)))  
    //      (x._1, u_n_sim)
    //    })

    //a way to compute cosine similarity between item vectors and user_preference but it costs too much.
    //a simple implementation waiting for improvements such as clustering news

//    val test_lda_vector_sample = sc.parallelize(test_lda_vector.take(10000))
    val sim_result = old_user_representation.cartesian(test_lda_vector).map { 
      case ((uid, u_vector), (nid, n_vector)) => (uid, (nid, cosineSimilarity(u_vector, n_vector))) }
    //    sim_result.saveAsTextFile("/home/laura/Documents/testla")

    //(uid, List(nid, n_sim))
    val topK = 20
    val recommend_news = sim_result.groupByKey().map {
      case (uid, nid_sim) =>
        (uid, nid_sim.toList.sortWith(_._2 > _._2).take(topK))
    }
    

    val recommend_news_flatten = recommend_news.flatMap({case (uid, n_set) => n_set.map(uid-> _._1)})
    val recommend_news_id = recommend_news_flatten.groupByKey()
    
//    recommend_news_flatten.saveAsTextFile("/home/laura/Documents/recommend_news_flatten_2")
    
    //(uid, Iterable[nid])
    val test_news_labels = sc.textFile("/home/laura/Documents/old_user_records.txt").map { x =>
      var data = x.split(",")
      (data(0), data(1))
    }.groupByKey()
    

    val recommend_result = recommend_news_id.join(test_news_labels).map{case (uid, (pred, labels))=> 
      val labels_ = labels.toArray
      val pred_ = pred.toArray
      val tp = pred_.intersect(labels_).length
      val min_v = Math.min(topK, labels_.length)
      var correct = 0
      
      if( tp > min_v / 2)
         correct = 1
      
      (uid, correct)
    }
    
    val results = recommend_result.values.sum()
    println(results)
  }
}