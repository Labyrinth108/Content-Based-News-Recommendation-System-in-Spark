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

object Solution {
  
  def parse_News(srcRDD: org.apache.spark.rdd.RDD[(String, String)], sc: SparkContext):org.apache.spark.sql.DataFrame ={
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
  
  def LDA_stuff(sourcefile: String, sc: SparkContext):org.apache.spark.rdd.RDD[(String, Vector)]={

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
      
//    val computeLDAModel = false
//    
//    if (computeLDAModel) {
      
      //LDA begins
      val numTopics = 5

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

      ldaModel.save(sc, s"file:///home/laura/Documents/LDA_K$numTopics")
//    }
 
    //compute topic distributions of news in the training data
    //topicProb : (useless normal_news_index from 1 to n, topic representation of news)
    val topicProb = localmodel.topicDistributions(lda_countvector)
    val representaion = topicProb.map(x => x._2)
    //    topicProb.saveAsTextFile(s"file:///home/laura/Documents/topicDistributions_K$numTopics")

    //nid_representation : (nid, topic representation of news)
    val nid_representation = news_index.zip(representaion)
    
    return nid_representation
  }

  def process_test_news(sourcefile: String, sc: SparkContext):org.apache.spark.rdd.RDD[(String, Vector)]={
    
    var srcRDD = sc.textFile(sourcefile).map { x =>
      val data = x.split(",")
      (data(0), data(1))
    }
    var news_index = srcRDD.map { x => x._1 }     
    
    //preprocess(remove the stop words) in the test dataset
    val filtered_df = parse_News(srcRDD, sc)
      
    // Set parameters for CountVectorizer
    val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
    //Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")

    //Convert DF to RDD
    val lda_countvector = countVectors.map { case Row(id: Long, countVector: Vector) => (id, countVector) }
    
    val localmodel = LocalLDAModel.load(sc, "file:///home/laura/Documents/LDA_K5")
    
    val topicProb = localmodel.topicDistributions(lda_countvector)
    val representaion = topicProb.map(x => x._2)
    
    //nid_representation : (nid, topic representation of news)
    val nid_representation = news_index.zip(representaion)
    return nid_representation
  }
  
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ON)

    val conf = new SparkConf().setMaster("local").setAppName("LDA")
    val sc = new SparkContext(conf)
    
    //nid_representation : (nid, topic representation of news)
    val nid_representation = LDA_stuff("/home/laura/Documents/training_data.txt", sc)

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
      var avg = Vectors.dense(0, 0, 0, 0, 0)
      var num = 1
      for (i <- f._2) {
        val v1 = new DenseVector(i.toArray)
        val v2 = new DenseVector(avg.toArray)
        avg = Vectors.dense((v1 + v2).toArray)
        num = num + 1
      }
      val v = new DenseVector(avg.toArray)
      val num_v = new DenseVector(Vectors.dense(num, num, num, num, num).toArray)
      (f._1, Vectors.dense((v :/ num_v).toArray))
    })

    user_representation.saveAsObjectFile("/home/laura/Documents/user_representation")

    //Recommend news to users
    val old_user_id = sc.textFile("/home/laura/Documents/old_user_test.txt")  
    //    user_representation.filter(x=> x._1 )
    
    val test_lda_vector = process_test_news("/home/laura/Documents/testing_data.txt", sc)
    test_lda_vector.saveAsTextFile("/home/laura/Documents/test_news_representation")
    
  }
}