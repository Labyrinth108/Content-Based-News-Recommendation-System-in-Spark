import scala.reflect.runtime.universe
 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering.{LDA, LDAModel,OnlineLDAOptimizer,LocalLDAModel}

import org.apache.spark.ml.feature.StopWordsRemover 
import org.apache.log4j.{Level, Logger}

object Solution {
  def main(args : Array[String]) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.ALL)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ON)

    val conf = new SparkConf().setMaster("local").setAppName("TestLDA")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    var srcRDD = sc.textFile("/home/laura/Documents/news.txt").map { x => x    }
    val corpus_df = srcRDD.zipWithIndex.toDF("corpus", "id")
   
    
    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("corpus").setOutputCol("words")
    var wordsData = tokenizer.transform(corpus_df)
    
    val stopwords = sc.textFile("/home/laura/Documents/News_Stopwords.txt").collect()
    val remover = new StopWordsRemover().setStopWords(stopwords).setInputCol("words").setOutputCol("filtered")
    val filtered_df = remover.transform(wordsData)
    
   // Set parameters for CountVectorizer
    val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
   //Create vector of token counts
    val countVectors = vectorizer.transform(filtered_df).select("id", "features")
    
    //Convert DF to RDD
    val lda_countvector = countVectors.map { case Row(id:Long, countVector:Vector) => (id, countVector) }
    
    //LDA begins
    val numTopics = 5

    val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
    .setK(numTopics).setMaxIterations(10).setDocConcentration(-1).setTopicConcentration(-1)
    
    println(s"Dont scare me.$numTopics")
    
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
    val topics = topicIndices.map{case(terms, termWeights) => 
      terms.map(vocabList(_)).zip(termWeights)}
    
    println(s"$numTopics topics:")
    topics.zipWithIndex.foreach{ case(topic, i) =>
      println(s"TOPIC: $i")
      topic.foreach{case (term,weight) => println(s"$term\t$weight")}
      println(s"=====================")     
    }
    
    ldaModel.save(sc, "target/LDA")
    
    //save topic distributions of the training data
    
    val topicProb = localmodel.topicDistributions(lda_countvector)
 
     topicProb.take(10).foreach(println)

    topicProb.saveAsTextFile("file:///home/laura/Documents/topicDistributions.txt")
  }
}