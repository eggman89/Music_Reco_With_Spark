import breeze.numerics.abs

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, Row, DataFrameStatFunctions}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint

object tagGenerator2 {
  def main(args: Array[String]){
    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)


    println("Select a Method to classify songs")
    println("1: Random Forest; 2:Logistic Regression With LBFGS; 3:Decision Trees;  4:Naive Bayes ")
    val method = readInt()
    //load tags and tag ids and attributes
    val map_tagid_tag0 = new hashmap()
    val map_tagid_tag = new hashmap()
    val schema_string = "track_id1 tag_id"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "tag_id") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )
    val tid_trackid = sc.textFile("D:/Project/FinalDataset/track_id_to_tag.txt").map(_.split("\t")).map(p => Row(p(0), map_tagid_tag0.add(p(1))))
    val df_track_id_tag_id = sqlContext.createDataFrame(tid_trackid, schema)
    //df_track_id_tag_id.printSchema()


    val schema_string_attr = "track_id 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16"
    val schema_attr = StructType(schema_string_attr.split(" ").map(fieldName =>
      if (fieldName == "tag_id") StructField(fieldName, StringType, true)
      else StructField(fieldName, StringType, true))
    )
    val attributes = sc.textFile("D:/Project/FinalDataset/att20.txt").map(_.split(",")).map(p => Row(p(16).dropRight(1).drop(1),p(0) ,p(1),
    p(2),p(3),p(4),p(5),p(6),p(7),p(8),p(9),p(10),
      p(11),p(12),p(13),p(14),p(15)))

    val df_attributes =  sqlContext.createDataFrame(attributes, schema_attr)

    val temp = sc.textFile("D:/Project/FinalDataset/track_id_to_tag.txt").collect().map(_.split("\t")).map(p => Row(p(0), map_tagid_tag.add(p(1))))
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_tid_attributes_tag_id = df_attributes.join(df_track_id_tag_id, df_track_id_tag_id("track_id1") ===df_attributes("track_id") )

    var temp11 = df_tid_attributes_tag_id
    val split = temp11.randomSplit(Array(0.95, 0.05))
    val df_train_tid_attributes_tag_id = split(0)
    val df_test_tid_attributes_tag_id = split(1)

    val RDD_LP_tid_attributes_tag_id: RDD[LabeledPoint] = df_train_tid_attributes_tag_id.map(l =>

        (LabeledPoint
        (l(18).toString.toDouble ,
          Vectors.dense(
            math.round((1).toString.toFloat),
            math.round(l(2).toString.toFloat * 100),
            math.round(l(3).toString.toFloat * 1000),
            math.round(l(4).toString.toFloat/10),
            math.round(l(5).toString.toFloat * 1000),
            math.round(l(6).toString.toFloat * 10),
            math.round(l(7).toString.toFloat * 10),
            math.round(l(8).toString.toFloat),
            math.round(l(9).toString.toFloat),
            math.round(l(10).toString.toFloat * 100),
            math.round(l(11).toString.toFloat * 1000),
            math.round(l(11).toString.toFloat ),
            math.round(l(13).toString.toFloat * 1000),
            math.round(l(14).toString.toFloat * 10),
            math.round(l(15).toString.toFloat * 10),
            math.round(l(16).toString.toFloat )
          )))
)
    //train and test
    var predicted_res_RDD: RDD[(String, Int, String)] = sc.emptyRDD

    //RDD_LP_tid_attributes_tag_id.take(500).foreach(println)
    if (method == 1)
    {
      predicted_res_RDD = doRandomForest.test(doRandomForest.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==2)
    {
      predicted_res_RDD = doLogisticRegressionWithLBFGS.test(doLogisticRegressionWithLBFGS.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==3)
    {
      predicted_res_RDD = doDecisionTrees.test(doDecisionTrees.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==4)
    {
      predicted_res_RDD = doNaiveBayes.test(doNaiveBayes.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }


    //calculate accuracy

    val predictionAndLabels : RDD[(Double,Double)] = predicted_res_RDD.toDF().map(l => (l(1).toString.toDouble,l(2).toString.toDouble))
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
    println("End: Prediction")
  }

}

