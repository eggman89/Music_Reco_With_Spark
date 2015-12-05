import breeze.numerics.abs
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, Row, DataFrameStatFunctions}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object tagGenerator {
  val tagidtoid : Map[Int, Int] =  Map(1->95,2->5,3->96,4->38,5->70,6->98,7->238,8->86,9->322)
  val idtotagid : Map[Int, Int] =  Map(95->1,5->2,96->3,38->4,70->5,98->6,238->7,86->8,322->9)


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
    println("1: Random Forest; 2:Logistic Regression With LBFGS; 3:Decision Trees;  4:Naive Bayes")
    val method = readInt()
    //load tags and tag ids and attributes

    val df_tid_trackid = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/tag_id_track_id.csv", "header" -> "true"))
    val df_tagid_tag = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/tag_tag_id_tag.csv", "header" -> "true")).where("tag_id = '95' or tag_id = '5' or tag_id = '96' or tag_id = '38' or tag_id = '70' or tag_id = '98' or tag_id = '238' or tag_id = '86' or tag_id = '322'")
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_attributes =  sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))
    val df_tid_tag_id = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/tag_tid_tag_id.csv", "header" -> "true"))
    val df_tid_tag_id_top10 =  df_tid_tag_id.where("tag_id = '95' or tag_id = '5' or tag_id = '96' or tag_id = '38' or tag_id = '70' or tag_id = '98' or tag_id = '238' or tag_id = '86' or tag_id = '322'").limit(100)

    val arr = new Array[String](1)
    arr(0) = "tid"
    val df_tid_tag_id_with_dup =df_tid_tag_id_top10.dropDuplicates(arr) //remove duplicate

    //merge tid with song_attributed

    val df_tid_attributes = df_attributes.join(df_tid_trackid, df_tid_trackid("track_id") ===df_attributes("track_id") ).select("tid", "danceability","energy" ,"loudness", "tempo" )
    val df_tid_attributes_tag_id = df_tid_attributes.join(df_tid_tag_id_with_dup, df_tid_tag_id_with_dup("tid") ===df_tid_attributes("tid"))
    val split = df_tid_attributes_tag_id.randomSplit(Array(0.9, 0.1))
    val df_train_tid_attributes_tag_id = split(0)
    val df_test_tid_attributes_tag_id = split(1)
    var df_tid_attributes_tag_id2 = df_tid_attributes_tag_id.toDF("tid","danceability","energy","loudness","tempo","tid2","tag_id")
    //creating train_set


    val RDD_LP_tid_attributes_tag_id: RDD[LabeledPoint] = df_train_tid_attributes_tag_id.map(l =>
  if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(3).toString.isEmpty == false & l(4).toString.isEmpty == false)
    (LabeledPoint
    (idtotagid(l(6).toString.toInt) ,
      Vectors.dense(math.round((l(1).toString.toDouble)*10),
        math.round(l(2).toString.toDouble*10),
        // l(3).toString.toDouble,
        math.round(l(4).toString.toDouble))))
  else
    (LabeledPoint(0 , Vectors.dense(0.0,0.0,0.0))))


    //train and test
    val predicted_res_RDD: RDD[(String, Int, String)] = sc.emptyRDD

    if (method == 1)
      {
        val predicted_res_RDD = doRandomForest.test(doRandomForest.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
      }

    if(method ==2)
      {
        val predicted_res_RDD = doLogisticRegressionWithLBFGS.test(doLogisticRegressionWithLBFGS.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
      }

    if(method ==3)
    {
      val predicted_res_RDD = doDecisionTrees.test(doDecisionTrees.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

    if(method ==4)
    {
      val predicted_res_RDD = doNaiveBayes.test(doNaiveBayes.train(df_train_tid_attributes_tag_id,RDD_LP_tid_attributes_tag_id),df_test_tid_attributes_tag_id)
    }

   //calculate accuracy

    val predictionAndLabels : RDD[(Double,Double)] = predicted_res_RDD.toDF().map(l => (l(1).toString.toDouble,l(2).toString.toDouble))
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
    println("End: Prediction")

    //show output
    //[OPTIONAL : not needed to measure precision]following code is to show output ..not optimized right now
   /*println("Starting Output/file write")
    val predicted_res_DF = predicted_res_RDD.toDF("ptid","pre_tag_id", "real_tag_id")
    var predicted_res_tag_id = predicted_res_DF.join(df_tid_attributes_tag_id2, df_tid_attributes_tag_id2("tid") ===predicted_res_DF("ptid") ).
    select("tid","danceability","energy","loudness","tempo", "tag_id", "pre_tag_id", "real_tag_id")

    //predicted_res_tag_id.printSchema()
    var pred_df1 = predicted_res_tag_id.join(df_tagid_tag,df_tagid_tag("tag_id") === predicted_res_tag_id("pre_tag_id")  ).toDF("tid","danceability","energy","loudness","tempo","tag_id","pre_tag_id","real_tag_id","tag_id2","predicted_genre")

    var pred_df2 = pred_df1.select("tid","danceability","energy","loudness","tempo","real_tag_id" ,"predicted_genre")
    var pred_df3 = pred_df2.join(df_tagid_tag,df_tagid_tag("tag_id") === pred_df2("real_tag_id")  ).toDF("tid","danceability","energy","loudness","tempo","real_tag_id" ,"predicted_genre","tag_id2","real_genre" )
    pred_df3.printSchema()
    val pred_df4 = pred_df3.select("tid","danceability","energy","loudness","tempo","predicted_genre", "real_genre")
    pred_df4.printSchema()
    val pred_df5 = pred_df4.join(df_tid_trackid ,pred_df4("tid") === df_tid_trackid("tid") )
    val pred_df6 = pred_df5.join(df_metadata,df_metadata("track_id") === pred_df5("track_id") ).select("title","release","artist_name","duration","year","danceability","energy","loudness","tempo","predicted_genre", "real_genre")
    pred_df6.show()*/

  }

}

