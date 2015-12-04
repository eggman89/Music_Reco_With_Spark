import breeze.numerics.abs
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{IntegerType, StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object tagGenerator {

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

    //load tags and tag ids and attributes

    val df_tid_trackid = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/tag_id_track_id.csv", "header" -> "true"))
    val df_tagid_tag = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/tag_tag_id_tag.csv", "header" -> "true")).where("tag_id = '95' or tag_id = '5' or tag_id = '96' or tag_id = '38' or tag_id = '70' or tag_id = '98' or tag_id = '238' or tag_id = '86' or tag_id = '322'")
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_attributes =  sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))
    val df_tid_tag_id = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/tag_tid_tag_id.csv", "header" -> "true"))
    val df_tid_tag_id_top10 =  df_tid_tag_id.where("tag_id = '95' or tag_id = '5' or tag_id = '96' or tag_id = '38' or tag_id = '70' or tag_id = '98' or tag_id = '238' or tag_id = '86' or tag_id = '322'")

    //df_attributes.describe("danceability","loudness","energy","tempo").show()
    val arr = new Array[String](1)
    arr(0) = "tid"
    val df_tid_tag_id_with_dup =df_tid_tag_id_top10.dropDuplicates(arr) //remove duplicate

   // df_tid_tag_id.show(100)
   // df_tid_tag_id_with_dup.show(100)


    //println(df_tid_tag_id_with_dup.count())
    //df_tid_tag_id_with_dup.show(111)

    //merge tid with song_attributed

    val df_tid_attributes = df_attributes.join(df_tid_trackid, df_tid_trackid("track_id") ===df_attributes("track_id") ).select("tid", "danceability","energy" ,"loudness", "tempo" )
    val df_tid_attributes_tag_id = df_tid_attributes.join(df_tid_tag_id_with_dup, df_tid_tag_id_with_dup("tid") ===df_tid_attributes("tid"))
    val split = df_tid_attributes_tag_id.randomSplit(Array(0.70, 0.30))
    val df_train_tid_attributes_tag_id = split(0)
    val df_test_tid_attributes_tag_id = split(1)
    var df_tid_attributes_tag_id2 = df_tid_attributes_tag_id.toDF("tid","danceability","energy","loudness","tempo","tid2","tag_id")
    //creating train_set

    val RDD_LP_tid_attributes_tag_id: RDD[LabeledPoint] = df_train_tid_attributes_tag_id.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(3).toString.isEmpty == false & l(4).toString.isEmpty == false)
        (LabeledPoint
        (l(6).toString.toDouble ,
          Vectors.dense(math.round(l(1).toString.toDouble*10/2),
            math.round((l(2).toString.toDouble*10)/3),
            math.round(abs( l(3).toString.toDouble)),
            math.round(l(4).toString.toDouble/3))))
      else
        (LabeledPoint(0 , Vectors.dense(0,0,0,0))))


    println("Start: Training Bayes Naive with ", df_train_tid_attributes_tag_id.count(), " songs")
   // val model = NaiveBayes.train(RDD_LP_tid_attributes_tag_id, lambda = 1.0, modelType = "multinomial")
   val model = NaiveBayes.train(RDD_LP_tid_attributes_tag_id, lambda = 1.0, modelType = "multinomial")
    println("End: Training Bayes Naive")

    //create test set
    println("Start: Prediction of",df_test_tid_attributes_tag_id.count() ,"with  Bayes Naive ")
    val predicted_res_RDD:RDD[(String, Int)] = df_test_tid_attributes_tag_id.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(3).toString.isEmpty == false & l(4).toString.isEmpty == false)
        (l(0).toString,
          model.predict(Vectors.dense(math.round(((l(1).toString.toDouble)*10)/2),
            math.round(((l(2).toString.toDouble)*10)/3),
            math.round(abs(l(4).toString.toDouble)),
            math.round(l(5).toString.toDouble/3))).toInt)
      else (l(0).toString,0))



    println("End: Prediction")

    println("Starting Output/file write")
    val predicted_res_DF = predicted_res_RDD.toDF()
    var predicted_res_tag_id = predicted_res_DF.join(df_tid_attributes_tag_id2, df_tid_attributes_tag_id2("tid") ===predicted_res_DF("_1") )
    predicted_res_DF.show(50)
    //println("pre:",predicted_res_DF.count())
    val df_test_tid_attributes_tag_id2 = df_test_tid_attributes_tag_id.toDF( "tid","danceability","energy","loudness","tempo","tid2","tag_id").select("tid","tag_id")
    df_test_tid_attributes_tag_id2.show(50)
    //predicted_res_tag_id.join(df_tagid_tag,df_tagid_tag("tag_id") === predicted_res_tag_id("tag_id") ).show()
    //df_test_tid_attributes_tag_id2.except(predicted_res_DF).show()
    //Comparing accuracy
    println("Computing Accuracy:")
    println("Total size of train dataset: " ,df_test_tid_attributes_tag_id.count())
    println("Total wrong predictions:", df_test_tid_attributes_tag_id2.except(predicted_res_DF).count())
  }

}
