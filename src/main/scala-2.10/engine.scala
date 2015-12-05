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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
object engine {
  def main(args: Array[String]) {
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
    println("Select a Method to Predict the songs values")
    println("1: LinearRegressionWithSGD; 2:RidgeRegressionWithSGD; 3:LassoWithSGD ")
    val method = readInt()

    //df_userdata > dataframe for user data
    //val text_train_triplets = sc.textFile("hdfs://localhost:19000/train_triplets_small.txt")
    val text_train_triplets = sc.textFile("D:/Project/FinalDataset/train_triplets_small2.txt")
    val schema_string = "user song_id play_count"

    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "play_count") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )

    val rowRDD = text_train_triplets.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).toInt))
    val df_train_triplets = sqlContext.createDataFrame(rowRDD, schema)

    /*df_metadata > dataframe for songs metadata
    df_similar >  dataframe for similar songs
    df_attributes > dataframe for song attributes
    loading the dataframe from HDFS ; currently local node*/

    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/lastfm_similar_dest.csv", "header" -> "true"))
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))
    val df_old_songs = df_metadata.where("year < 2009")
    val df_new_songs = df_metadata.where("year >= 2009")
    val df_new_keys = df_new_songs.select("track_id").toDF("tid")
    val df_new_attributes = df_attributes.join(df_new_keys, df_new_keys("tid") === df_attributes("track_id"))select("track_id",	"danceability",	"energy",	"key"	,"loudness",	"tempo",	"time_signature")
    val df_old_attributes = df_attributes.join(df_old_songs, df_old_songs("track_id") === df_attributes("track_id"))

    df_metadata.registerTempTable("meta_table")
    df_old_songs.registerTempTable("old_meta_table")
    df_similar.registerTempTable("similar_table")
    df_attributes.registerTempTable("attributes")
    df_train_triplets.registerTempTable("triplets_table")
    //df_old_attributes.show(10)


    println("User Profile Loaded")
    var user_history_df = profile.get_existing(sqlContext, "b80344d063b5ccb3212f76538f3d9e43d87dca9e").where("year < 2009") //get user history

    println("Top 10 songs listened by the user")
    user_history_df.select("title", "artist_name", "release", "duration", "year", "play_count").show(20)

    var user_history_list = user_history_df.select("track_id", "play_count").limit(10).map(r => Row(r(0), r(1)))

    //converting RDD to List
    var list_of_songs = Map[String, Int]()
    for (temp <- user_history_list.collect()) {
      var row = temp.toString().split(",")
      list_of_songs += (row(0).drop(1).toString -> row(1).dropRight(1).toInt)

    }
    val similar_songs = song.get_similar(sqlContext, list_of_songs) //Map for similar songs
    val similar_songs_RDD: RDD[(String, Float)] = sc.parallelize(similar_songs.toSeq) //Map to RDD
    val similar_songs_DF = similar_songs_RDD.toDF() //RDD to DF
    // DF song attributes for similar Songs
    val songs_attr = song.getAttributes(sqlContext, similar_songs.keysIterator)
    var similar_songs_attr = songs_attr.join(similar_songs_DF, songs_attr("track_id") === similar_songs_DF("_1")).select("_2", "danceability", "energy", "loudness", "tempo")
    var sim_song_keys = song.get_similar(sqlContext, list_of_songs)

    var SimilarResult = song.FinalResult(sc, sqlContext, sim_song_keys).select("track_id","reco_conf") //DO NOT TOUCH

    //get top score and divide by 10
    //sim_song_keys.foreach(println)
    //SimilarResult.show(10)

    var top_score = SimilarResult.select("reco_conf").first().toString().dropRight(1).drop(1).toDouble
    //println("here6")
    top_score = top_score / 100.00

    //convert SimilarResult DF to Similar Result RDD
    var SimilarResultRDD:RDD[(String, Int,String)] = SimilarResult.map(r=>(r(0).toString,math.round(r(1).toString.toDouble/top_score).toInt," "))
    var SimilarResultRDD2:RDD[(String, Int)] = SimilarResult.map(r=>(r(0).toString,math.round(r(1).toString.toDouble/top_score).toInt))

 //train and test

    val similar_songs_RDD_LP: RDD[LabeledPoint] = similar_songs_attr.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(4).toString.isEmpty == false  )

        LabeledPoint
        		 (math.round(l(0).toString.toDouble / top_score),
      Vectors.dense(math.round(l(1).toString.toDouble*10),
          math.round(l(2).toString.toDouble*10),
         // math.round((30 + l(3).toString.toDouble)/3),
          math.round(l(4).toString.toDouble)))
    else LabeledPoint(0,Vectors.dense(0,0,0)))

 var new_song_RDD: RDD[(String, Int, String)] = sc.emptyRDD

    if (method ==1)
    {
      new_song_RDD = doLinearSVM.test(doLinearSVM.train(df_old_songs,similar_songs_RDD_LP),df_new_attributes)
    }

    if(method ==2)
    {
      new_song_RDD = doRidgeRegressionWithSGD.test(doRidgeRegressionWithSGD.train(df_old_songs,similar_songs_RDD_LP),df_new_attributes)
    }

    if(method ==3)
    {
      new_song_RDD = doLassoWithSGD.test(doLassoWithSGD.train(df_old_songs,similar_songs_RDD_LP),df_new_attributes)
    }

 // test the set of new songs

 val Top200TrackIdDF = SimilarResultRDD.union(new_song_RDD).sortBy(_._2,false).toDF().limit(200)
 val Top200TrackId:RDD[(String,Int)] = Top200TrackIdDF.map(f=>(f(0).toString,f(1).toString.toInt))
 val Top200TrackIdMap: Map[String,Int] =  Top200TrackId.collect().toMap

 val final_songs = song.getDetails(sqlContext,Top200TrackIdMap.keysIterator)
 final_songs.join(Top200TrackIdDF,Top200TrackIdDF("_1")===final_songs("track_id") ).select("track_id","title","release","artist_name","duration","year","_2","_3").toDF("track_id","title","release","artist_name","duration","year","Confidence","Hot?").sort($"_2".desc).show(100)

 sc.stop()
 println("Spark Context stopped")
}

}
