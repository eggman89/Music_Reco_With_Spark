import breeze.linalg.sum
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
object hitSongPredictor {

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

    //get user data
    val text_train_triplets = sc.textFile("D:/Project/FinalDataset/train_triplets.txt")
    val schema_string = "user song_id play_count"

    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "play_count") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )

    val rowRDD = text_train_triplets.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).toInt))
    val df_train_triplets = sqlContext.createDataFrame(rowRDD, schema)

    //load song data
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))

    val df_metadata_recent = df_metadata.where("year >= 2000 AND year <=2004")
    val df_train_triplets_top_50000 = df_train_triplets.groupBy("song_id").sum("play_count").sort($"sum(play_count)".desc).limit(50000)
    df_train_triplets_top_50000.show(100)
    val df_meta_data_with_play_count = df_metadata_recent.join(df_train_triplets_top_50000, df_train_triplets_top_50000("song_id") === df_metadata_recent("song_id")).sort($"sum(play_count)".desc).select("track_id","title","release","artist_name","year","sum(play_count)")

    val arr = new Array[String](1)
    arr(0) = "track_id"

    val df_song_details = df_attributes.join(df_meta_data_with_play_count,df_attributes("track_id") === df_meta_data_with_play_count("track_id") ).
    toDF("track_id","danceability","energy","key","loudness","tempo","time_signature", "track_id2", "title", "release", "artist_name", "year", "total_play_count").
    select("track_id","title","release","artist_name","year","danceability","energy","loudness","tempo","total_play_count").dropDuplicates(arr).sort($"total_play_count".desc)

    df_song_details.show(100)
    val df_train_song = df_song_details.select("track_id","danceability","energy", "loudness","tempo","total_play_count").sort($"total_play_count".desc)

    var top_score = df_train_song.select("total_play_count").first().toString().dropRight(1).drop(1).toDouble
    top_score = top_score/10

    val df_graduation = df_song_details.where("artist_name = 'Kanye West' AND release = 'Graduation'").select("track_id","danceability","energy", "loudness","tempo","total_play_count")
    val df_curtis = df_song_details.where("artist_name = '50 Cent' AND release = 'Curtis'").select("track_id","danceability","energy", "loudness","tempo","total_play_count")

    //top score


  //train

    val train_RDD_LP: RDD[LabeledPoint] = df_train_song.map(l =>
      if (l(5).toString.isEmpty == false & l(6).toString.isEmpty == false & l(7).toString.isEmpty == false & l(8).toString.isEmpty == false)
        (LabeledPoint
        (math.round(l(10).toString.toDouble / top_score),
          Vectors.dense(math.round(l(5).toString.toDouble*10),
            math.round(l(6).toString.toDouble*10),
            math.round(abs( l(7).toString.toDouble)/3),
            math.round(l(8).toString.toDouble))))
      else
        (LabeledPoint(0 , Vectors.dense(0,0,0,0))))

    println("Start: Training Bayes Naive")
    val model = NaiveBayes.train(train_RDD_LP, lambda = 1.0, modelType = "multinomial")
    println("End: Training Bayes Naive")

    //test with curtis
    val curtis_RDD:RDD[(String, Int)] = df_curtis.map(v =>
      if (v(5).toString.isEmpty == false & v(6).toString.isEmpty == false & v(7).toString.isEmpty == false & v(8).toString.isEmpty == false)
        (v(0).toString,
          model.predict(Vectors.dense(math.round((v(5).toString.toDouble)*10),
            math.round((v(6).toString.toDouble)*10),
            math.round(abs(v(7).toString.toDouble)/3),
            math.round(v(8).toString.toDouble))).toInt)
      else (v(0).toString, 0))

    //test with curtis
    val grad_RDD:RDD[(String, Int)] = df_graduation.map(v =>
      if (v(5).toString.isEmpty == false & v(6).toString.isEmpty == false & v(7).toString.isEmpty == false & v(8).toString.isEmpty == false)
        (v(0).toString,
          model.predict(Vectors.dense(math.round((v(5).toString.toDouble)*10),
            math.round((v(6).toString.toDouble)*10),
            math.round(abs(v(74).toString.toDouble)/3),
            math.round(v(8).toString.toDouble))).toInt)
      else (v(0).toString, 0))

    println("output")
    curtis_RDD.foreach(println)
    grad_RDD.foreach(println)
  }


}
