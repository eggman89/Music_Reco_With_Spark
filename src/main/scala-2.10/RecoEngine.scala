import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.joda.time
import org.joda.time.DateTime



object RecoEngine {
  def main(args: Array[String]) {
    //remove logging from console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutil/")

    val conf = new SparkConf().setAppName("MusicReco").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.executor.memory", "6g").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //setting up sql context to query the data later on
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println("Spark Context started")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("INFO").setLevel(Level.OFF)

    val userid_hashmap = new hashmap()
    val songid_hashmap = new hashmap()
    println("Input a user number (1-1000) ")
    val user = readInt()
    val rawUserSongPlaycount = sc.textFile("D:/Project/FinalDataset/train_triplets1234.txt").map(_.split("\t")).collect().map(p => Rating(userid_hashmap.add(p(0)), songid_hashmap.add(p(1)), p(2).toInt))
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/lastfm_similar_dest.csv", "header" -> "true"))
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))
    val df_new_songs = df_metadata.where("year >= 2009")
    val df_new_keys = df_new_songs.select("track_id").toDF("tid")
    val df_new_attributes = df_attributes.join(df_new_keys, df_new_keys("tid") === df_attributes("track_id")).select("track_id", "danceability", "energy", "key", "loudness", "tempo", "time_signature")

    val text_train_triplets_all = sc.textFile("D:/Project/FinalDataset/train_triplets.txt")
    val schema_string = "user song_id play_count"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "play_count") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )
    val rowRDD1 = text_train_triplets_all.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).toInt))
    val df_train_triplets_all = sqlContext.createDataFrame(rowRDD1, schema)

    df_metadata.registerTempTable("meta_table")
    df_similar.registerTempTable("similar_table")
    df_attributes.registerTempTable("attributes")
    df_train_triplets_all.registerTempTable("triplets_table")

    println("User Profile Loaded")

    val t1 = df_train_triplets_all.filter(df_train_triplets_all("user") === userid_hashmap.findval(user))
    val user_history_df = t1.join(df_metadata, df_metadata("song_id") === t1("song_id"))
    //val user_history_df = profile.get_existing(sqlContext, userid_hashmap.findval(user)).where("year < 2009") //get user history

    print("Top 10 songs listened by the user")
    //t2.show()
     user_history_df.select("title", "artist_name", "release", "duration", "year", "play_count").show(10)

    // step 1 : Collaborative filtering 1
    val trainData = sc.parallelize(rawUserSongPlaycount)

    print("Starting Collaborative filtering training")
    val model = ALS.trainImplicit(trainData, 10, 1, 0.01, 1.0)
    print("End:  Collaborative filtering training")
    var song_val: Map[String, Int] = Map()

    print("Starting Collaborative filtering Recomendation")
    val recommendations = model.recommendProducts(user, 1000)
    var topcolab = recommendations.sortWith(_.rating  > _.rating).head.rating
    topcolab = topcolab/10
    print("End Collaborative filtering Recomendation")

    val song_val_temp = recommendations.flatMap {
      line => Some(songid_hashmap.obj.find(_._2 == line.product), math.round(line.rating / topcolab))
    }

    val temp_1 = sc.parallelize(song_val_temp)
    for (x <- temp_1.collect()) {

      song_val += (x._1.toString.drop(6).take(18) -> x._2.toInt) //list of similar songs by collabarative recco with weightage

    }
    val song_val_rdd = sc.parallelize(song_val.toSeq)
    val song_val_df = song_val_rdd.toDF("song_id1", "score")

    val df_reco_trackid_score = df_metadata.join(song_val_df, song_val_df("song_id1") === df_metadata("song_id")).select("track_id", "score").toDF("track_id", "score")

    val df_reco_old_attributes = df_reco_trackid_score.join(df_attributes, df_reco_trackid_score("track_id") === df_attributes("track_id"))

    //step 2: find similar songs based on user reccomended songs on last.fm
    val user_history_list = user_history_df.select("track_id", "play_count").map(r => Row(r(0), r(1)))

    //converting RDD to List
    var list_of_songs = Map[String, Int]()
    for (temp <- user_history_list.collect()) {
      var row = temp.toString().split(",")
      list_of_songs += (row(0).drop(1).toString -> row(1).dropRight(1).toInt)

    }

    val user_similar_songs = song.get_similar(sqlContext, list_of_songs) //Map for similar songs
    val user_similar_songs_RDD: RDD[(String, Int)] = sc.parallelize(user_similar_songs.toSeq) //Map to RDD
    val user_similar_songs_DF = user_similar_songs_RDD.toDF("_1", "_2").where("_2 > 0 ") //RDD to DF
    // DF song attributes for similar Songs
    val songs_attr = song.getAttributes(sqlContext, user_similar_songs.keysIterator)
    var user_SimilarResult = song.FinalResult(sc, sqlContext, user_similar_songs).select("track_id", "reco_conf") //DO NOT TOUCH

    //step 3 : prepare training set
    var top_score = user_SimilarResult.select("reco_conf").first().toString().dropRight(1).drop(1).toDouble
    top_score = top_score / 10.00

    val user_similar_songs_attr = user_SimilarResult.join(df_attributes, df_attributes("track_id") === user_SimilarResult("track_id"))
    val temp1 = user_similar_songs_attr.toDF("track_id","confidence","track_id2","danceability","energy","key" , "loudness","tempo","time_signature")
    .select("track_id","danceability","energy", "tempo" ,"key","time_signature","confidence").where("confidence > 0")


    val similar_songs_RDD_LP: RDD[LabeledPoint] = temp1.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(4).toString.isEmpty == false & l(5).toString.isEmpty == false)

        LabeledPoint
        (math.round(l(6).toString.toDouble / top_score),
          Vectors.dense(math.round((l(1).toString.toDouble)*10),
            math.round(l(2).toString.toDouble*10),
            l(3).toString.toDouble,
            math.round(l(4).toString.toDouble),
            l(5).toString.toDouble))
      else LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0)))
    val temp2 = df_reco_old_attributes.toDF("track_id","confidence","track_id2","danceability","energy","key" , "loudness","tempo","time_signature")
      .select("track_id","danceability","energy", "tempo" ,"key","time_signature","confidence").where("confidence > 0")

    val colab_similar_songs_RDD_LP: RDD[LabeledPoint] = temp2.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(4).toString.isEmpty == false)

        LabeledPoint
        (math.round(l(6).toString.toDouble ),
          Vectors.dense(math.round((l(1).toString.toDouble)*10),
            math.round(l(2).toString.toDouble*10),
            l(3).toString.toDouble,
            math.round(l(4).toString.toDouble),
            l(5).toString.toDouble))
      else LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0)))

    //merge colab reccomennded + user reccomended songs

    val train_set = colab_similar_songs_RDD_LP.union(similar_songs_RDD_LP)
   // train_set.toDF().toDF().sort($"label".desc).show(500)
    val startTime =  new DateTime()
    println("Start: Training LogisticRegressionWithLBFGS with ", train_set.count(), " songs")
    val finalmodel = new LogisticRegressionWithLBFGS()
      .setNumClasses(11)
      .run(train_set)

    println("End: LogisticRegressionWithLBFGS Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")


    val startTime1 =  new DateTime()
    val temp3 = df_new_attributes.select("track_id","danceability","energy", "tempo" ,"key","time_signature").limit(200)

    println("Start: Prediction of",10000 ,"with LogisticRegressionWithLBFGS ")

    val new_song_RDD:RDD[(String, Int, String)] =
      temp3.map(v =>
      if (v(1).toString.isEmpty == false & v(2).toString.isEmpty == false & v(4).toString.isEmpty == false & v(5).toString.isEmpty == false)
        ((v(0).toString,
          finalmodel.predict(Vectors.dense(math.round((v(1).toString.toDouble)*10),
            math.round(v(2).toString.toDouble*10),
            v(3).toString.toDouble,
            math.round(v(4).toString.toDouble),
            v(5).toString.toDouble)).toInt,"Hot"))
      else (v(0).toString, 0,"Hot"))

    new_song_RDD.foreach(println)
    val endTime1 = new DateTime()
    val totalTime1 = new time.Interval(startTime1,endTime1)
    println("Time to test:" , totalTime1.toDuration.getStandardSeconds, "seconds")

    println("1")
    val SimilarResultRDD2:RDD[(String, Int,String)] = user_SimilarResult.map(r=>(r(0).toString,math.round(r(1).toString.toDouble).toInt," "))
    println("2")
    val Top200TrackIdRDD = SimilarResultRDD2.union(new_song_RDD)
    val Top200TrackIdDF = Top200TrackIdRDD.toDF("_1","_2","_3")

    val Top200TrackId:RDD[(String,Int)] = Top200TrackIdDF.map(f=>(f(0).toString,f(1).toString.toInt))
    println("4")
    Top200TrackId.foreach(println)
    val Top200TrackIdMap: Map[String,Int] =  Top200TrackId.collect().toMap
    println("5")

    val final_songs = song.getDetails(sqlContext,Top200TrackIdMap.keysIterator)
    println("6")
    final_songs.join(Top200TrackIdDF,Top200TrackIdDF("_1")===final_songs("track_id") ).select("track_id","title","release","artist_name","duration","year","_2","_3").toDF("track_id","title","release","artist_name","duration","year","Confidence","Hot?").sort($"Confidence".desc).show(100)

    sc.stop()
    println("Spark Context stopped")



  }




}
