import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
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

   val rawUserSongPlaycount = sc.textFile("D:/Project/FinalDataset/train_triplets1234.txt").map(_.split("\t")).map(p => Rating(userid_hashmap.add(p(0)), songid_hashmap.add(p(1)), p(2).toInt))
   val rawSongData =  sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_metadata = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/track_metadata_without_dup.csv", "header" -> "true"))
    val df_similar = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/lastfm_similar_dest.csv", "header" -> "true"))
    val df_attributes = sqlContext.load("com.databricks.spark.csv", Map("path" -> "D:/Project/FinalDataset/song_attributes.csv", "header" -> "true"))
    val df_old_songs = df_metadata.where("year < 2009")
    val df_new_songs = df_metadata.where("year >= 2009")
    val df_new_keys = df_new_songs.select("track_id").toDF("tid")
    val df_new_attributes = df_attributes.join(df_new_keys, df_new_keys("tid") === df_attributes("track_id"))select("track_id",	"danceability",	"energy",	"key"	,"loudness",	"tempo",	"time_signature")
    val df_old_attributes = df_attributes.join(df_old_songs, df_old_songs("track_id") === df_attributes("track_id"))

    val text_train_triplets_all = sc.textFile("D:/Project/FinalDataset/train_triplets.txt")
    val schema_string = "user song_id play_count"
    val schema = StructType(schema_string.split(" ").map(fieldName =>
      if (fieldName == "play_count") StructField(fieldName, IntegerType, true)
      else StructField(fieldName, StringType, true))
    )
    val rowRDD1 = text_train_triplets_all.map(_.split("\t")).map(p => Row(p(0), p(1), p(2).toInt))
    val df_train_triplets_all = sqlContext.createDataFrame(rowRDD1, schema)

    df_metadata.registerTempTable("meta_table")
    df_old_songs.registerTempTable("old_meta_table")
    df_similar.registerTempTable("similar_table")
    df_attributes.registerTempTable("attributes")
    df_train_triplets_all.registerTempTable("triplets_table")

    println("User Profile Loaded")
    var user_history_df = profile.get_existing(sqlContext, "b80344d063b5ccb3212f76538f3d9e43d87dca9e").where("year < 2009") //get user history

    println("Top 10 songs listened by the user")
    user_history_df.select("title", "artist_name", "release", "duration", "year", "play_count").show(20)

    // step 1 : collabarative filtering 1
    val trainData = sc.parallelize(rawUserSongPlaycount.collect())
    val model = ALS.trainImplicit(trainData, 10, 1, 0.01, 1.0)

   // val song:Map[String,Int]
     //model.recommendProducts(1,100).mkString(",").foreach(p=>(p(0)))
    //song.foreach(println)


    //step 2: find similar songs based on user reccomended songs


    /*val rawArtistData = sc.textFile("D:/Project/Final2/artist_data.txt")

      val artistByID = rawArtistData.flatMap { line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case e: NumberFormatException => None
          }
        }
      }
      val rawArtistAlias = sc.textFile("D:/Project/Final2/artist_alias.txt")
      val artistAlias = rawArtistAlias.flatMap { line =>
        val tokens = line.split('\t')
        if (tokens(0).isEmpty) {
          None
        } else {
          Some((tokens(0).toInt, tokens(1).toInt))}
      }.collectAsMap()

      println(artistByID.lookup(6803336).head)
      println(artistByID.lookup(1000010).head)

      val bArtistAlias = sc.broadcast(artistAlias)
      val trainData = rawUserArtistData.map { line =>
        val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
        val finalArtistID =
          bArtistAlias.value.getOrElse(artistID, artistID)
        Rating(userID, finalArtistID, count)
      }

    trainData.take(500).foreach(println)
      val model = ALS.trainImplicit(trainData, 10, 1, 0.01, 1.0)
      model.userFeatures.mapValues(_.mkString(", ")).first

      val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
        filter { case Array(user,_,_) => user.toInt == 2093760 }
      val existingProducts =
        rawArtistsForUser.map { case Array(_,artist,_) => artist.toInt }.
          collect().toSet
      artistByID.filter { case (id, name) =>existingProducts.contains(id)
      }.values.collect().foreach(println)

    val recommendations = model.recommendProducts(2093760, 5)

    val recommendedProductIDs = recommendations.map(_.product).toSet
    artistByID.filter { case (id, name) =>
      recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)*/
    }


}
