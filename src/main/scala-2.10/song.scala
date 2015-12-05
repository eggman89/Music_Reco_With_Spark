import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.SparkContext
import scala.collection.immutable.ListMap
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.FloatType


object song {


  def get_similar(sqlContext: SQLContext, user_history: Map[String, Int]): Map[String, Float] = {

    var index = 0;    // to track the index in loop
    var i = 0;
    var song_tmp = ""
    var sim_songs = ""
    var parent_key = ""
    var map_sim_songs = Map[String, Float]() //Map to keep track of similar songs with priority


    var tid = ""
    var tidlist = ""
    var tidinorder = ""


    for (tid <- user_history.keysIterator) {
      tidlist = tidlist + "'" + tid + "', "
      tidinorder = tidinorder + " WHEN '" + tid + "' THEN " + (i + 1)
      i = i + 1
    }
    tidlist = tidlist.dropRight(2)

    //var sim_songs_list = df_similar.filter(df_similar("tid") === track_id).select("target").first().toString() // lists all the similar songs
    var sim_songs_df = sqlContext.sql("SELECT * FROM similar_table WHERE tid IN (" + tidlist + ")")

    for (sim_songs_list <- sim_songs_df.collect()) {

      //parsing and stuff

      parent_key = sim_songs_list.toString().substring(1,19)
      sim_songs = sim_songs_list.toString()
      sim_songs = sim_songs.dropRight(1)
      sim_songs = sim_songs.drop(20)

      var ar_similar_songs = sim_songs.split(",") //spliting the result by a comma
      index = 0 //initializing to 0 before loop

      //loop to put the similar songs and likness factor into a SortedMap
      for (song_tmp <- ar_similar_songs) {

        if (index % 2 == 0) // store the tid & frequency {
        {
          map_sim_songs += (ar_similar_songs(index).toString() -> (user_history(parent_key) * ar_similar_songs(index + 1).toFloat))
        }
        index = index + 1
      }
    }
    new ListMap() ++ map_sim_songs.toSeq.sortWith (_._2 > _._2)
  }

  def getDetails(sqlContext: SQLContext, track_id:String) :DataFrame=
  {
    sqlContext.sql("SELECT * FROM meta_table WHERE track_id = '" + track_id + "'")
  }

  def getDetails(sqlContext: SQLContext, id:Iterator[String]) :DataFrame=  {
    var tid=""
    var tidlist=""
    var tidinorder=""
    var index=0

    for (tid<-id)
    {
      // if(index < 20){
      tidlist = tidlist + "'" + tid + "', "
      tidinorder = tidinorder + " WHEN '" + tid+ "' THEN " + (index + 1)

      // }
      index = index + 1

    }
    tidlist = tidlist.dropRight(2)
    var sqlQuery = ""
    sqlQuery = "SELECT * FROM meta_table WHERE track_id IN (" + tidlist + ") ORDER BY CASE track_id" + tidinorder + " END"
    sqlContext.sql(sqlQuery)
  }

  def FinalResult(sc: SparkContext, sqlContext: SQLContext, user_history:Map[String, Float]) :DataFrame=
  {
    var sqlQuery = ""
    val schemaString = "track_id reco_conf"
    //val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schema = StructType(
                  schemaString.split(" ").map(fieldName =>
                  if(fieldName == "track_id" ) StructField(fieldName, StringType, true)
                  else StructField(fieldName, FloatType, true))
    )
    val rowRDD = sc.parallelize(user_history.toSeq).map(_.toString().drop(1).dropRight(1).split(",")).map(x => Row(x(0), x(1).toFloat))

    val likeDataFrame = sqlContext.createDataFrame(rowRDD, schema)
     //case class Like(track_id: String, reco_conf: Float)
    //val people = sc.parallelize(user_history.toSeq).map(_.toString().drop(1).dropRight(1).split(",")).map(x => Like(x(0), x(1).toFloat)).toDF()

    likeDataFrame.registerTempTable("like_table")
    sqlQuery = "SELECT meta_table.track_id, title,artist_name,release,duration,year,reco_conf FROM meta_table JOIN like_table ON like_table.track_id = meta_table.track_id ORDER BY like_table.reco_conf DESC "
    sqlContext.sql(sqlQuery)
  }

  def getAttributes(sqlContext: SQLContext, id:Iterator[String]) :DataFrame=
  {
    var tid=""
    var tidlist=""
    var tidinorder=""
    var index=0

    for (tid<-id)
    {
      // if(index < 20){
      tidlist = tidlist + "'" + tid + "', "
      tidinorder = tidinorder + " WHEN '" + tid+ "' THEN " + (index + 1)

      // }
      index = index + 1

    }
    tidlist = tidlist.dropRight(2)
    var sqlQuery = ""
    sqlQuery = "SELECT * FROM attributes WHERE track_id IN (" + tidlist + ") ORDER BY CASE track_id" + tidinorder + " END"
    sqlContext.sql(sqlQuery)
  }

}
