import org.apache.spark.SparkConf
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.avro
import scala.collection.immutable.ListMap
import scala.util.control.Breaks._
import org.apache.spark.sql.Row

import scala.collection.immutable.ListMap

object profile {
  def get_existing(sqlContext:SQLContext, profile_id:String): DataFrame=
  {
    var sqlQuery = "SELECT * FROM meta_table JOIN triplets_table ON  meta_table.song_id = triplets_table.song_id WHERE triplets_table.user = '" +profile_id +"' ORDER BY triplets_table.play_count DESC"
    sqlContext.sql(sqlQuery)
  }

}
