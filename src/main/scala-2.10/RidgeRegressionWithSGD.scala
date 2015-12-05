import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.DateTime

/**
 * Created by sneha on 12/5/2015.
 */
object doRidgeRegressionWithSGD {
  def train(df_old_songs: DataFrame, similar_songs_RDD_LP: RDD[LabeledPoint]): RidgeRegressionModel = {
    val startTime = new DateTime()
    val numIterations = 1000
    println("Start: Training RidgeRegressionModel with ", df_old_songs.count(), " songs")
    val model = RidgeRegressionWithSGD.train(similar_songs_RDD_LP, numIterations)

    println("End: RidgeRegressionModel Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime, endTime)
    println("Time to train:", totalTime.toDuration.getStandardSeconds, "seconds")
    model

  }

  def test(model: RidgeRegressionModel, df_new_attributes: DataFrame): RDD[(String, Int, String)] = {
    val startTime = new DateTime()
    val new_song_RDD: RDD[(String, Int, String)] = df_new_attributes.map(v =>
      if (v(1).toString.isEmpty == false & v(2).toString.isEmpty == false & v(4).toString.isEmpty == false & v(5).toString.isEmpty == false)
        (v(0).toString,
          model.predict(Vectors.dense(math.round((v(1).toString.toDouble) * 10),
            math.round((v(2).toString.toDouble) * 10),
            //   math.round(abs(v(4).toString.toDouble)/3),
            math.round(v(5).toString.toDouble))).toInt, "Hot")
      else (v(0).toString, 0, "Hot"))
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime, endTime)
    println("Time to test:", totalTime.toDuration.getStandardSeconds, "seconds")

    new_song_RDD


  }
}
