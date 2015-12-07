import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.{Interval, DateTime}
import org.joda.time.base.AbstractInterval

/**
 * Created by sneha on 12/5/2015.
 */
object doNaiveBayes2 {

  def train(df_old_songs: DataFrame,similar_songs_RDD_LP : RDD[LabeledPoint] ): NaiveBayesModel={

    val startTime =  new DateTime()
    println("Start: Training NaiveBayes2 with ", similar_songs_RDD_LP.count(), " songs")
    val model = NaiveBayes.train(similar_songs_RDD_LP, lambda = 1.0, modelType = "multinomial")
    println("End: NaiveBayes Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")

    model
  }

  def test(model:NaiveBayesModel, df_new_attributes:DataFrame): RDD[(String, Int, String)]  =
  {
    val startTime =  new DateTime()
    println("Start: Prediction of",df_new_attributes.count() ,"with NaiveBayes ")
    val new_song_RDD:RDD[(String, Int, String)] = df_new_attributes.map(v =>
      if (v(1).toString.isEmpty == false & v(2).toString.isEmpty == false & v(4).toString.isEmpty == false & v(5).toString.isEmpty == false)
        (v(0).toString,
          model.predict(Vectors.dense(math.round((v(1).toString.toDouble)*10),
            math.round((v(2).toString.toDouble)*10),
            //   math.round(abs(v(4).toString.toDouble)/3),
            math.round(v(5).toString.toDouble))).toInt,"Hot")
      else (v(0).toString, 0,"Hot"))
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to test:" , totalTime.toDuration.getStandardSeconds, "seconds")

    new_song_RDD

  }

}
