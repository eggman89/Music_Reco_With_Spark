import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.{Interval, DateTime}
import org.joda.time.base.AbstractInterval

/**
 * Created by sneha on 12/5/2015.
 */
object doLogisticRegressionWithLBFGS {


  def train(df_train_tid_attributes_tag_id: DataFrame,RDD_LP_tid_attributes_tag_id : RDD[LabeledPoint] ): LogisticRegressionModel=
  {

    println("Start: Training LogisticRegressionWithLBFGS with ", df_train_tid_attributes_tag_id.count(), " songs")
    val startTime =  new DateTime()
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(16)
      .run(RDD_LP_tid_attributes_tag_id)
    println("End: LogisticRegressionWithLBFGS Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")
    model

  }

  def test(model : LogisticRegressionModel,df_test_tid_attributes_tag_id : DataFrame ): RDD[(String, Int, String)] = {
    val startTime =  new DateTime()
    println("Start: Prediction of", df_test_tid_attributes_tag_id.count(), "with LogisticRegressionWithLBFGS ")
    val predicted_res_RDD:RDD[(String, Int, String)] = df_test_tid_attributes_tag_id.map(l =>
      ( (l(0).toString,
        (model.predict(Vectors.dense(
          math.round((1).toString.toFloat),
          math.round(l(2).toString.toFloat * 100),
          math.round(l(3).toString.toFloat * 1000),
          math.round(l(4).toString.toFloat),
          math.round(l(5).toString.toFloat * 1000),
          math.round(l(6).toString.toFloat * 10),
          math.round(l(7).toString.toFloat * 10),
          math.round(l(8).toString.toFloat),
          math.round(l(9).toString.toFloat),
          math.round(l(10).toString.toFloat * 100),
          math.round(l(11).toString.toFloat * 1000),
          math.round(l(11).toString.toFloat ),
          math.round(l(13).toString.toFloat * 1000),
          math.round(l(14).toString.toFloat * 10),
          math.round(l(15).toString.toFloat * 10),
          math.round(l(16).toString.toFloat )
        )).toInt),l(18).toString))
    )


    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to test:" , totalTime.toDuration.getStandardSeconds, "seconds")
    predicted_res_RDD

  }
}
