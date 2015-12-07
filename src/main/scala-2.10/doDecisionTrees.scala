import java.util.Calendar
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time
import org.joda.time.{Interval, DateTime}
import org.joda.time.base.AbstractInterval

/**
 * Created by sneha on 12/5/2015.
 */
object doDecisionTrees {

  def train(df_train_tid_attributes_tag_id: DataFrame,RDD_LP_tid_attributes_tag_id : RDD[LabeledPoint] ): DecisionTreeModel={

    val startTime =  new DateTime()
    val numClasses = 16
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 32

    println("Start: Training DecisionTree with ", df_train_tid_attributes_tag_id.count(), " songs")
    val model = DecisionTree.trainClassifier(RDD_LP_tid_attributes_tag_id, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    println("End: DecisionTree Prediction")
    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to train:" , totalTime.toDuration.getStandardSeconds, "seconds")
    model
  }
  def test(model : DecisionTreeModel,df_test_tid_attributes_tag_id : DataFrame ): RDD[(String, Int, String)] = {
    val startTime =  new DateTime()
    println("Start: Prediction of", df_test_tid_attributes_tag_id.count(), "with DecisionTree ")
    val predicted_res_RDD:RDD[(String, Int, String)] = df_test_tid_attributes_tag_id.map(l =>
      if (l(1).toString.isEmpty == false & l(2).toString.isEmpty == false & l(3).toString.isEmpty == false & l(4).toString.isEmpty == false)
        ((l(0).toString,
          (model.predict(Vectors.dense(math.round((l(1).toString.toDouble) * 10),
            math.round(l(2).toString.toDouble * 10),
            l(3).toString.toDouble,
            math.round(l(4).toString.toDouble),

            math.round(l(5).toString.toDouble))).toInt), l(7).toString))
      else (l(0).toString, 0, (l(7).toString.toInt).toString))

    val endTime = new DateTime()
    val totalTime = new time.Interval(startTime,endTime)
    println("Time to test:" , totalTime.toDuration.getStandardSeconds, "seconds")

    predicted_res_RDD

  }
}
