import java.io.File


import org.apache.log4j.{Logger, Level}
import org.apache.mahout
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood
import org.apache.mahout.cf.taste.model.DataModel
import org.apache.mahout.cf.taste.common.Refreshable
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.impl.recommender.{GenericUserBasedRecommender, GenericItemBasedRecommender}
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity
import org.apache.mahout.cf.taste.model.DataModel
import org.apache.mahout.cf.taste.recommender.{Recommender, RecommendedItem, IDRescorer}
import org.apache.mahout.cf.taste.similarity.UserSimilarity
import org.apache.mahout.cf.taste.similarity.ItemSimilarity
import spire.std.long
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import java.util.Random
import org.apache.spark.rdd._
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;

class ItemRecommend {

  def start() {
    val conf = new SparkConf().setAppName("MovieReco").setMaster("local[*]")
    val sc = new SparkContext(conf)


    var dm = new FileDataModel(new File("D:/Project/Dataset/movies1234.csv"))

    var sim = new TanimotoCoefficientSimilarity(dm)
    var recommender = new GenericItemBasedRecommender(dm, sim)
    var x=1
    var items = dm.getItemIDs()

      //if(x>10) System.exit(1);




  }
}

object SampleTest2 {
  def main(args: Array[String]) {
    var mov = new ItemRecommend
    mov.start()
  }



}
