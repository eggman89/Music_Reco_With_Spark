import java.io.File


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

/**
 * Created by sneha on 12/1/2015.
 */
object SampleTest {
  def main(args: Array[String])  {
    var a = 1
    var model = new FileDataModel(new File("D:/Project/Dataset/dataset.csv"))
    var similarity = new PearsonCorrelationSimilarity(model)
    var neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model)
    var recommender = new GenericUserBasedRecommender(model, neighborhood, similarity)
    var recommendations = recommender.recommend(2, 3);
    recommendations
    println(recommendations)
  }
  }


