
package TestRecommendation;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public class TestRecommendation {

	public static void main(String[] args) {
		try {
                    
                        // Create objects for input and output files
			DataModel data = new FileDataModel(new File("data/input.txt"));
                        PrintWriter writer = new PrintWriter("output.txt");
			writer.println("Patient Accession | Genetic Test | Score");
                                                
                        // Algorithm being used
			TanimotoCoefficientSimilarity rec = new TanimotoCoefficientSimilarity(data);
                        
			GenericItemBasedRecommender recommender = new GenericItemBasedRecommender(data, rec);
			
                        LongPrimitiveIterator testID = data.getItemIDs();
                        
                        // Loop through tests
			while(testID.hasNext()) {
				long patientAccession = testID.nextLong();
				List<RecommendedItem>recommendations = recommender.mostSimilarItems(patientAccession, 3);
				
				for(RecommendedItem recommendation : recommendations) {
					writer.println(patientAccession + " | " + recommendation.getItemID() + " | " + recommendation.getValue());
				}
			}
			
		} 
                
                catch (IOException e) {
			e.printStackTrace();
                        System.out.println("IOException");
		} 
                catch (TasteException e) {
			e.printStackTrace();
                        System.out.println("TasteException");
		}

	}

}