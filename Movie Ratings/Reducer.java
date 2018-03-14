package RatingDistribution;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class HW5Reducer extends Reducer<Text, Text, Text, Text> {
    Text final_value = new Text();
    String string_a_value;
    int uuCount = 0;
    int ratingCount = 0;
    int ratingTotal = 0;
        
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        uuCount = 0;
        ratingCount = 0;
        ratingTotal = 0;
        
        for (Text val : values) {
            String new_val = val.toString();
            char c = new_val.charAt(0);
            
            if (c == 'a') {
                new_val = new_val.substring(new_val.indexOf("|")+1);
                new_val = new_val.substring(new_val.indexOf("|")+1);
                string_a_value = new_val;
                Counter counter = context.getCounter("MyCounter2", "UNIQUE_MOVIES");
                counter.increment(1);
            }
            else if (c == 'b') {
                uuCount = uuCount + 1;
            }
            else if (c == 'c') {
                ratingCount = ratingCount + 1;
                
                char rating = new_val.charAt(1);
                ratingTotal = ratingTotal + Character.getNumericValue(rating);
            }
            else {
                continue;
            }
        }
        
        String string_final_value = string_a_value + "|" + ratingTotal/ratingCount  + "|" + uuCount + "|" + ratingCount;
        
        final_value.set(string_final_value);
        
        context.write(key, final_value);
        
    
    }   
}
