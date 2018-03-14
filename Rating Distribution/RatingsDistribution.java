package RatingDistribution;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RatingsDistribution {
    public static void main(String args[])  {
        Job ratingCountJob = null;
        
        Configuration conf = new Configuration();
        
        System.out.println ("======");
        try {
            ratingCountJob = new Job(conf, "RatingCount");
        } catch (IOException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
                                
        
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(ratingCountJob, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        ratingCountJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        ratingCountJob.setMapperClass(HW4Mapper.class);
        ratingCountJob.setReducerClass(HW4Reducer.class);

        // Set the Jar file 
        ratingCountJob.setJarByClass(RatingDistribution.RatingsDistribution.class);       
        
        // Set the Output path
        FileOutputFormat.setOutputPath(ratingCountJob, new Path(args[1]));
        
        // Set the Output Data Format
        ratingCountJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        ratingCountJob.setOutputKeyClass(Text.class);
        ratingCountJob.setOutputValueClass(IntWritable.class); 
        
        
        //wordCountJob.setNumReduceTasks(3);
       
             
        try {
            ratingCountJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
        }
       
    }
    
}
