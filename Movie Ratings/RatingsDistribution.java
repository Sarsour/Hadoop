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
        Job movieRatingsJob = null;
        
        Configuration conf = new Configuration();
        
        // Job Output Compression (job input compression is default)
        //conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        //conf.setStrings("mapreduce.output.fileoutputformat.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        
        System.out.println ("======");
        try {
            movieRatingsJob = new Job(conf, "RatingCount");
        } catch (IOException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
                                
        
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(movieRatingsJob, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        movieRatingsJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        movieRatingsJob.setMapperClass(HW5Mapper.class);
        movieRatingsJob.setReducerClass(HW5Reducer.class);

        // Set the Jar file 
        movieRatingsJob.setJarByClass(RatingDistribution.RatingsDistribution.class);       
        
        // Set the Output path
        FileOutputFormat.setOutputPath(movieRatingsJob, new Path(args[1]));
        
        // Set the Output Data Format
        movieRatingsJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        movieRatingsJob.setOutputKeyClass(Text.class);
        movieRatingsJob.setOutputValueClass(Text.class); 
        
        // Number of reduce tasks
        movieRatingsJob.setNumReduceTasks(2);
       
        
             
        try {
            movieRatingsJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingsDistribution.class.getName()).log(Level.SEVERE, null, ex);
        }
       
    }
    
}
