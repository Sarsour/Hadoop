package RatingDistribution;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.hadoop.mapreduce.Counter;

public class HW5Mapper extends Mapper<LongWritable, Text, Text, Text>{ 

    public static Enum<?> MAP_RECORD_COUNTER;
    
    Logger logger = Logger.getLogger(HW5Mapper.class);
    Text info = new Text();
    Text movie_id = new Text();
    Text user_id = new Text();
    Text item_id = new Text();
    Text rating = new Text();
   

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());
    }
    
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        Counter counter = context.getCounter("MyCounter", "INPUT_LINES");
        counter.increment(1);
        
        String line = value.toString();
        
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        
        if (filename.startsWith("u.item")) {
            String[] parts = line.split("\\|");
            System.out.println(Arrays.toString(parts));
            movie_id.set(parts[0]);
            info.set(("a|" + parts[0] + "|" + parts[1] + "|" + parts[2] + "|" + parts[4]));
            context.write(movie_id, info);
        }
        
        else {
            
            StringTokenizer tokenizer = new StringTokenizer(line, "\t");
            
            if (tokenizer.countTokens() > 3) {
                user_id.set(tokenizer.nextToken());
                item_id.set(tokenizer.nextToken());
                rating.set(tokenizer.nextToken());

                String tagged_user_id = "b" + user_id.toString();
                String tagged_rating = "c" + rating.toString();

                Text text_tagged_user_id = new Text(tagged_user_id);
                Text text_tagged_rating = new Text(tagged_rating);

                context.write(item_id, text_tagged_user_id);
                context.write(item_id, text_tagged_rating);                
                
            }

        }
    }   
}
