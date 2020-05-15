package PackageDemo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OnlyOnce {
    public static void main(String [] args) throws Exception
    {
        Configuration conf=new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("onlyonce");
        job.setJarByClass(OnlyOnce.class);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/only_once_output"), true);
        
        job.setMapperClass(BigramMapper.class);
        job.setReducerClass(BigramReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path("/user/Hadoop/word_count_data/pride_and_prejudice.txt")); 
        FileOutputFormat.setOutputPath(job, new Path("/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/only_once_output"));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
    public static class BigramMapper extends Mapper<Object, Text, Text, IntWritable>{
    	
    	private static final IntWritable ONE = new IntWritable(1);
        private static final Text BIGRAM = new Text();
          
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
         {
           String line = value.toString();
    	   String prev = null;
           StringTokenizer itr = new StringTokenizer(line);
          while (itr.hasMoreTokens()) 
          {
            String cur = itr.nextToken();
           // Emit only if we have an actual BIGRAM 
            if (prev != null && cur.matches("^[a-zA-Z]+$") && prev.matches("^[a-zA-Z]+$")) 
            {
              BIGRAM.set(prev + " " + cur);
              context.write(BIGRAM, ONE);
            }
            prev = cur;
          } 
         }
    }
    public static class BigramReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
   
    	private int wordCount; 
    	
    	public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable value : values)
            {
                sum += value.get();
            }
            if (sum == 1) {
            	++wordCount;
            	con.write(word, new IntWritable(wordCount));
            }
        }
    	
    }
}
