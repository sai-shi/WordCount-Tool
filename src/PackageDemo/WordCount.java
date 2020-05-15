package PackageDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class WordCount {
	
	public static class Globals {
	    public static String INPUT_PATH = "/user/Hadoop/word_count_data/pride_and_prejudice.txt";
	    public static String OUTPUT_PATH = "/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/word_count_output";
	}

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().toUpperCase());
                // word.set(tokenizer.nextToken());
                String word_string = word.toString();
                Pattern p = Pattern.compile("[a-zA-Z]+");
                Matcher m = p.matcher(word_string);
                if (m.find()) {
                	String g = m.group();
                	context.write(new Text(g), one);
                
                }
            }
        }
        
    }
    
    
    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	
    	public void setup(Context context) throws IOException, InterruptedException 
		{
		}
    

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            context.write(key, new IntWritable(sum));
            
            
        }
        
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	
    	private int wordCount;
    	
    	public void setup(Context context) throws IOException, InterruptedException 
		{ 
			wordCount = 0;
		}
    

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            ++wordCount;

            context.write(key, new IntWritable(sum));
            
            
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException 
		{ 

        	context.write(new Text("Number of unique words (converted to uppercase): "),  new IntWritable(wordCount));
        	
		}
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("wordcount");
        job.setNumReduceTasks(1);
        job.setJarByClass(WordCount.class);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(Globals.OUTPUT_PATH), true);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setCombinerClass(WordCountCombiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Globals.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Globals.OUTPUT_PATH));

        job.waitForCompletion(true);
    }

    
    
}
    





