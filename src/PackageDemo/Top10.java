package PackageDemo;

import java.util.*; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Top10 {
	
	
	public static class Globals {
	    public static String INPUT_PATH = "/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/word_count_output";
	    public static String OUTPUT_PATH = "/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/top_10_output";
	    public static String LOCAL_PATH = "/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/pride_and_prejudice.txt";
	    public static int TOP = 10;
	}
	
	
	public static class top_k_Mapper extends Mapper<Object, Text, Text, LongWritable> { 

		private TreeMap<Long, String> tmap; 

		public void setup(Context context) throws IOException, 	InterruptedException 
		{ 
			tmap = new TreeMap<Long, String>(); 
		} 

		public void map(Object key, Text value, Context context) throws IOException,  InterruptedException 
	    { 
			String[] tokens = value.toString().split("\t"); 

			String word = tokens[0]; 
			long count = Long.parseLong(tokens[1]); 

			// insert data into treeMap, we want top 10 words so we pass count as key 
			
			if (!word.contains(" ")) {
				tmap.put(count, word); 
				}
			tmap.put(count, word); 

			// we remove the first key-value if it's size increases 10 
			if (tmap.size() > Globals.TOP) 
			{ 
				tmap.remove(tmap.firstKey()); 
			} 
	    } 

		public void cleanup(Context context) throws IOException, InterruptedException 
		{ 
			for (Map.Entry<Long, String> entry : tmap.entrySet())  
			{ 

				long count = entry.getKey(); 
				String word = entry.getValue(); 

				context.write(new Text(word), new LongWritable(count)); 
			} 
		} 
	} 
	
	
	public static class top_k_Reducer extends Reducer<Text, LongWritable, DoubleWritable, Text> { 

		private TreeMap<Long, String> tmap2; 

		public void setup(Context context) throws IOException, InterruptedException 
		{ 
			tmap2 = new TreeMap<Long, String>(); 
		} 

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
		{ 

			String word = key.toString(); 
			long count = 0; 

			for (LongWritable val : values) 
			{ 
				count = val.get(); 
			} 

		// insert data into treeMap, we want top 10 words so we pass count as key 
			if (!word.contains(" ")) {
			tmap2.put(count, word); 
			}

		// we remove the first key-value if it's size increases 10 
			if (tmap2.size() > Globals.TOP) 
			{ 
				tmap2.remove(tmap2.firstKey()); 
			} 
		} 

		public void cleanup(Context context) throws IOException, InterruptedException 
		{   
			
			int total = 0;
			File file = new File(Globals.LOCAL_PATH);
		    try(Scanner sc = new Scanner(new FileInputStream(file))){
		        while(sc.hasNext()){
		            sc.next();
		            total++;
		        }
		    }
		   
		    

			for (Map.Entry<Long, String> entry : tmap2.descendingMap().entrySet())  
			{ 

				long count = entry.getKey(); 
				double d = (double)count;
				double ratio = d * 100  / total;
				String word = entry.getValue(); 
				context.write(new DoubleWritable(ratio), new Text(word)); 
			} 
			
			context.write(new DoubleWritable(total), new Text("Total Words."));
		} 
	} 
	
	
	public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
  
        Job job = Job.getInstance(conf, "top10"); 
        job.setJarByClass(Top10.class); 
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(Globals.OUTPUT_PATH), true);
  
        job.setMapperClass(top_k_Mapper.class); 
        job.setReducerClass(top_k_Reducer.class); 
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(DoubleWritable.class); 
        job.setOutputValueClass(Text.class); 
  
        FileInputFormat.addInputPath(job, new Path(Globals.INPUT_PATH)); 
        FileOutputFormat.setOutputPath(job, new Path(Globals.OUTPUT_PATH)); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
	
	
}
