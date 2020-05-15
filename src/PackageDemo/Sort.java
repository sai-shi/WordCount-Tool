package PackageDemo;

import java.util.*; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sort {
	public static class SortMapper extends Mapper<Object, Text, Text, LongWritable> { 

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
			tmap.put(count, word); 

			// we remove the first key-value if it's size increases 10 
			if (tmap.size() > 208) 
			{ 
				tmap.remove(tmap.lastKey()); 
			} 
	    } 

		public void cleanup(Context context) throws IOException, InterruptedException 
		{ 
			for (Map.Entry<Long, String> entry : tmap.entrySet())  
			{ 

				long count = entry.getKey(); 
				String name = entry.getValue(); 

				context.write(new Text(name), new LongWritable(count)); 
			} 
		} 
	} 
	
	
	public static class SortReducer extends Reducer<Text, LongWritable, LongWritable, Text> { 

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
			tmap2.put(count, word); 

		// we remove the first key-value if it's size increases 10 
			if (tmap2.size() > 208) 
			{ 
				tmap2.remove(tmap2.lastKey()); 
			} 
		} 

		public void cleanup(Context context) throws IOException, InterruptedException 
		{ 

			for (Map.Entry<Long, String> entry : tmap2.descendingMap().entrySet())  
			{ 

				long count = entry.getKey(); 
				String word = entry.getValue(); 
				context.write(new LongWritable(count), new Text(word)); 
			} 
		} 
	} 
	
	
	public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
  
        Job job = Job.getInstance(conf, "sort"); 
        job.setJarByClass(Sort.class); 
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/sorted_output"), true);
  
        job.setMapperClass(SortMapper.class); 
        job.setReducerClass(SortReducer.class); 
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
  
        FileInputFormat.addInputPath(job, new Path("/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/word_count_output")); 
        FileOutputFormat.setOutputPath(job, new Path("/Users/samshi/Desktop/Academic/Temple/courses/CloudCumputing/assignment/hw3/my_test/sorted_output")); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
	
	
}
