package inverted_frequency;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;      




import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



        
public class Inverted_frequency {
 


	public static class Map extends Mapper<LongWritable, Text, Text, WordFrequency> {
		private WordFrequency wordfrequency = new WordFrequency();
		private Text word = new Text();


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			HashSet<String> stopwords = new HashSet<String>();
			BufferedReader Reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/Inverted Index/stop_words.csv")));
			String Stopline;
			
	    	while((Stopline = Reader.readLine()) !=null) {

	    		String[] array = Stopline.split(",");
	    		stopwords.add(array[1].toLowerCase());
	    	}		
			String filename = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			Reader.close();
            
			String line = value.toString().toLowerCase().replaceAll("[\\p{Punct}&&[^']&&[^-]]|(?<![a-zA-Z])'|'(?![a-zA-Z])|--|(?<![a-zA-Z])-|-(?![a-zA-Z])|\\d+"," ");
			for (String token : line.split("\\s+")) {
				if (!stopwords.contains(token.toLowerCase()) && !token.isEmpty()) {
					word.set(token.toLowerCase());
					wordfrequency.set(filename,1);
					context.write(word,wordfrequency);					
				}
				
			}		
				 
		}
	}
 
	public static class Combine extends
	Reducer<Text, WordFrequency, Text, WordFrequency> {
		


		public void reduce(Text key, Iterable<WordFrequency> values,
		Context context) throws IOException, InterruptedException {
			HashMap<String, IntWritable> map = new HashMap<String, IntWritable>();

			String file = "";
	
			for (WordFrequency val : values) {
				file = val.getFile().toString();
				if (map.containsKey(file)) {
					IntWritable currentfreq = (IntWritable) map.get(file);
	                currentfreq.set(currentfreq.get() + 1);
				}
			  else {
	                map.put(file, new IntWritable(1));
	            }
			}
			WordFrequency newmap = new WordFrequency();
			for (String filename : map.keySet()) {
				newmap.set(filename, map.get(filename).get()); 
				//file = filename + " " + map.get(filename).get();	
				}
			context.write(key, newmap);
		}
	}

	public static class Reduce extends
		Reducer<Text, WordFrequency, Text, Text> {

		public void reduce(Text key, Iterable<WordFrequency> values,
				Context context) throws IOException, InterruptedException {
			
			StringBuilder builder = new StringBuilder();
			for (WordFrequency val : values) {
				Text file = new Text(val.getFile());
				int freq = val.getFreq().get();
				if (builder.toString().isEmpty()) {
					builder.append(file+"#"+freq);
				}
				else {
					builder.append(", "+file+"#"+freq);
				}	
			}
			if (!key.toString().isEmpty()) {
				context.write(key, new Text(builder.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "inverted_frequency");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(WordFrequency.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(Inverted_frequency.class);
		
	   
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setCombinerClass(Combine.class);
	    job.setNumReduceTasks(10);  
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    conf.setBoolean("mapreduce.map.output.compress",true);
	    conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	        
	    job.waitForCompletion(true);
	 }
	        
	 }
		


 
        
