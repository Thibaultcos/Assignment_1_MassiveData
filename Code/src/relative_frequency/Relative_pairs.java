package relative_frequency;

import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import  java.util.HashMap;
import  java.util.Map;
        
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



        
public class Relative_pairs {
 
      
	 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		    private final static IntWritable one = new IntWritable(1);
		    
	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        	 String line = value.toString().toLowerCase().replaceAll("[\\p{Punct}&&[^']&&[^-]]|(?<![a-zA-Z])'|'(?![a-zA-Z])|--|(?<![a-zA-Z])-|-(?![a-zA-Z])|\\d+"," ");
		         String[] words = line.split("\\s+");

		            for (String token : words) {
		                if (!token.isEmpty() && token != null) {
		                    int sum = 0;
		                    for (String pair : words) {
		                        if (!pair.isEmpty() && !pair.equals(token)) {
		                            context.write(new Text(token + " " + pair), one);
		                            sum = sum + 1;
		                        	}
		                    }
		                    context.write(new Text(token + " _all"), new IntWritable(sum));
		                }
		            }
	        }
	 } 
  
	    private static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
	    	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	                throws IOException, InterruptedException {
	            int sum = 0;
	            for (IntWritable val : values) {
	            	sum += val.get();
	            }
	            context.write(key, new IntWritable(sum));
	        }
	    }
        
	    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
	    	ArrayList<Double> freq =new ArrayList<Double>();
	        ArrayList<String> words= new ArrayList<String>();
	        double totalCount = 0;
	       
	        public void reduce(Text key, Iterable<IntWritable> values, Context context)
	                throws IOException, InterruptedException {
	            String word = key.toString();
	            int sum = 0;
	            double temp = 0;
	            int index = 0;
	            
	            for (IntWritable val : values) {
	                sum += val.get();
	           }

	            if (word.contains(" _all")) {
	                totalCount = sum;
	            } else {
	            	temp = sum/totalCount;
	            	if(freq.isEmpty()) {
	    	        	freq.add(temp);
	    	        	words.add(word);
	    	        }
	    	        else {
	    	        	while(index < 101 && index < freq.size()) {
	    	        		if (freq.get(index)<temp) {
	    	    	            		break;
	    	            			}
	    	            			index = index + 1;
	    	            		}
	    	        	
	    	        	if(index < 100) {
	    	        	freq.add(index, temp);
	    	    	    words.add(index, word);
	    	        	}
	    	    	}
	            }
	    	            		
	        }
	            	      
	        @Override
	        protected void cleanup(Context context)
	                throws IOException,
	                InterruptedException {
	        	int index = 0;
	            while (index<100) {
	               context.write(new Text(words.get(index)), new Text(""));
	               index = index+1;
	            }
	        }
	    }
	               
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "relative_pairs");
    
    job.setOutputKeyClass(Text.class); 
    job.setOutputValueClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setJarByClass(Relative_pairs.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Combine.class);

        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        
    job.waitForCompletion(true);
 }
        
 }
	

