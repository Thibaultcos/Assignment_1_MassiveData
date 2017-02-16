package relative_stripes;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class relative_stripes {
        
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	 String line = value.toString().toLowerCase().replaceAll("[\\p{Punct}&&[^']&&[^-]]|(?<![a-zA-Z])'|'(?![a-zA-Z])|--|(?<![a-zA-Z])-|-(?![a-zA-Z])|\\d+"," ");
	         String[] words = line.split("\\s+");

	            for (String token : words) {
	            	if( !token.isEmpty() && token != null) {
	            	context.write(new Text(token),new Text(line));
	                }	                    
	            }	                 
       }
	}      
            
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
	 private HashMap<String, IntWritable> Map = new HashMap<String, IntWritable>();
 	 ArrayList<Double> freqlist =new ArrayList<Double>();
     ArrayList<String> wordslist= new ArrayList<String>();

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	String mainword = key.toString();
    	
    	int sum =0;
    	
    	for (Text line2 : values) {
    		
    		String[] words = line2.toString().split("\\s+");
    		for (String token : words) {
    			
    			if (!token.isEmpty() && token != null && !token.equals(mainword)) {
    				if (!Map.containsKey(token.toLowerCase())) {
    					Map.put(token.toLowerCase(),new IntWritable(1));
    				}
    				
    				else if (Map.containsKey(token.toLowerCase())) {
    					IntWritable one = new IntWritable(1);
    					IntWritable currentfreq = (IntWritable) Map.get(token);
    					currentfreq.set(currentfreq.get() + one.get());
    				}
    			}		
    		}
    	}
    	
    	for (String name: Map.keySet()) {
    		sum = sum + Map.get(name).get();
    	}
    	  	
		for (String name: Map.keySet()) {
			int index = 0;
			double temp = 0;
			temp = (double) Map.get(name).get()/sum;
        	if(freqlist.isEmpty()) {
	        	freqlist.add(temp);
	        	wordslist.add(mainword+ " " + name);
	        }
	        else {
	        	while(index < 100 && index < freqlist.size()) {
	        		if (freqlist.get(index)<temp) {
	    	            		break;
	            	}
	            	index = index + 1;
	            }
	        	
	        	if(index < 100) {
	        	freqlist.add(index, temp);
	    	    wordslist.add(index, mainword+ " " + name);
	        	}
	    	}
		}
    }
 
    @Override
    protected void cleanup(Context context)
            throws IOException,InterruptedException {
    	int index = 0;
        while (index<100) {
           context.write(new Text(wordslist.get(index)), new Text(""));
           index = index+1;
        }
    }
    
}
        
 public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
     
    Job job = new Job(conf, "relative_stripes");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(relative_stripes.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(1);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
