package index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


        
public class Inverted_unique {
 
	public static enum CUSTOM_COUNTER {
		Counter_Unique_Words,
	};

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text filename = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			HashSet<String> stopwords = new HashSet<String>();

			BufferedReader Reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/stopwords.csv")));
			
			String Stopline;		
	    	while((Stopline = Reader.readLine()) !=null) {
	    		String[] array = Stopline.split(",");
	    		stopwords.add(array[0].toLowerCase());
	    	}
	 		
			filename = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
			Reader.close();
             			
			String line = value.toString().toLowerCase().replaceAll("[\\p{Punct}&&[^']&&[^-]]|(?<![a-zA-Z])'|'(?![a-zA-Z])|--|(?<![a-zA-Z])-|-(?![a-zA-Z])|\\d+"," ");
			for (String token : line.split(" ")) {
				if (!stopwords.contains(token.toLowerCase()) && !token.isEmpty() && token != null) {
					word.set(token.toLowerCase());
					context.write(word, filename);
				}
			}								 
		}
	}
 
 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

	 @Override
		public void reduce(final Text key, final Iterable<Text> values,
				final Context context) throws IOException, InterruptedException {
		
		    HashSet<String> files = new HashSet<String>();

			for (Text value : values) {
				files.add(value.toString());
			}

			StringBuilder stringBuilder = new StringBuilder();
			
			for (String value : files) {
				if (stringBuilder.toString().isEmpty()) {
					stringBuilder.append(value.toString());
					}
				else {
					stringBuilder.append(", "+value);
				}				
			}
			if (!key.toString().isEmpty()) {
				if (files.size() == 1 ) {
					context.getCounter(CUSTOM_COUNTER.Counter_Unique_Words).increment(1);
				}
			}
		}
	 protected void cleanup(Context context)
             throws IOException, InterruptedException {
			Long final_count = new Long(5);
			final_count = context.getCounter(CUSTOM_COUNTER.Counter_Unique_Words).getValue();
            context.write(new Text("Number of words in unique file: "), new Text(String.valueOf(final_count)));
         }
	}
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "inverted_unique");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(Inverted_unique.class);
    job.setNumReduceTasks(1);    
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    conf.setBoolean("mapreduce.map.output.compress",true);
    conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        
    job.waitForCompletion(true);
 }
}       
