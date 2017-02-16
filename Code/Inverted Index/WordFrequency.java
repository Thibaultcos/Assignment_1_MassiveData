package inverted_frequency;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WordFrequency implements Writable {

	private Text File;
	private IntWritable freq;

	public WordFrequency() {
		this.File = new Text();
		this.freq = new IntWritable();
	}

	public void set(String docID, int freq) {
		this.File.set(docID);
		this.freq.set(freq);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		File.readFields(dataInput);
		freq.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		File.write(dataOutput);
		freq.write(dataOutput);
	}

	public Text getFile() {
		return File;
	}

	public IntWritable getFreq() {
		return freq;
	}
}
