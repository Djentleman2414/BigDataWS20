package crkhsh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AlphabetGenerator {

	public static final String ALPHABETSIZE = "alphabetsize";

	public static class CharWritable implements WritableComparable<CharWritable> {

		private char c;

		public CharWritable() {}
		
		public CharWritable(char c) {
			this.c = c;
		}

		public char get() {
			return c;
		}

		public void set(char c) {
			this.c = c;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeChar(c);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			c = in.readChar();
		}

		@Override
		public int compareTo(CharWritable o) {
			return Character.compare(c, o.c);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + c;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CharWritable other = (CharWritable) obj;
			if (c != other.c)
				return false;
			return true;
		}
		
		public String toString() {
			return "" + c;
		}

	}

	public static class AlphabetInputFormat extends InputFormat<LongWritable, CharWritable> {

		@Override
		public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
			int numOfSplits = context.getConfiguration().getInt(ALPHABETSIZE, 1);
			ArrayList<InputSplit> splits = new ArrayList<>();
			for (int i = 0; i < numOfSplits; i++) {
				AlphabetSplit as = new AlphabetSplit();
				as.setShard(65 + i);
				splits.add(as);
			}
			return splits;
		}

		@Override
		public RecordReader<LongWritable, CharWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			AlphabetRecordReader arr = new AlphabetRecordReader();
			arr.initialize(split, context);
			ArrayList<Character> l = new ArrayList<>();
			char c = (char) (((AlphabetSplit) split).getShard());
			l.add(c);
			Iterator<Character> iter = l.iterator();
			arr.setIterator(iter);
			return arr;
		}
	}
	
	public static class AlphabetSplit extends InputSplit implements Writable {
		
		private int shard;
		
		public void setShard(int id) {
			this.shard=id;
		}
		
		public int getShard() {
			return shard;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(shard);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			shard = in.readInt();
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}
		
	}

	public static class AlphabetRecordReader extends RecordReader<LongWritable, CharWritable> {
		
		private LongWritable key;
		private CharWritable value;
		private Iterator<Character> iter;
		
		public void setIterator(Iterator<Character> iter) {
			this.iter = iter;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			key = new LongWritable();
			value = new CharWritable();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			boolean valid = iter.hasNext();
			if (valid) {
				char c = iter.next();
				key.set((int) c - 65); 
				value.set(c);
			}
			return valid;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public CharWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (iter.hasNext()) ? 0 : 1; // simplified
		}

		@Override
		public void close() throws IOException {
			// no files were opened		
		}	
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		Path out = new Path("alphabet");
		
		conf.setInt(ALPHABETSIZE, 3);
		
		job.setNumReduceTasks(0);
		job.setJarByClass(AlphabetGenerator.class);
		job.setInputFormatClass(AlphabetInputFormat.class);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(CharWritable.class);
		
		job.waitForCompletion(true);
	}
}
