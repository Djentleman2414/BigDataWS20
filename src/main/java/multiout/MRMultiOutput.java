package multiout;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * example of using multiple outputs.. generates:
 * 
 * <pre>
 * -rw-r--r--   3 hoeppner hdfs          0 2020-09-03 23:03 mmm/_SUCCESS
 * -rw-r--r--   3 hoeppner hdfs        843 2020-09-03 23:03 mmm/part-r-00000
 * -rw-r--r--   3 hoeppner hdfs        167 2020-09-03 23:03 mmm/sec0-r-00000
 * -rw-r--r--   3 hoeppner hdfs        169 2020-09-03 23:03 mmm/sec2-r-00000
 * -rw-r--r--   3 hoeppner hdfs        169 2020-09-03 23:03 mmm/sec4-r-00000
 * -rw-r--r--   3 hoeppner hdfs        169 2020-09-03 23:03 mmm/sec6-r-00000
 * -rw-r--r--   3 hoeppner hdfs        169 2020-09-03 23:03 mmm/sec8-r-00000
 * -rw-r--r--   3 hoeppner hdfs       2306 2020-09-03 23:03 mmm/seq_a-r-00000
 * -rw-r--r--   3 hoeppner hdfs       2306 2020-09-03 23:03 mmm/seq_b-r-00000
 * -rw-r--r--   3 hoeppner hdfs       1043 2020-09-03 23:03 mmm/text-r-00000
 * </pre>
 * 
 * wobei seq_?-Dateien SequenceFiles, der Rest Textfiles sind
 */

public class MRMultiOutput {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance();

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(MRMultiOutput.class);
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MOReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		// Defines additional single text based output 'text' for the job
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, LongWritable.class, Text.class);
		// Defines additional sequence-file based output 'sequence' for the job
		MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, Text.class, LongWritable.class);
		job.setNumReduceTasks(1);

		job.waitForCompletion(true);
	}

	public static class MyMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class MOReducer extends Reducer<LongWritable, Text, IntWritable, IntWritable> {
		private MultipleOutputs<IntWritable, IntWritable> mos;

		public void setup(Context context) {
			mos = new MultipleOutputs<IntWritable, IntWritable>(context);
		}

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// standard => part-r-00000
			context.write(new IntWritable((int) key.get()),new IntWritable(42));
			// standard with multiple files => secX-r-00000
			mos.write(new IntWritable((int) key.get()),new IntWritable(42), generateFileName(key));
			// selected channel 'text' => text-r-00000
			mos.write("text", key, new Text("test"));
			// selected channel 'seq' with additional path => seq_?-r-00000
			mos.write("seq", new Text("hello"), key, "seq_a"); // last param: base output path
			mos.write("seq", new Text("world"), key, "seq_b");
		}

		public void cleanup(Context c) throws IOException, InterruptedException {
			mos.close();
		}

		private String generateFileName(LongWritable key) {
			return "sec"+(key.get()%10);
		}
	}
}
