package multiin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Provide two input files (args[0], args[1]) which are handled by different
 * mappers but enter the same shuffle and reducer.
 * 
 * Here: Mapper1 reports #columns (csv), Mapper2 reports line length, both
 * depending on file position. If the same file is provided twice, the positions
 * should match and the output should show both numbers for each file position.
 * 
 * However, internally addInputPath stores the different inputs in the
 * configuration variable and if we provide two inputs with the same file and
 * format, the old entry gets overwritten. Just provide two different input file
 * names.
 */

public class MRMultiInput extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		int status = -1;
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, MRMultiInput.class.getSimpleName());
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyMapper2.class);
		job.setJarByClass(MRMultiInput.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
		status = job.isSuccessful() ? 0 : -1;
		return status;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MRMultiInput(), args);
	}

	public static class MyMapper1 extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("M1: " + key + " " + value);
			String[] x = value.toString().split(",");
			context.write(key, new IntWritable(x.length));
		}
	}

	public static class MyMapper2 extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("M2: " + key + " " + value);
			context.write(key, new IntWritable(value.toString().length()));
		}
	}

	public static class MyReducer extends Reducer<LongWritable, IntWritable, Text, Text> {
		@Override
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			String s = "";
			for (IntWritable w : values)
				s += w.toString() + " ";
			context.write(new Text(key.toString()), new Text(s));
		}
	}

}
