package testing;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.IntPairWritable;

public class KeyTest extends Configured implements Tool{

	public static class MyMapper extends Mapper<Object, Text, IntPairWritable, IntWritable> {
		
		IntPairWritable outKey = new IntPairWritable();
		IntWritable outValue = new IntWritable();
		
		public void setup(Context context) {
			outKey.setX(0);
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			int i = Integer.parseInt(value.toString());
			outKey.setY(i);
			outValue.set(i);
			context.write(outKey, outValue);
		}
	}
	
	public static class MyReducer extends Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable> {
		
		IntWritable outKey = new IntWritable();
		
		public void reduce(IntPairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			outKey.set(-1);
			context.write(outKey, outKey);
			for(IntWritable value : values) {
				outKey.set(key.getY());
				context.write(outKey, value);
			}
		}
	}
	
	public static class KeyGroupingComparator extends WritableComparator {
		
		public KeyGroupingComparator() {
			super(IntPairWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			IntPairWritable left = (IntPairWritable) a;
			IntPairWritable right = (IntPairWritable) b;
			return Integer.compare(left.getX(), right.getX());
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		
		FileInputFormat.addInputPath(job, new Path(args[0] + "/testFile"));
		FileOutputFormat.setOutputPath(job, new Path(args[0] + "/out"));
		
		job.setJarByClass(KeyTest.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setGroupingComparatorClass(KeyGroupingComparator.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KeyTest(), args));
	}

}
