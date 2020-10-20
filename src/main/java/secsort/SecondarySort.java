package secsort;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import types.IntPairFullComparator;
import types.IntPairPrefixComparator;
import types.IntPairPrefixPartitioner;
import types.IntPairWritable;

/**
 * secondary sort
 */

public class SecondarySort {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, SecondarySort.class.getSimpleName());

		job.setJarByClass(SecondarySort.class);

		job.setPartitionerClass(IntPairPrefixPartitioner.class);
		job.setGroupingComparatorClass(IntPairPrefixComparator.class);
		// not necessary, as compareTo of IntWritable does the same
		job.setSortComparatorClass(IntPairFullComparator.class);

		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static class MyMapper extends Mapper<LongWritable, Text, IntPairWritable, DoubleWritable> {
		private Random r = new Random();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int x = r.nextInt(10), y = r.nextInt(10);
			context.write(new IntPairWritable(x, y), new DoubleWritable(r.nextInt(10) + 0.1 * y));
		}
	}

	public static class MyReducer extends Reducer<IntPairWritable, DoubleWritable, Text, Text> {
		@Override
		public void reduce(IntPairWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			String s = "";
			for (DoubleWritable w : values)
				s += w.toString() + " ";
			context.write(new Text(key.toString()), new Text(s));
		}
	}

}
