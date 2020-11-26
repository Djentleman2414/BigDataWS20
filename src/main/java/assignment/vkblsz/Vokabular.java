package assignment.vkblsz;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.IntPairWritable;

public class Vokabular extends Configured implements Tool {

	public static class NGramMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final IntWritable outKey = new IntWritable();
		private final IntWritable outValue = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			if (values[0].contains("_")) {
				values[0] = values[0].split("_")[0];
			}
			outKey.set(Integer.parseInt(values[1]));
			outValue.set(1 << leastSignificant1(values[0].hashCode()));

			context.write(outKey, outValue);
		}
	}

	public static class VokabularCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		IntWritable value = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int combinedHash = 0;

			for (IntWritable tempValue : values) {
				combinedHash |= tempValue.get();
			}

			value.set(combinedHash);

			context.write(key, value);
		}
	}

	public static class VokabularReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private final IntWritable outValue = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int combinedHash = 0;

			for (IntWritable value : values) {
				combinedHash |= value.get();
			}

			outValue.set((int) flajoletMartinEstimate(leastSignificant1(combinedHash ^ (-1))));
			context.write(key, outValue);
		}
	}

	public static class IdentityMapper extends Mapper<Object, Text, Object, IntPairWritable> {

		private static final IntPairWritable outValue = new IntPairWritable();
		private static final IntWritable outKey = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			outValue.set(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
			context.write(outKey, outValue);
		}
	}

	public static class VokabularReducer2 extends Reducer<Object, IntPairWritable, IntWritable, IntWritable> {

		private static final IntWritable outKey = new IntWritable();
		private static final IntWritable outValue = new IntWritable();

		private static int numYears = 10;

		public void reduce(Object key, Iterable<IntPairWritable> values, Context context)
				throws IOException, InterruptedException {
			YearCountPair[] maxYears = new YearCountPair[numYears];
			for(int i = 0; i < numYears; i++) {
				maxYears[i] = new YearCountPair();
			}

			for (IntPairWritable value : values) {
				int i = 0;
				while (i < numYears && Integer.compare(maxYears[i].getCount(), value.getY()) < 0) {
					if (i > 0) {
						maxYears[i - 1].setYear(maxYears[i].getYear());
						maxYears[i - 1].setCount(maxYears[i].getCount());
					}
					i++;
				}
				if (i != 0) {
					maxYears[i - 1].setYear(value.getX());
					maxYears[i - 1].setCount(value.getY());
				}
			}

			for (YearCountPair entry : maxYears) {
				outKey.set(entry.getYear());
				outValue.set(entry.getCount());
				context.write(outKey, outValue);
			}
		}

		public static class YearCountPair {
			int year;
			int count;
			
			public YearCountPair() {
				
			}

			public YearCountPair(int a, int b) {
				year = a;
				count = b;
			}

			public void setYear(int a) {
				year = a;
			}

			public int getYear() {
				return year;
			}

			public void setCount(int a) {
				count = a;
			}

			public int getCount() {
				return count;
			}
		}
	}

	public static double flajoletMartinEstimate(int r) {
		return Math.pow(2, r) / 0.77351;
	}

	public static int leastSignificant1(int num) {
		int i = 0;
		while (num != 0) {
			num <<= 1;
			i++;
		}
		return (32 - i);
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new Vokabular(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output folder>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = getFirstJob(args[0], args[1]);
		int exitcode = job.waitForCompletion(true) ? 0 : -1;

		if (exitcode != 0)
			return exitcode;

		job = getSecondJob(args[1]);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	private Job getFirstJob(String input, String outputFolder) throws IOException {
		Job job = Job.getInstance(getConf(), Vokabular.class.getSimpleName());
		if (!outputFolder.endsWith("/"))
			outputFolder += "/";

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + "temp"));

		job.setJarByClass(Vokabular.class);

		job.setMapperClass(NGramMapper.class);
		job.setCombinerClass(VokabularCombiner.class);
		job.setReducerClass(VokabularReducer1.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(10);

		return job;
	}

	private Job getSecondJob(String folder) throws IOException {
		Job job = Job.getInstance(getConf(), Vokabular.class.getSimpleName());
		if (!folder.endsWith("/"))
			folder += "/";

		FileInputFormat.addInputPath(job, new Path(folder + "temp"));
		FileOutputFormat.setOutputPath(job, new Path(folder + "final"));

		job.setJarByClass(Vokabular.class);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(VokabularReducer2.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntPairWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		return job;
	}

}
