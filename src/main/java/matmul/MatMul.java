package matmul;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.IntDoubleWritable;
import types.IntPairWritable;
import types.IntTrippleWritable;

public class MatMul extends Configured implements Tool {

	protected static int leftRowCount = 0;
	protected static int rightColumnCount = 0;

	public static abstract class MatrixMapper extends Mapper<Object, Text, IntTrippleWritable, IntDoubleWritable> {
		protected IntTrippleWritable outKey = new IntTrippleWritable();
		protected IntDoubleWritable outValue = new IntDoubleWritable();

		protected int rowCount;
		protected int colCount;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			rowCount = conf.getInt("ROWCOUNT", 0);
			colCount = conf.getInt("COLCOUNT", 0);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] args = value.toString().split("\t");
			int rowIndex = Integer.parseInt(args[0]);
			int colIndex = Integer.parseInt(args[1]);
			double val = Double.parseDouble(args[2]);
			if (val != 0) {
				outValue.setDouble(val);
				writeToContext(rowIndex, colIndex, context);
			}
		}

		protected abstract void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException;
	}

	public static class LeftMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException {
			outKey.set(rowIndex, 0, colIndex);
			outValue.setInt(colIndex);
			for (int i = 0; i < colCount; i++) {
				outKey.setY(i);
				context.write(outKey, outValue);
			}
		}

	}

	public static class RightMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException {
			outKey.set(0, colIndex, rowIndex);
			outValue.setInt(rowIndex);
			for (int i = 0; i < rowCount; i++) {
				outKey.setX(i);
				context.write(outKey, outValue);
			}
		}
	}

	public static class MatMulReducer
			extends Reducer<IntTrippleWritable, IntDoubleWritable, IntPairWritable, DoubleWritable> {
		private IntPairWritable outKey = new IntPairWritable();
		private DoubleWritable outValue = new DoubleWritable();

		public void reduce(IntTrippleWritable key, Iterable<IntDoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			
			double temp = 0;
			double sum = 0;
			boolean toggle = true;
			int lastPos = 0;
			for (IntDoubleWritable value : values) {
				if (toggle || lastPos != value.getInt()) { // (L,0,1),(R,0,2),(L,1,2),(L,2,4),(R,2,2)  -> überspringen (R,1,...)
					temp = value.getDouble();
					lastPos = value.getInt();
					toggle = false;
				} else {
					sum += temp * value.getDouble();
					temp = 0;
					toggle = true;
				}
			}
			outKey.set(key.getX(), key.getY());
			outValue.set(sum);
			context.write(outKey, outValue);
		}
	}
	
	
	public static class MatMulPartitioner extends Partitioner<IntTrippleWritable, IntDoubleWritable> {

		@Override
		public int getPartition(IntTrippleWritable key, IntDoubleWritable value, int numPartitions) {
			final int prime = 31;
			int hashValue = prime * (key.getX() + key.getY());
			return hashValue % numPartitions;
		}
		
	}
	
	
	public static class MatMulGroupingComparator extends WritableComparator {
		
		public MatMulGroupingComparator() {
			super(IntTrippleWritable.class, true);
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			IntTrippleWritable left = (IntTrippleWritable) a;
			IntTrippleWritable right = (IntTrippleWritable) b;
			int dist = Integer.compare(left.getX(), right.getX());
			if (dist != 0)
				return dist;
			
			return Integer.compare(left.getY(), right.getY());
		}
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new MatMul(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// arguments: folder matrix0 matrix1 outputFolder
		if (args.length != 4) {
			System.err.printf("Usage: %s [generic options] <folder> <input1> <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = getConf();

		getMatrixDimensions(conf, args[1], args[2]);
		
		Job job = getJob(conf, args[0], args[1], args[2], args[3]);
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public void getMatrixDimensions(Configuration conf, String input0, String input1) {
		String[] input0Array = input0.split("\\.")[0].split("-");
		String[] input1Array = input1.split("\\.")[0].split("-");
		conf.setInt("ROWCOUNT", Integer.parseInt(input0Array[1]));
		conf.setInt("COLCOUNT", Integer.parseInt(input1Array[2]));
	}

	public Job getJob(Configuration conf, String inputFolder, String matrix0, String matrix1, String outputFolder)
			throws IOException {
		Job job = Job.getInstance(conf, MatMul.class.getSimpleName());
		if (!inputFolder.endsWith("/"))
			inputFolder += "/";
		if (!outputFolder.endsWith("/"))
			outputFolder += "/";
		MultipleInputs.addInputPath(job, new Path(inputFolder + matrix0), TextInputFormat.class, LeftMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputFolder + matrix1), TextInputFormat.class, RightMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + "result"));

		job.setJarByClass(MatMul.class);
		job.setReducerClass(MatMulReducer.class);

		job.setMapOutputKeyClass(IntTrippleWritable.class);
		job.setMapOutputValueClass(IntDoubleWritable.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setPartitionerClass(MatMulPartitioner.class);
		job.setGroupingComparatorClass(MatMulGroupingComparator.class);

		job.setNumReduceTasks(10);

		return job;
	}
}
