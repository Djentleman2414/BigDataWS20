package matmul;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.DoublePairWritable;
import types.IntPairWritable;
import types.IntTrippleWritable;

public class MatMul extends Configured implements Tool {
	
	protected static int leftRowCount = 0;
	protected static int rightColumnCount = 0;
	
	public static abstract class MatrixMapper extends Mapper<Object, Text, IntTrippleWritable, DoubleWritable> {
		protected IntTrippleWritable outKey =  new IntTrippleWritable();
		protected DoubleWritable outValue = new DoubleWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] args = value.toString().split("\t");
			int rowIndex = Integer.parseInt(args[0]);
			int colIndex = Integer.parseInt(args[1]);
			double val = Double.parseDouble(args[2]);
			if(val != 0) {
				outValue.set(val);
				writeToContext(rowIndex, colIndex, context);
			}		
		}
		
		protected abstract void writeToContext(int rowIndex, int colIndex, Context context) throws IOException, InterruptedException;
	}
	
	public static class LeftMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context) throws IOException, InterruptedException {
			outKey.set(rowIndex, colIndex, 0);
			for(int i = 0; i < rightColumnCount; i++) {
				outKey.setZ(i);
				context.write(outKey, outValue);
			}
		}
		
	}
	
	public static class RightMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context) throws IOException, InterruptedException {
			outKey.set(0, rowIndex, colIndex);
			for(int i = 0; i < leftRowCount; i++) {
				outKey.setX(i);
				context.write(outKey, outValue);
			}		
		}
	}

	public static class MatMulReducer1 extends Reducer<IntTrippleWritable, DoubleWritable, IntPairWritable, DoublePairWritable> {
		private IntPairWritable outKey = new IntPairWritable();
		private DoublePairWritable outValue = new DoublePairWritable();
		
		public void reduce(IntTrippleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			boolean toggle = true;
			double val1 = 0;
			double val2 = 0;
			
			for(DoubleWritable value : values) {
				if(toggle) {
					val1 = value.get();
					toggle = !toggle;
				} else {
					val2 = value.get();
				}
				
				if(val2 != 0) {
					outKey.setX(key.getX());
					outKey.setY(key.getZ());
					outValue.set(val1, val2);
					context.write(outKey, outValue);
				}
			}
		}
	}
	
	// 2nd job
	
	public static class MatMulPartitioner extends Partitioner<IntPairWritable, DoubleWritable> {

		@Override
		public int getPartition(IntPairWritable key, DoubleWritable value, int numPartitions) {
			return Integer.hashCode(key.getX()) % numPartitions;
		}
		
	}
	
	public static class MatMulMapper2 extends Mapper<Object, Text, IntPairWritable, DoubleWritable> {
		
		private IntPairWritable outKey = new IntPairWritable();
		private DoubleWritable outValue = new DoubleWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] args = value.toString().split("\t");
			int rowIndex = Integer.parseInt(args[0]);
			int colIndex = Integer.parseInt(args[1]);
			double val1 = Double.parseDouble(args[2]);
			double val2 = Double.parseDouble(args[3]);
			
			outKey.set(rowIndex, colIndex);
			outValue.set(val1 * val2);
			
			context.write(outKey, outValue);
		}
	}
	
	public static class MatMulReducer2 extends Reducer<IntPairWritable, DoubleWritable, IntPairWritable, DoubleWritable> {
		
		private IntPairWritable outKey = new IntPairWritable();
		private DoubleWritable outValue = new DoubleWritable();
		
		private int previousColIndex = -1;
		
		public void reduce(IntPairWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			outKey.setX(key.getX());
			outValue.set(0);
			if(previousColIndex > key.getY()) {
				while(previousColIndex < MatMul.rightColumnCount - 1) {
					outKey.setY(++previousColIndex);				
					context.write(outKey, outValue);
				}
				previousColIndex = -1;
			}
			
			while(previousColIndex + 1 != key.getY()) {				
				outKey.setY(++previousColIndex);
				context.write(outKey, outValue);
			}
			
			double sum = 0;
			for(DoubleWritable value : values) {
				sum += value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
			previousColIndex++;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			outValue.set(0);
			while(previousColIndex < MatMul.rightColumnCount - 1) {
				outKey.setY(++previousColIndex);			
				context.write(outKey, outValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new MatMul(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// arguments: folder matrix0 matrix1 outputFolder
		if (args.length!=4) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration conf = getConf();
		
		getMatrixDimensions(args[1], args[2]);
		
		Job firstJob = getFirstJob(conf, args[0], args[1], args[2], args[3]);
		firstJob.waitForCompletion(true);
		
		Job secondJob = getSecondJob(conf, args[3]);
			
		return secondJob.waitForCompletion(true) ? 0 : -1;
	}
	
	public void getMatrixDimensions(String input0, String input1) {
		String[] input0Array = input0.split(".")[0].split("-");
		String[] input1Array = input1.split(".")[0].split("-");
		MatMul.leftRowCount = Integer.parseInt(input0Array[1]);
		MatMul.rightColumnCount = Integer.parseInt(input1Array[2]);
	}
	
	public Job getFirstJob(Configuration conf, String inputFolder, String matrix0, String matrix1, String outputFolder) throws IOException {
		Job job = Job.getInstance(conf, MatMul.class.getSimpleName());
		if(!inputFolder.endsWith("/"))
			inputFolder += "/";
		if(!outputFolder.endsWith("/"))
			outputFolder += "/";
		MultipleInputs.addInputPath(job, new Path(inputFolder + matrix0), TextInputFormat.class, LeftMapper.class);
		MultipleInputs.addInputPath(job, new Path(inputFolder + matrix1), TextInputFormat.class, RightMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + "temp"));
			
		job.setJarByClass(MatMul.class);
		job.setReducerClass(MatMulReducer1.class);
		
		job.setMapOutputKeyClass(IntTrippleWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoublePairWritable.class);
		
		job.setNumReduceTasks(10);
		
		return job;
	}
	
	public Job getSecondJob(Configuration conf, String folder) throws IOException {
		if(!folder.endsWith("/"))
			folder += "/";
		
		Job job = Job.getInstance(conf, MatMul.class.getSimpleName());
		FileInputFormat.addInputPath(job, new Path(folder + "temp/*"));
		FileOutputFormat.setOutputPath(job, new Path(folder + "final/"));
		
		job.setJarByClass(MatMul.class);
		job.setMapperClass(MatMulMapper2.class);;
		job.setReducerClass(MatMulReducer2.class);
		
		job.setPartitionerClass(MatMulPartitioner.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(10);
		
		return job;
	}

}
