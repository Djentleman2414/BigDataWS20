package matmul;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import matmul.FirstPhase.LeftMatrixMapper;
import matmul.FirstPhase.MapKeyClass;
import matmul.FirstPhase.MatMulReducer;
import matmul.FirstPhase.MatrixEntry;
import matmul.FirstPhase.MatrixGroupingComparator;
import matmul.FirstPhase.MatrixPartitioner;
import matmul.FirstPhase.MatrixSortingComparator;
import matmul.FirstPhase.RightMatrixMapper;
import matmul.SecondPhase.IdentityMapper;
import matmul.SecondPhase.MatrixEntryReducer;
import types.IntPairWritable;

public class MatMul extends Configured implements Tool {

	public static final int MAX_BUCKET_SIZE = 2000000;
	public static final int MIN_BUCKET_SIZE = 20000;
	public static final int MAX_REDUCE_TASKS = 10;

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new MatMul(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input1> <input2> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration conf = getConf();

		int numOfBuckets = getMatrixDimensions(conf, args[0]);

		Job job = getFirstJob(conf, args[0], args[1], args[2], numOfBuckets);

		if (!job.waitForCompletion(true))
			return -1;

		job = getSecondJob(conf, args[2]);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * Bei dieser Funktion werden drei Fälle unterschieden.
	 * 
	 * 
	 * Fall 1: MIN_BUCKET_SIZE * MAX_REDUCE_TASKS < Anzahl Elemente
	 * 
	 * Die Bucketgröße entspricht MIN_BUCKET_SIZE, da kleinere Buckets den
	 * Mehraufwand nicht wert wären. Es werden soviele Buckets mit der Größe
	 * MIN_BUCKET_SIZE erstellt wie nötig. Die Anzahl Buckets entspricht in diesem
	 * Fall der Anzahl der Reducer
	 * 
	 * Fall 2: Anzahl Elemente < MAX_BUCKET_SIZE * MAX_REDUCE_TASKS
	 * 
	 * In diesem Fall sollen MAX_REDUCE_TASKS Reducer und Buckets erzeugt werden.
	 * Die Bucketgröße entspricht Anzahl Elemente / MAX_REDUCE_TASKS
	 * 
	 * Fall 3: Anzahl Elemente > MAX_BUCKET_SIZE * MAX_REDUCE_TASKS
	 * 
	 * In diesem Fall werden MAX_REDUCE_TASKS Reducer erstellt. Die Anzahl der
	 * Buckets entspricht Anzahl Elemente / MAX_BUCKET_SIZE da größere Buckets nicht
	 * lokal gehalten werden können.
	 * 
	 * 
	 */
	public int getMatrixDimensions(Configuration conf, String input0) {
		String[] inputArray = input0.split("\\.")[0].split("-");
		int rows = Integer.parseInt(inputArray[inputArray.length - 2]);
		int columns = Integer.parseInt(inputArray[inputArray.length - 1]);
		conf.setInt("NUM_OF_COLUMNS", columns);

		int numOfElements = rows * columns;
		int numOfBuckets = 1;
		int maxBucketSize = MAX_BUCKET_SIZE;

		if (numOfElements < (MIN_BUCKET_SIZE * MAX_REDUCE_TASKS)) {
			maxBucketSize = MIN_BUCKET_SIZE;
			numOfBuckets = (int) Math.ceil((double) numOfElements / MIN_BUCKET_SIZE);
		} else if (numOfElements < (MAX_BUCKET_SIZE * MAX_REDUCE_TASKS)) {
			maxBucketSize = (int) Math.ceil((double) numOfElements / MAX_REDUCE_TASKS);
			numOfBuckets = MAX_REDUCE_TASKS;
		} else {
			maxBucketSize = MAX_BUCKET_SIZE;
			numOfBuckets = (int) Math.ceil((double) numOfElements / MAX_BUCKET_SIZE);
		}

		conf.setInt("MAX_BUCKET_SIZE", maxBucketSize);
		conf.setInt("NUM_OF_BUCKETS", numOfBuckets);

		return numOfBuckets;
	}

	public Job getFirstJob(Configuration conf, String matrix0, String matrix1, String outputFolder, int numOfBuckets)
			throws IOException {
		Job job = Job.getInstance(conf, MatMul.class.getSimpleName());
		if (!outputFolder.endsWith("/"))
			outputFolder += "/";
		MultipleInputs.addInputPath(job, new Path(matrix0), TextInputFormat.class, LeftMatrixMapper.class);
		MultipleInputs.addInputPath(job, new Path(matrix1), TextInputFormat.class, RightMatrixMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + "tmp"));

		job.setJarByClass(MatMul.class);
		job.setReducerClass(MatMulReducer.class);

		job.setMapOutputKeyClass(MapKeyClass.class);
		job.setMapOutputValueClass(MatrixEntry.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setPartitionerClass(MatrixPartitioner.class);
		job.setGroupingComparatorClass(MatrixGroupingComparator.class);
		job.setSortComparatorClass(MatrixSortingComparator.class);

		job.setNumReduceTasks(Math.min(numOfBuckets, MAX_REDUCE_TASKS));

		return job;
	}

	public Job getSecondJob(Configuration conf, String outputFolder) throws IOException {
		Job job = Job.getInstance(conf, MatMul.class.getSimpleName());
		if (!outputFolder.endsWith("/"))
			outputFolder += "/";

		FileInputFormat.addInputPath(job, new Path(outputFolder + "tmp"));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + "result"));

		job.setJarByClass(MatMul.class);
		job.setMapperClass(IdentityMapper.class);
		job.setCombinerClass(MatrixEntryReducer.class);
		job.setReducerClass(MatrixEntryReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setNumReduceTasks(10);

		return job;
	}
}
