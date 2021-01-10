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

	public static final int MAX_BUCKET_SIZE = 50000000; // 5 * 10^7
	public static final int MIN_BUCKET_SIZE = 100000;
	public static final int MAX_REDUCE_TASKS = 32;

	public static final String CONF_NUM_OF_ROWS_LEFT = "num.of.rows.left";
	public static final String CONF_NUM_OF_COLUMNS_LEFT = "num.of.columns.left";
	public static final String CONF_NUM_OF_ROWS_RIGHT = "num.of.rows.left";
	public static final String CONF_NUM_OF_COLUMNS_RIGHT = "num.of.columns.left";
	public static final String CONF_MAX_BUCKET_SIZE = "max.bucket.size";
	public static final String CONF_NUM_OF_BUCKETS = "num.of.buckets";
	public static final String CONF_ROWS_PER_BUCKET = "rows.per.bucket";

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

		Job job;
		Configuration conf = getConf();

		int[] dimensions = parseMatrixDimensions(conf, args[0], args[1]);
		boolean small = dimensions[1] + dimensions[3] <= MAX_BUCKET_SIZE;

		/**
		 * Bei kleinen Matritzen wird mit einer Phase gearbeitet. Bei großen Matrizen
		 * wird bei zwei Phasen gearbeitet.
		 */
		int numOfBuckets;
		if (small) {
			numOfBuckets = getNumerOfBucketsSmall(dimensions, conf);
			job = getSmallJob(conf, args[0], args[1], args[2], numOfBuckets);
		} else {
			numOfBuckets = getNumberOfBuckets(dimensions, conf);
			job = getFirstJob(conf, args[0], args[1], args[2], numOfBuckets);
		}

		int err = job.waitForCompletion(true) ? 0 : -1;

		if (small || err != 0)
			return err;

		job = getSecondJob(conf, args[2]);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public int[] parseMatrixDimensions(Configuration conf, String input0, String input1) {
		int[] dimensions = new int[4];
		// input ist von der Form "folders/MA-rows-columns.txt
		// Die Zeile ist also immer an vorletzer Stelle und die
		// Spalte an vorvorletzert Stelle im Array
		// Man kann nicht von vorne z�hlen, da in den Namen
		// der Order auch Bindestriche vorkommen k�nnen.
		String[] leftInputArray = input0.split("\\.")[0].split("-");
		String[] rightInputArray = input0.split("\\.")[0].split("-");
		int rowsLeft = Integer.parseInt(leftInputArray[leftInputArray.length - 2]);
		int columnsLeft = Integer.parseInt(leftInputArray[leftInputArray.length - 1]);
		int rowsRight = Integer.parseInt(rightInputArray[leftInputArray.length - 2]);
		int columnsRight = Integer.parseInt(rightInputArray[leftInputArray.length - 1]);
		conf.setInt(CONF_NUM_OF_ROWS_LEFT, rowsLeft);
		conf.setInt(CONF_NUM_OF_COLUMNS_LEFT, columnsLeft);
		conf.setInt(CONF_NUM_OF_ROWS_RIGHT, rowsRight);
		conf.setInt(CONF_NUM_OF_COLUMNS_RIGHT, columnsRight);
		dimensions[0] = rowsLeft;
		dimensions[1] = columnsLeft;
		dimensions[2] = rowsRight;
		dimensions[3] = columnsRight;

		return dimensions;

	}

	/*
	 * Es wird bestimmt, wie viele Zeilen der linken Matrix und der Ergebnismatrix gleichzeitig
	 * Hauptspeicher gehalten werden k�nnen, daraus ergibt sich die Zahl der Zeilen aus der linken
	 * Matrix die an den selben Reducer geschickt werden (und so auch die Anzahl der Buckts)
	 * Um die Parallelit�t ausnutzen werden aber maximal 1/10 aller Zeilen an den selben Reducer geschickt
	 * (bei sehr sehr kleinen Matrizen f�hrt dies zu einer Verlangsamung)
	 */
	public int getNumerOfBucketsSmall(int[] dimensions, Configuration conf) {
		int rowsPerBucket = Math.min(MAX_BUCKET_SIZE / (dimensions[1] + dimensions[3]), dimensions[0] / 10);
		int numOfBuckets = dimensions[0] / rowsPerBucket + 1;

		conf.setInt(CONF_NUM_OF_BUCKETS, numOfBuckets);
		conf.setInt(CONF_ROWS_PER_BUCKET, rowsPerBucket);

		return numOfBuckets;
	}

	/**
	 * Hier wird dynamisch die Anzahl der Reducer Tasks, die Anzahl der Buckets und
	 * die Größe der Buckets berechnet. Bei dieser Funktion werden drei Fälle
	 * unterschieden.
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
	 */
	public int getNumberOfBuckets(int[] dimensions, Configuration conf) {
		int numOfElements = dimensions[0] * dimensions[1];
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

		conf.setInt(CONF_MAX_BUCKET_SIZE, maxBucketSize);
		conf.setInt(CONF_NUM_OF_BUCKETS, numOfBuckets);

		return numOfBuckets;
	}

	public Job getSmallJob(Configuration conf, String matrix0, String matrix1, String outputFolder, int numOfBuckets)
			throws IOException {
		Job job = Job.getInstance(conf, MatMul.class.getSimpleName());
		MultipleInputs.addInputPath(job, new Path(matrix0), TextInputFormat.class,
				AlternativeMapReduce.LeftMatrixMapper.class);
		MultipleInputs.addInputPath(job, new Path(matrix1), TextInputFormat.class,
				AlternativeMapReduce.RightMatrixMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setJarByClass(MatMul.class);
		job.setReducerClass(AlternativeMapReduce.MatMulReducer.class);

		job.setMapOutputKeyClass(MapKeyClass.class);
		job.setMapOutputValueClass(MatrixEntry.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setPartitionerClass(MatrixPartitioner.class);
		job.setGroupingComparatorClass(MatrixGroupingComparator.class);
		job.setSortComparatorClass(MatrixSortingComparator.class);

		job.setNumReduceTasks(Math.min(numOfBuckets, MAX_REDUCE_TASKS));

		return job;
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
