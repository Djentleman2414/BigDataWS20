package wrdnbh;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import types.TextIntWritable;
import wrdnbh.NachbarschaftPhase1.CombinerGroupingComparator;
import wrdnbh.NachbarschaftPhase1.NeighborCountCombiner;
import wrdnbh.NachbarschaftPhase1.NeighborCountReducer;
import wrdnbh.NachbarschaftPhase1.NeighborCounter;
import wrdnbh.NachbarschaftPhase1.OutArrayWritable;
import wrdnbh.NachbarschaftPhase1.SentenceMapper;
import wrdnbh.NachbarschaftPhase1.WordGroupingComparator;
import wrdnbh.NachbarschaftPhase1.WordPartitioner;
import wrdnbh.NachbarschaftPhase2.BucketMapValue;
import wrdnbh.NachbarschaftPhase2.MinHashMapper;
import wrdnbh.NachbarschaftPhase2.SimilarityReducer;

public class Nachbarschaft extends Configured implements Tool {

	public final static int MAX_AVERAGE_WORDS_PER_REDUCER = 1000000;
	public final static double ASSUMED_AVERAGE_JACCARD_INDEX = 0.3;
	public static final double MIN_COLLISION_RATE = 0.8;

	public final static String MAX_DISTANCE = "max.distance";
	public final static String MIN_NEIGHBOR_COUNT = "min.count";
	public final static String NUM_OF_BANDS = "num.bands";
	public final static String NUM_OF_HASHES_PER_BAND = "hashes.per.band";
	public final static String MIN_JACCARD_INDEX = "min.jaccard";

	@Override
	public int run(String[] args) throws Exception {
		// es gibt 3 Argumente, input, outputFolder und ein Flag der angibt, ob es sich
		// um den kleinen oder großen Datensatz handelt. Letzteres ist dafür gedacht
		// nicht immer die volle Anzahl an Reducern zu verwenden (wenn man einfach nur testen möchte)

		if (!args[1].endsWith("/"))
			args[1] += '/';

		int numReduceTasks = args[2] == "t" ? 64 : 10;

		Random r = new Random();
		Configuration conf = getConf();
		conf.setInt(MAX_DISTANCE, 3); // set k
		conf.setInt(MIN_NEIGHBOR_COUNT, 10); // set h
		conf.setInt("SEED", r.nextInt());
		conf.setDouble(MIN_JACCARD_INDEX, 0.5);

		Job job = getFirstJob(args, conf, numReduceTasks);

		if (!job.waitForCompletion(true))
			return -1;

		long wordCount = job.getCounters()
				.findCounter("org.apache.hadoop.mapreduce.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();

		setBands(wordCount, conf);

		job = getSecondJob(args[1], conf, numReduceTasks);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/*
	 * Sei s die durchschnittliche Jaccard-Ähnlichkeit. Die Wahrscheinlichkeit, dass
	 * 2 Mengen in einem Band unterschiedlich sind liegt bei (1-s^r). Die
	 * Wahrscheinlichkeit, dass l+1 (~= l, da l >> 1) Werte in einem Band
	 * unterschiedlich sind, liegt bei (1-s^r)^l Wenn man jetzt annimmt, dass dies
	 * ab einem L mit (1-s^r)^L < 0.0001 nicht mehr vorkommt (eine sehr vereinfachte
	 * Annahme), dann gibt es L verschiedene Buckets in einem Band. Dieses L setzt
	 * man dann in m = n / L ein. wobei n die Anzahl aller Wörter ist und demnach m
	 * die maximale Anzahl an Wörtern pro Bucket. m kann man dann auf eine Zahl
	 * (hier 1e6) festelegen und so die Mindestanzahl der Hashes pro band (r)
	 * bestimmen. Über diese Zahl kommt man dann wiederum an die Anzahl der Bänder,
	 * sodass 2 Worte mit der Wahrscheinlichkeit MIN_COLLISION_RATE nicht in allen
	 * Bändern verschieden sind.
	 */
	public void setBands(long numOfWords, Configuration conf) {
		double interValue = Math.log(1 - Math.pow(10000, -(double) MAX_AVERAGE_WORDS_PER_REDUCER / numOfWords));
		interValue /= Math.log(ASSUMED_AVERAGE_JACCARD_INDEX);
		int r = Math.max(5, (int) Math.round(interValue));
		interValue = Math.log(1 - MIN_COLLISION_RATE);
		interValue /= Math.log(1 - Math.pow(0.5, r));
		int b = Math.max(52, (int) Math.round(interValue));

		conf.setInt(NUM_OF_BANDS, b);
		conf.setInt(NUM_OF_HASHES_PER_BAND, r);

		System.out.println("b:" + b + ", r:" + r);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Nachbarschaft(), args));
	}

	private Job getFirstJob(String[] args, Configuration conf, int numReduceTasks) throws IOException {
		Job job = Job.getInstance(conf, getClass().getSimpleName() + "_firstPhase");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "temp"));

		job.setJarByClass(Nachbarschaft.class);

		job.setMapperClass(SentenceMapper.class);
		job.setCombinerClass(NeighborCountCombiner.class);
		job.setReducerClass(NeighborCountReducer.class);

		job.setMapOutputKeyClass(TextIntWritable.class);
		job.setMapOutputValueClass(NeighborCounter.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OutArrayWritable.class);

		job.setPartitionerClass(WordPartitioner.class);
		job.setSortComparatorClass(CombinerGroupingComparator.class);
		job.setCombinerKeyGroupingComparatorClass(CombinerGroupingComparator.class);
		job.setGroupingComparatorClass(WordGroupingComparator.class);

		job.setNumReduceTasks(numReduceTasks);

		return job;
	}

	private Job getSecondJob(String folder, Configuration conf, int numReduceTasks) throws IOException {
		Job job = Job.getInstance(conf, getClass().getSimpleName() + "_sndJob");

		FileInputFormat.addInputPath(job, new Path(folder + "temp"));
		FileOutputFormat.setOutputPath(job, new Path(folder + "final"));

		job.setJarByClass(Nachbarschaft.class);

		job.setMapperClass(MinHashMapper.class);
		job.setReducerClass(SimilarityReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BucketMapValue.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(numReduceTasks);

		return job;
	}

}
