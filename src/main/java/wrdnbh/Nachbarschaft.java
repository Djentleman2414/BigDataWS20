package wrdnbh;

import java.io.IOException;
import java.util.Random;

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
import wrdnbh.NachbarschaftPhase1.NeighborCountCombiner;
import wrdnbh.NachbarschaftPhase1.NeighborCountReducer;
import wrdnbh.NachbarschaftPhase1.NeighborCounter;
import wrdnbh.NachbarschaftPhase1.OutArrayWritable;
import wrdnbh.NachbarschaftPhase1.SentenceMapper;
import wrdnbh.NachbarschaftPhase1.WordGroupingComparator;
import wrdnbh.NachbarschaftPhase2.BucketMapValue;
import wrdnbh.NachbarschaftPhase2.MinHashMapper;
import wrdnbh.NachbarschaftPhase2.SimilarityReducer;

public class Nachbarschaft extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if(!args[1].endsWith("/"))
			args[1] += '/';
		
		Random r = new Random();
		getConf().setInt("MAX_DISTANCE", 3); // set k
		getConf().setInt("MIN_COUNT", 10); // set h
		getConf().setInt("NUM_OF_HASHES", 500);
		getConf().setInt("NUM_OF_BANDS", 100);
		getConf().setInt("SEED", r.nextInt());
		getConf().setDouble("MIN_JACCARD", 0.5);
		
		Job job = getFirstJob(args);
		
		if(!job.waitForCompletion(true))
			return -1;
	
		job = getSecondJob(args[1]);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Nachbarschaft(), args));
	}
	
	private Job getFirstJob(String[] args) throws IOException {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName() + "_firstPhase");
			
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
		
		job.setGroupingComparatorClass(WordGroupingComparator.class);
		
		job.setNumReduceTasks(10);
		
		return job;
	}
	
	private Job getSecondJob(String folder) throws IOException {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName() + "_sndJob");
		
		FileInputFormat.addInputPath(job, new Path(folder + "temp"));
		FileOutputFormat.setOutputPath(job, new Path(folder + "final"));
		
		job.setJarByClass(Nachbarschaft.class);
		
		job.setMapperClass(MinHashMapper.class);
		job.setReducerClass(SimilarityReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BucketMapValue.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(10);
		
		return job;
	}

}
