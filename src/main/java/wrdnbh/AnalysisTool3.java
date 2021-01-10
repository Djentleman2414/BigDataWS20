package wrdnbh;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wrdnbh.NachbarschaftPhase2.BucketMapValue;
import wrdnbh.NachbarschaftPhase2.SimilarityReducer;

public class AnalysisTool3 extends Configured implements Tool {
	
	public static class MinHashMapper extends Mapper<Object, Text, IntWritable, BucketMapValue> {

		private IntWritable outKey = new IntWritable();
		private BucketMapValue outValue = new BucketMapValue();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] hashStrings = value.toString().split("\t");
			outValue.setWord(hashStrings[0]);

			for (int i = 1; i < hashStrings.length; i++) {
				int hash = Integer.parseInt(hashStrings[i]);
				outValue.addNeighbor(hash);
			}
			context.write(outKey, outValue);
			outValue.reset();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName() + "_sndJob");
		
		FileInputFormat.addInputPath(job, new Path("wrdnbh/" + args[0] + "/temp"));
		FileOutputFormat.setOutputPath(job, new Path("wrdnbh/analysis/" + args[0] + "/final"));

		job.setJarByClass(AnalysisTool3.class);

		job.setMapperClass(AnalysisTool3.MinHashMapper.class);
		job.setReducerClass(SimilarityReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BucketMapValue.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new AnalysisTool3(), args));
	}

}
