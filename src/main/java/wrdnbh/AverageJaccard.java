package wrdnbh;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import wrdnbh.NachbarschaftPhase2.BucketMapValue;
import wrdnbh.NachbarschaftPhase2.SimilarityReducer.NeighborSet;

public class AverageJaccard extends Configured implements Tool {

	public static class AlphabeticBucketMapper extends Mapper<Object, Text, IntWritable, BucketMapValue> {

		IntWritable outKey = new IntWritable();
		BucketMapValue outValue = new BucketMapValue();
		
		public void setup(Context c) {
			outValue.setWord("");
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] hashStrings = value.toString().split("\t");
			
			for (int i = 1; i < hashStrings.length; i++) {
				int hash = Integer.parseInt(hashStrings[i]);
				outValue.addNeighbor(hash);
			}
			context.write(outKey, outValue);
			outValue.reset();
		}
	}
	
	public static class JaccardReducer extends Reducer<IntWritable, BucketMapValue, DoubleWritable, IntWritable> {
		
		double jaccardSum;
		int numOfComparisons;
		
		int entryCount;
		
		private NeighborSet[] neighborSets = new NeighborSet[10000];
		
		public void reduce(IntWritable key, Iterable<BucketMapValue> values, Context context) {
			entryCount = 0;
			
			for (BucketMapValue value : values) {
				if (entryCount == neighborSets.length)
					expandNeighborSetArray();
				if (neighborSets[entryCount] == null)
					neighborSets[entryCount] = new NeighborSet(value.getWord(), value.getEntryCount(),
							value.getNeighbors());
				else
					neighborSets[entryCount].set(value.getWord(), value.getEntryCount(), value.getNeighbors());
				entryCount++;
			}
			
			for(int i = 0; i < entryCount - 1; i++) {
				for(int j = i + 1; j < entryCount; j++) {
					jaccardSum += neighborSets[i].jaccard(neighborSets[j]);
					numOfComparisons++;
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new DoubleWritable(jaccardSum/numOfComparisons), new IntWritable());
		}
		
		private void expandNeighborSetArray() {
			NeighborSet[] neighborSets = new NeighborSet[entryCount + 1000];
			System.arraycopy(this.neighborSets, 0, neighborSets, 0, entryCount);
			this.neighborSets = neighborSets;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "avgJaccard");
		
		FileInputFormat.addInputPath(job, new Path("/user/big10/wrdnbh/small_1/temp"));
		FileOutputFormat.setOutputPath(job, new Path("/user/big10/wrdnbh/avgJaccard/"));
		
		job.setJarByClass(AverageJaccard.class);
		
		job.setMapperClass(AlphabeticBucketMapper.class);
		job.setReducerClass(JaccardReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BucketMapValue.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
			
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new AverageJaccard(), args);
		System.exit(status);
	}

}
