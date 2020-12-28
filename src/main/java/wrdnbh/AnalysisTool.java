package wrdnbh;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AnalysisTool extends Configured implements Tool {
	
	public static class AnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, one);
		}
	}
	
	public static class AnalysisCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable outValue = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for(IntWritable value : values) {
				sum += value.get();
			}
			
			outValue.set(sum);
			
			context.write(key, outValue);
		}
	}
	
	public static class AnalysisReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
		
		private HashMap<Integer, Integer> frequencyMap = new HashMap<>();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {		
			int sum = 0;
			
			for(IntWritable value : values) {
				sum += value.get();
			}
			
			if(!frequencyMap.containsKey(sum))
				frequencyMap.put(sum, 1);
			else
				frequencyMap.put(sum, frequencyMap.get(sum) + 1);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			
			for(Entry<Integer,Integer> e : frequencyMap.entrySet()) {
				key.set(e.getKey());
				value.set(e.getValue());
				context.write(key, value);
			}
		}	
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), AnalysisTool.class.getSimpleName());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(AnalysisTool.class);
		
		job.setMapperClass(AnalysisMapper.class);
		job.setReducerClass(AnalysisReducer.class);
		job.setCombinerClass(AnalysisCombiner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new AnalysisTool(), args);
		System.exit(exit);
	}

}
