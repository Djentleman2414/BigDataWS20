package matmul;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Iterables;

import types.IntPairWritable;

public class DuplicateIndexFinder extends Configured implements Tool {
	
	public static class IndexMapper extends Mapper<Object, Text, IntPairWritable, DoubleWritable> {
		
		IntPairWritable outKey = new IntPairWritable();
		DoubleWritable outValue = new DoubleWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			outKey.set(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
			outValue.set(Double.parseDouble(split[2]));
			//if(outKey.getX() >= 1000 || outKey.getY() >= 800 || outKey.getX() < 0 || outKey.getY() < 0)
				context.write(outKey, outValue);
		}
	}
	
	public static class IndexReducer extends Reducer<IntPairWritable, DoubleWritable, IntPairWritable, DoubleWritable> {
		
		public DoubleWritable outValue =  new DoubleWritable();
		
		public void reduce(IntPairWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
//			if(Iterables.size(values) == 1)
//				return;
			ArrayList<Double> list = new ArrayList<>();
			for(DoubleWritable value : values)
				list.add(value.get());
			
			if(list.size() > 1) {
				for(double d : list) {
					outValue.set(d);
					context.write(key, outValue);
				}
			}
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		
		FileInputFormat.addInputPath(job, new Path("matmul/" + args[0]));
		FileOutputFormat.setOutputPath(job, new Path("matmul/analysis/" + args[0]));
		
		job.setJarByClass(DuplicateIndexFinder.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new DuplicateIndexFinder(), args);
		System.exit(exit);
	}

}
