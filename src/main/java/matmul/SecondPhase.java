package matmul;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPairWritable;

public class SecondPhase {
	
	public static class IdentityMapper extends Mapper<Object, Text, IntPairWritable, DoubleWritable> {
		
		private IntPairWritable outKey = new IntPairWritable();
		private DoubleWritable outValue = new DoubleWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t");
			outKey.set(Integer.parseInt(input[0]), Integer.parseInt(input[1]));
			outValue.set(Double.parseDouble(input[2]));
			context.write(outKey, outValue);
		}
		
	}
	
	// can also be used as Combiner
	public static class MatrixEntryReducer extends Reducer<IntPairWritable, DoubleWritable, IntPairWritable, DoubleWritable> {
		
		private DoubleWritable outValue = new DoubleWritable();
		
		public void reduce(IntPairWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			
			for(DoubleWritable value : values)
				sum += value.get();
			outValue.set(sum);
			context.write(key, outValue);
		}
		
	}

}
