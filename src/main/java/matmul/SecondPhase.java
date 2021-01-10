package matmul;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPairWritable;

public class SecondPhase {
	
	public static class IdentityMapper extends Mapper<IntPairWritable, DoubleWritable, IntPairWritable, DoubleWritable> {
		
		public void map(IntPairWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
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
