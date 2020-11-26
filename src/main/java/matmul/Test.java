package matmul;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Reducer.Context;

import matmul.AlternativeMapReduce.AltMapValue;
import matmul.AlternativeMapReduce.AltMatrixReducer;
import types.IntPairWritable;

public class Test {

	public static void main(String[] args) throws IOException, InterruptedException {
		AltMapValue[] inputs = {
				new AltMapValue(true,0,0,1),
				new AltMapValue(true,0,1,2),
				new AltMapValue(true,0,2,3),
				new AltMapValue(false,0,0,4),
				new AltMapValue(false,0,1,5),
				new AltMapValue(false,1,0,6),
				new AltMapValue(false,1,2,7),
				new AltMapValue(false,2,1,8),
				new AltMapValue(false,2,2,9),
				new AltMapValue(false,3,0,10),
				new AltMapValue(false,3,1,11)
		};
		
		AltMatrixReducer reducer = new AltMatrixReducer();
		reducer.sharedDimensionSize = 3;
		reducer.leftMatrixRow = new double[3];
		reducer.reduce(new IntPairWritable(0,-1), Arrays.asList(inputs), null);
	}

}
