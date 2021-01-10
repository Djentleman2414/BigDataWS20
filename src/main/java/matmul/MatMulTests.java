package matmul;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import matmul.FirstPhase.MapKeyClass;
import matmul.FirstPhase.MatMulReducer;
import matmul.FirstPhase.MatrixEntry;
import types.IntPairWritable;


public class MatMulTests {
	
	ReduceDriver<MapKeyClass, MatrixEntry, IntPairWritable, DoubleWritable> reduceDriver;
	
	@Before
	public void setup() {
		MatMulReducer reducer = new MatMulReducer();
		
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
	
	@Test
	public void testReducer1() throws IOException {
		reduceDriver.getConfiguration().setInt("MAX_BUCKET_SIZE", 20);
		reduceDriver.getConfiguration().setInt("NUM_OF_COLUMNS", 5);
		
		MapKeyClass key = new MapKeyClass();
		MatrixEntry[] values = new MatrixEntry[9];
		values[0] = new MatrixEntry(0, 0, 1.0, true);
		values[1] = new MatrixEntry(1, 1, 3.0, true);
		values[2] = new MatrixEntry(2, 2, 4.0, true);
		values[3] = new MatrixEntry(2, 4, 2.0, true);
		values[4] = new MatrixEntry(0, 2, 1.0, false);
		values[5] = new MatrixEntry(1, 1, 3.0, false);
		values[6] = new MatrixEntry(2, 2, 4.0, false);
		values[7] = new MatrixEntry(4, 1, 2.0, false);
		values[8] = new MatrixEntry(4, 0, 1.0, false);
		
		reduceDriver.addInput(key, Arrays.asList(values));
		
		reduceDriver.addOutput(new IntPairWritable(0,2), new DoubleWritable(1.0));
		reduceDriver.addOutput(new IntPairWritable(1,1), new DoubleWritable(9.0));
		reduceDriver.addOutput(new IntPairWritable(2,2), new DoubleWritable(16.0));
		reduceDriver.addOutput(new IntPairWritable(2,1), new DoubleWritable(4.0));
		reduceDriver.addOutput(new IntPairWritable(2,0), new DoubleWritable(2.0));


		
		reduceDriver.runTest();
	}

}
