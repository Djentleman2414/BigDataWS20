package matmul;

import java.io.IOException;
import java.util.ArrayList;
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
		reduceDriver.getConfiguration().setInt("max.bucket.size", 4);
		reduceDriver.getConfiguration().setInt("num.of.columns.left", 5);
		
		MapKeyClass key1 = new MapKeyClass(1, true, 0, 0);
		MapKeyClass key2 = new MapKeyClass(2, true, 0, 0);
		MapKeyClass key3 = new MapKeyClass(3, true, 0, 0);
		MapKeyClass key4 = new MapKeyClass(4, true, 0, 0);
		ArrayList<MatrixEntry> values1 = new ArrayList<MatrixEntry>();
		ArrayList<MatrixEntry> values2 = new ArrayList<MatrixEntry>();
		ArrayList<MatrixEntry> values3 = new ArrayList<MatrixEntry>();
		ArrayList<MatrixEntry> values4 = new ArrayList<MatrixEntry>();
		MatrixEntry rawvalues1 = new MatrixEntry(0, 0, 1.0, true);
		MatrixEntry rawvalues2 = new MatrixEntry(1, 1, 3.0, true);
		MatrixEntry rawvalues3 = new MatrixEntry(2, 2, 4.0, true);
		MatrixEntry rawvalues4 = new MatrixEntry(2, 4, 2.0, true);
		values1.add(rawvalues1);
		values2.add(rawvalues2);
		values3.add(rawvalues3);
		values4.add(rawvalues4);
		MatrixEntry[] rawrightValues = new MatrixEntry[5];
		rawrightValues[0] = new MatrixEntry(0, 2, 1.0, false);
		rawrightValues[1] = new MatrixEntry(1, 1, 3.0, false);
		rawrightValues[2] = new MatrixEntry(2, 2, 4.0, false);
		rawrightValues[3] = new MatrixEntry(4, 1, 2.0, false);
		rawrightValues[4] = new MatrixEntry(4, 0, 1.0, false);
		
		values1.addAll(Arrays.asList(rawrightValues));
		values2.addAll(Arrays.asList(rawrightValues));
		values3.addAll(Arrays.asList(rawrightValues));
		values4.addAll(Arrays.asList(rawrightValues));

		reduceDriver.addInput(key1, values1);
		reduceDriver.addInput(key2, values2);
		reduceDriver.addInput(key3, values3);
		reduceDriver.addInput(key4, values4);
	
		reduceDriver.addOutput(new IntPairWritable(0,2), new DoubleWritable(1.0));
		reduceDriver.addOutput(new IntPairWritable(1,1), new DoubleWritable(9.0));
		reduceDriver.addOutput(new IntPairWritable(2,2), new DoubleWritable(16.0));
		reduceDriver.addOutput(new IntPairWritable(2,1), new DoubleWritable(4.0));
		reduceDriver.addOutput(new IntPairWritable(2,0), new DoubleWritable(2.0));
		
		reduceDriver.runTest();
	}
}
