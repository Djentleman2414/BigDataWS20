package matmul;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import matmul.AlternativeMapReduce.AltLeftMapper;
import matmul.AlternativeMapReduce.AltMapValue;
import matmul.AlternativeMapReduce.AltMatMulGroupingComparator;
import matmul.AlternativeMapReduce.AltMatrixReducer;
import matmul.AlternativeMapReduce.AltRightMapper;
import types.IntPairWritable;

public class AlternativeMatMulTest {
	
	MapDriver<Object, Text, IntPairWritable, AltMapValue> leftMapDriver;
	MapDriver<Object, Text, IntPairWritable, AltMapValue> rightMapDriver;
	ReduceDriver<IntPairWritable, AltMapValue, IntPairWritable, DoubleWritable> reduceDriver;
	MultipleInputsMapReduceDriver<IntPairWritable, AltMapValue, IntPairWritable, DoubleWritable> mapReduceDriver;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup() {
		AltLeftMapper leftMapper = new AltLeftMapper();
		AltRightMapper rightMapper = new AltRightMapper();
		AltMatrixReducer reducer = new AltMatrixReducer();
		AltMatMulGroupingComparator comparator = new AltMatMulGroupingComparator();
		
		leftMapDriver = MapDriver.newMapDriver(leftMapper);
		rightMapDriver = MapDriver.newMapDriver(rightMapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer);
		mapReduceDriver.withKeyGroupingComparator(comparator);
	}
	
	@Test
	public void leftMapperTest() throws IOException {
		Text key = new Text();
		Pair[] inputs = {
				new Pair(key, new Text("0\t0\t1")),
				new Pair(key, new Text("0\t1\t2")),
				new Pair(key, new Text("1\t0\t3")),
				new Pair(key, new Text("1\t1\t4")),
				new Pair(key, new Text("1\t2\t5")),
				new Pair(key, new Text("2\t1\t6")),
				new Pair(key, new Text("2\t2\t7"))
		};
		
		
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,-1), new AltMapValue(true,0,1)),
				new Pair(new IntPairWritable(0,-1), new AltMapValue(true,1,2)),
				new Pair(new IntPairWritable(1,-1), new AltMapValue(true,0,3)),
				new Pair(new IntPairWritable(1,-1), new AltMapValue(true,1,4)),
				new Pair(new IntPairWritable(1,-1), new AltMapValue(true,2,5)),
				new Pair(new IntPairWritable(2,-1), new AltMapValue(true,1,6)),
				new Pair(new IntPairWritable(2,-1), new AltMapValue(true,2,7))
		};
		
		leftMapDriver.withAll(Arrays.asList(inputs));
		leftMapDriver.withAllOutput(Arrays.asList(outputs));
		
		leftMapDriver.runTest();
	}
	
	@Test
	public void rightMapperTest() throws IOException {
		Text key = new Text();
		Pair[] inputs = {
				new Pair(key, new Text("0\t0\t1")),
				//new Pair(key, new Text("0\t2\t2")),
				//new Pair(key, new Text("0\t3\t3")),
				new Pair(key, new Text("1\t1\t4")),
				//new Pair(key, new Text("1\t2\t5")),
				//new Pair(key, new Text("1\t3\t6")),
				new Pair(key, new Text("2\t0\t7")),
				//new Pair(key, new Text("2\t1\t8")),
				//new Pair(key, new Text("2\t3\t9")),
		};
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,0), new AltMapValue(false,0,0,1)),
				new Pair(new IntPairWritable(1,0), new AltMapValue(false,0,0,1)),
				new Pair(new IntPairWritable(2,0), new AltMapValue(false,0,0,1)),
				new Pair(new IntPairWritable(0,1), new AltMapValue(false,1,1,4)),
				new Pair(new IntPairWritable(1,1), new AltMapValue(false,1,1,4)),
				new Pair(new IntPairWritable(2,1), new AltMapValue(false,1,1,4)),
				new Pair(new IntPairWritable(0,0), new AltMapValue(false,0,2,7)),
				new Pair(new IntPairWritable(1,0), new AltMapValue(false,0,2,7)),
				new Pair(new IntPairWritable(2,0), new AltMapValue(false,0,2,7)),
		};
		
		rightMapDriver.getConfiguration().setInt("ROWCOUNT", 3);
		rightMapDriver.withAll(Arrays.asList(inputs));
		rightMapDriver.withAllOutput(Arrays.asList(outputs));
		
		rightMapDriver.runTest();
	}
	
	@Test
	public void reduceTest() throws IOException {
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
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,0), new DoubleWritable(14)),
				new Pair(new IntPairWritable(0,1), new DoubleWritable(27)),
				new Pair(new IntPairWritable(0,2), new DoubleWritable(43)),
				new Pair(new IntPairWritable(0,3), new DoubleWritable(32))
		};
		
		reduceDriver.getConfiguration().setInt("SHAREDDIMENSION", 3);
		reduceDriver.withInput(new Pair(new IntPairWritable(0,-1), Arrays.asList(inputs)));
		reduceDriver.withAllOutput(Arrays.asList(outputs));
		
		reduceDriver.runTest();
	}
	
}
