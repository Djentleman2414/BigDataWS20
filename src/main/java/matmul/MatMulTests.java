package matmul;

import java.io.IOException;
import java.util.Arrays;

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

import matmul.MatMul.LeftMapper;
import matmul.MatMul.MatMulMapper2;
import matmul.MatMul.MatMulReducer1;
import matmul.MatMul.MatMulReducer2;
import matmul.MatMul.RightMapper;
import types.DoublePairWritable;
import types.IntPairWritable;
import types.IntTrippleWritable;

public class MatMulTests {

	MapDriver<Object, Text, IntTrippleWritable, DoubleWritable> mapDriverLeft;
	MapDriver<Object, Text, IntTrippleWritable, DoubleWritable> mapDriverRight;
	ReduceDriver<IntTrippleWritable, DoubleWritable, IntPairWritable, DoublePairWritable> reduceDriver1;
	MultipleInputsMapReduceDriver<IntTrippleWritable, DoubleWritable, IntPairWritable, DoublePairWritable> mapReduceDriver1;
	
	MapDriver<Object, Text, IntPairWritable, DoubleWritable> mapDriver2;
	ReduceDriver<IntPairWritable, DoubleWritable, IntPairWritable, DoubleWritable> reduceDriver2;
	MapReduceDriver<Object, Text, IntPairWritable, DoubleWritable, IntPairWritable , DoubleWritable> mapReduceDriver2;
	
	@Before
	public void setup() {
		LeftMapper leftMapper = new LeftMapper();
		RightMapper rightMapper = new RightMapper();
		MatMulReducer1 reducer1 = new MatMulReducer1();
		
		MatMulMapper2 mapper2 = new MatMulMapper2();
		MatMulReducer2 reducer2 = new MatMulReducer2();
		
		mapDriverLeft = MapDriver.newMapDriver(leftMapper);
		mapDriverRight = MapDriver.newMapDriver(rightMapper);
		reduceDriver1 = ReduceDriver.newReduceDriver(reducer1);
		mapReduceDriver1 = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer1);
		
		mapDriver2 = MapDriver.newMapDriver(mapper2);
		reduceDriver2 = ReduceDriver.newReduceDriver(reducer2);
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper2, reducer2);
	}
	
	@Test
	public void mapLeftTest() throws IOException {
		IntWritable key = new IntWritable();
		Pair[] inputs = {
				new Pair (key, new Text("0\t1\t1")), // 0 1 3
				new Pair (key, new Text("0\t2\t3")), // 3 4 0
				new Pair (key, new Text("1\t0\t3")), // 2 0 1
				new Pair (key, new Text("1\t1\t4")),
				new Pair (key, new Text("2\t0\t2")),
				new Pair (key, new Text("2\t2\t1"))
		};
		
		MatMul.rightColumnCount = 2;
		
		mapDriverLeft.addAll(Arrays.asList(inputs));
		
		Pair[] outputs = {
				new Pair(new IntTrippleWritable(0,1,0), new DoubleWritable(1)), //
				new Pair(new IntTrippleWritable(0,1,1), new DoubleWritable(1)), //
				new Pair(new IntTrippleWritable(0,2,0), new DoubleWritable(3)), //
				new Pair(new IntTrippleWritable(0,2,1), new DoubleWritable(3)), // missing
				new Pair(new IntTrippleWritable(1,0,0), new DoubleWritable(3)), // missing
				new Pair(new IntTrippleWritable(1,0,1), new DoubleWritable(3)), //
				new Pair(new IntTrippleWritable(1,1,0), new DoubleWritable(4)), //
				new Pair(new IntTrippleWritable(1,1,1), new DoubleWritable(4)), //
				new Pair(new IntTrippleWritable(2,0,0), new DoubleWritable(2)), // missing
				new Pair(new IntTrippleWritable(2,0,1), new DoubleWritable(2)), // 
				new Pair(new IntTrippleWritable(2,2,0), new DoubleWritable(1)), // 
				new Pair(new IntTrippleWritable(2,2,1), new DoubleWritable(1)), // missing
		};
		
		mapDriverLeft.addAllOutput(Arrays.asList(outputs));
		
		mapDriverLeft.runTest();
		
	}
	
	@Test
	public void mapRightTest() throws IOException {
		IntWritable key = new IntWritable();
		Pair[] inputs = {
				new Pair(key, new Text("0\t1\t2")), // 0 2
				new Pair(key, new Text("1\t0\t5")), // 5 1
				new Pair(key, new Text("1\t1\t1")), // 6 0
				new Pair(key, new Text("2\t0\t6"))
		};
		
		MatMul.leftRowCount = 3;
		
		mapDriverRight.addAll(Arrays.asList(inputs));
		
		Pair[] outputs = {
				new Pair(new IntTrippleWritable(0,0,1), new DoubleWritable(2)),
				new Pair(new IntTrippleWritable(1,0,1), new DoubleWritable(2)),
				new Pair(new IntTrippleWritable(2,0,1), new DoubleWritable(2)),
				new Pair(new IntTrippleWritable(0,1,0), new DoubleWritable(5)),
				new Pair(new IntTrippleWritable(1,1,0), new DoubleWritable(5)),
				new Pair(new IntTrippleWritable(2,1,0), new DoubleWritable(5)),
				new Pair(new IntTrippleWritable(0,1,1), new DoubleWritable(1)),
				new Pair(new IntTrippleWritable(1,1,1), new DoubleWritable(1)),
				new Pair(new IntTrippleWritable(2,1,1), new DoubleWritable(1)),
				new Pair(new IntTrippleWritable(0,2,0), new DoubleWritable(6)),
				new Pair(new IntTrippleWritable(1,2,0), new DoubleWritable(6)),
				new Pair(new IntTrippleWritable(2,2,0), new DoubleWritable(6))
		};
		
		mapDriverRight.addAllOutput(Arrays.asList(outputs));
		
		mapDriverRight.runTest();
	}
	
	@Test
	public void reduce1Test() throws IOException {
		DoubleWritable[] inputs1 = {
			 new DoubleWritable(3),
			 new DoubleWritable(5)
		};
		DoubleWritable[] inputs2 = {
			new DoubleWritable(2)	
		};
		
		IntTrippleWritable key1 = new IntTrippleWritable(0,1,2);
		IntTrippleWritable key2 = new IntTrippleWritable(1,1,0);
		
		reduceDriver1.addInput(key1, Arrays.asList(inputs1));
		reduceDriver1.addInput(key2, Arrays.asList(inputs2));
		
		reduceDriver1.withOutput(new IntPairWritable(0,2), new DoublePairWritable(3,5));
		
		reduceDriver1.runTest();
	}
	
	public void mapReduce1Test() throws IOException {
		IntWritable key = new IntWritable();
		
		mapReduceDriver1.withInput(new LeftMapper(), key, new Text("0\t1\t1")); // 0 1 3
		mapReduceDriver1.withInput(new LeftMapper(), key, new Text("0\t2\t3")); // 3 4 0
		mapReduceDriver1.withInput(new LeftMapper(), key, new Text("1\t0\t3")); // 2 0 1
		mapReduceDriver1.withInput(new LeftMapper(), key, new Text("1\t1\t4"));
		mapReduceDriver1.withInput(new LeftMapper(), key, new Text("2\t0\t2"));
		mapReduceDriver1.withInput(new LeftMapper(), key, new Text("2\t2\t1"));
		
		mapReduceDriver1.withInput(new RightMapper(), key, new Text("0\t1\t2")); // 0 2
		mapReduceDriver1.withInput(new RightMapper(), key, new Text("1\t0\t5")); // 5 1
		mapReduceDriver1.withInput(new RightMapper(), key, new Text("1\t1\t1")); // 6 0
		mapReduceDriver1.withInput(new RightMapper(), key, new Text("2\t0\t6"));
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,0), new DoublePairWritable(1,5)),
				new Pair(new IntPairWritable(0,1), new DoublePairWritable(1,1)),
				new Pair(new IntPairWritable(0,0), new DoublePairWritable(3,6)),
				new Pair(new IntPairWritable(1,1), new DoublePairWritable(3,2)),
				new Pair(new IntPairWritable(1,0), new DoublePairWritable(4,5)),
				new Pair(new IntPairWritable(1,1), new DoublePairWritable(4,1)),
				new Pair(new IntPairWritable(2,1), new DoublePairWritable(2,2)),
				new Pair(new IntPairWritable(2,0), new DoublePairWritable(1,6))
		};
		
		mapReduceDriver1.withAllOutput(Arrays.asList(outputs));
		
		mapReduceDriver1.runTest();
	}
	
	@Test
	public void map2Test() throws IOException {
		IntWritable key =  new IntWritable();
		Text value = new Text("0\t0\t2\t3");
		IntPairWritable outKey = new IntPairWritable(0,0);
		DoubleWritable outValue = new DoubleWritable(6);
		
		mapDriver2.withInput(key, value);
		mapDriver2.withOutput(outKey, outValue);
		
		mapDriver2.runTest();
	}
	
	@Test
	public void reduce2test() throws IOException {
		MatMul.leftRowCount = 2;
		MatMul.rightColumnCount = 3;
		DoubleWritable[] inputs1 = { // (0,0)
			new DoubleWritable(4),
			new DoubleWritable(5)
		};
		DoubleWritable[] inputs2 = { // (0,2)
			new DoubleWritable(6),
			new DoubleWritable(2)
		};
		DoubleWritable[] inputs3 = { // (1,1)
			new DoubleWritable(2),
			new DoubleWritable(2)
		};
		
		reduceDriver2.withInput(new IntPairWritable(0,0), Arrays.asList(inputs1));
		reduceDriver2.withInput(new IntPairWritable(0,2), Arrays.asList(inputs2));
		reduceDriver2.withInput(new IntPairWritable(1,1), Arrays.asList(inputs3));
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,0), new DoubleWritable(9)),
				new Pair(new IntPairWritable(0,1), new DoubleWritable(0)),
				new Pair(new IntPairWritable(0,2), new DoubleWritable(8)),
				new Pair(new IntPairWritable(1,0), new DoubleWritable(0)),
				new Pair(new IntPairWritable(1,1), new DoubleWritable(4)),
				new Pair(new IntPairWritable(1,2), new DoubleWritable(0)),
		};
		
		reduceDriver2.addAllOutput(Arrays.asList(outputs));
		
		reduceDriver2.runTest();
	}
	
	@Test
	public void mapReduce2Test() throws IOException {
		
		MatMul.leftRowCount = 3;
		MatMul.rightColumnCount = 2;
		
		IntWritable key = new IntWritable();
		Pair[] inputs = {
				new Pair(key, new Text("0\t0\t1\t5")),
				new Pair(key, new Text("0\t1\t1\t1")),
				new Pair(key, new Text("0\t0\t3\t6")),
				new Pair(key, new Text("1\t0\t4\t5")),
				new Pair(key, new Text("1\t1\t4\t1")),
				new Pair(key, new Text("2\t0\t1\t6"))
		};
		
		mapReduceDriver2.withAll(Arrays.asList(inputs));
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,0), new DoubleWritable(23)),
				new Pair(new IntPairWritable(0,1), new DoubleWritable(1)),
				new Pair(new IntPairWritable(1,0), new DoubleWritable(20)),
				new Pair(new IntPairWritable(1,1), new DoubleWritable(4)),
				new Pair(new IntPairWritable(2,0), new DoubleWritable(6)),
				new Pair(new IntPairWritable(2,1), new DoubleWritable(0))
		};
		
		mapReduceDriver2.withAllOutput(Arrays.asList(outputs));
		
		mapReduceDriver2.runTest();
	}
	
	
}
