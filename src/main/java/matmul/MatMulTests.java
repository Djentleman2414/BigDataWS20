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

import matmul.MatMul.LeftMapper;
import matmul.MatMul.MatMulGroupingComparator;
import matmul.MatMul.MatMulReducer;
import matmul.MatMul.RightMapper;
import types.IntDoubleWritable;
import types.IntPairWritable;
import types.IntTrippleWritable;

public class MatMulTests {

	MapDriver<Object, Text, IntTrippleWritable, IntDoubleWritable> mapDriverLeft;
	MapDriver<Object, Text, IntTrippleWritable, IntDoubleWritable> mapDriverRight;
	ReduceDriver<IntTrippleWritable, IntDoubleWritable, IntPairWritable, DoubleWritable> reduceDriver1;
	MultipleInputsMapReduceDriver<IntTrippleWritable, IntDoubleWritable, IntPairWritable, DoubleWritable> mapReduceDriver1;
	
	@Before
	public void setup() {
		LeftMapper leftMapper = new LeftMapper();
		RightMapper rightMapper = new RightMapper();
		MatMulReducer reducer1 = new MatMulReducer();
		MatMulGroupingComparator groupingComparator = new MatMulGroupingComparator();
		
		mapDriverLeft = MapDriver.newMapDriver(leftMapper);
		mapDriverRight = MapDriver.newMapDriver(rightMapper);
		reduceDriver1 = ReduceDriver.newReduceDriver(reducer1);
		mapReduceDriver1 = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer1);
		mapReduceDriver1.withKeyGroupingComparator(groupingComparator);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		
		mapDriverLeft.getConfiguration().setInt("COLCOUNT", 2);
		
		mapDriverLeft.addAll(Arrays.asList(inputs));
		
		Pair[] outputs = {
				new Pair(new IntTrippleWritable(0,0,1), new IntDoubleWritable(1,1)), 
				new Pair(new IntTrippleWritable(0,1,1), new IntDoubleWritable(1,1)), 
				new Pair(new IntTrippleWritable(0,0,2), new IntDoubleWritable(2,3)), 
				new Pair(new IntTrippleWritable(0,1,2), new IntDoubleWritable(2,3)), 
				new Pair(new IntTrippleWritable(1,0,0), new IntDoubleWritable(0,3)), 
				new Pair(new IntTrippleWritable(1,1,0), new IntDoubleWritable(0,3)), 
				new Pair(new IntTrippleWritable(1,0,1), new IntDoubleWritable(1,4)), 
				new Pair(new IntTrippleWritable(1,1,1), new IntDoubleWritable(1,4)), 
				new Pair(new IntTrippleWritable(2,0,0), new IntDoubleWritable(0,2)), 
				new Pair(new IntTrippleWritable(2,1,0), new IntDoubleWritable(0,2)),  
				new Pair(new IntTrippleWritable(2,0,2), new IntDoubleWritable(2,1)),  
				new Pair(new IntTrippleWritable(2,1,2), new IntDoubleWritable(2,1)), 
		};
		
		mapDriverLeft.addAllOutput(Arrays.asList(outputs));
		
		mapDriverLeft.runTest();
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		mapDriverRight.getConfiguration().setInt("ROWCOUNT", 3);
		
		mapDriverRight.addAll(Arrays.asList(inputs));
		
		Pair[] outputs = {
				new Pair(new IntTrippleWritable(0,1,0), new IntDoubleWritable(0,2)),
				new Pair(new IntTrippleWritable(1,1,0), new IntDoubleWritable(0,2)),
				new Pair(new IntTrippleWritable(2,1,0), new IntDoubleWritable(0,2)),
				new Pair(new IntTrippleWritable(0,0,1), new IntDoubleWritable(1,5)),
				new Pair(new IntTrippleWritable(1,0,1), new IntDoubleWritable(1,5)),
				new Pair(new IntTrippleWritable(2,0,1), new IntDoubleWritable(1,5)),
				new Pair(new IntTrippleWritable(0,1,1), new IntDoubleWritable(1,1)),
				new Pair(new IntTrippleWritable(1,1,1), new IntDoubleWritable(1,1)),
				new Pair(new IntTrippleWritable(2,1,1), new IntDoubleWritable(1,1)),
				new Pair(new IntTrippleWritable(0,0,2), new IntDoubleWritable(2,6)),
				new Pair(new IntTrippleWritable(1,0,2), new IntDoubleWritable(2,6)),
				new Pair(new IntTrippleWritable(2,0,2), new IntDoubleWritable(2,6))
		};
		
		mapDriverRight.addAllOutput(Arrays.asList(outputs));
		
		mapDriverRight.runTest();
	}
	
	@Test
	public void reduceTest() throws IOException {
		IntDoubleWritable[] inputs1 = {
			 new IntDoubleWritable(1,3),
			 new IntDoubleWritable(1,5),
			 new IntDoubleWritable(2,5),
			 new IntDoubleWritable(3,2),
			 new IntDoubleWritable(3,5)
		};
		IntDoubleWritable[] inputs2 = {
			new IntDoubleWritable(1,2)	
		};
		
		IntTrippleWritable key1 = new IntTrippleWritable(0,1,2);
		IntTrippleWritable key2 = new IntTrippleWritable(1,1,0);
		
		reduceDriver1.addInput(key1, Arrays.asList(inputs1));
		reduceDriver1.addInput(key2, Arrays.asList(inputs2));
		
		reduceDriver1.withOutput(new IntPairWritable(0,1), new DoubleWritable(25));
		reduceDriver1.withOutput(new IntPairWritable(1,1), new DoubleWritable(0));
		
		reduceDriver1.runTest();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void mapReduceTest() throws IOException {
		IntWritable key = new IntWritable();
		
		LeftMapper leftMapper = new LeftMapper();
		RightMapper rightMapper = new RightMapper();
		
		mapReduceDriver1.withInput(leftMapper, key, new Text("0\t1\t1")); // 0 1 3
		mapReduceDriver1.withInput(leftMapper, key, new Text("0\t2\t3")); // 3 4 0
		mapReduceDriver1.withInput(leftMapper, key, new Text("1\t0\t3")); // 2 0 1
		mapReduceDriver1.withInput(leftMapper, key, new Text("1\t1\t4"));
		mapReduceDriver1.withInput(leftMapper, key, new Text("2\t0\t2"));
		mapReduceDriver1.withInput(leftMapper, key, new Text("2\t2\t1"));
		
		mapReduceDriver1.withInput(rightMapper, key, new Text("0\t1\t2")); // 0 2
		mapReduceDriver1.withInput(rightMapper, key, new Text("1\t0\t5")); // 5 1
		mapReduceDriver1.withInput(rightMapper, key, new Text("1\t1\t1")); // 6 0
		mapReduceDriver1.withInput(rightMapper, key, new Text("2\t0\t6"));
		
		mapReduceDriver1.withMapper(leftMapper);
		mapReduceDriver1.withMapper(rightMapper);
		
		mapReduceDriver1.getConfiguration().setInt("ROWCOUNT", 3);
		mapReduceDriver1.getConfiguration().setInt("COLCOUNT", 2);
		
		Pair[] outputs = {
				new Pair(new IntPairWritable(0,0), new DoubleWritable(23)),
				new Pair(new IntPairWritable(0,1), new DoubleWritable(1)),
				new Pair(new IntPairWritable(1,0), new DoubleWritable(20)),
				new Pair(new IntPairWritable(1,1), new DoubleWritable(10)),
				new Pair(new IntPairWritable(2,0), new DoubleWritable(6)),
				new Pair(new IntPairWritable(2,1), new DoubleWritable(4))
		};
		
		mapReduceDriver1.withAllOutput(Arrays.asList(outputs));
		
		mapReduceDriver1.runTest();
	}
}
