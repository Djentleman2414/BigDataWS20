package assignment.logsam;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import matmul.MatMul.LeftMapper;
import matmul.MatMul.MatMulGroupingComparator;
import matmul.MatMul.MatMulReducer;
import matmul.MatMul.RightMapper;
import assignment.logsam.LogStichprobe.*;
import types.DoublePairWritable;
import types.IntDoubleWritable;
import types.IntPairWritable;
import types.IntTrippleWritable;
import types.LongPairWritable;

public class LogStichprobeTest {

	MapDriver<Object, Text, LongPairWritable, LongWritable> mapDriver1;
	ReduceDriver<LongPairWritable, LongWritable, IntWritable, DoubleWritable> reduceDriver1;
	MapReduceDriver<Object, Text, LongPairWritable, LongWritable, IntWritable, DoubleWritable> mapReduceDriver1;

	MapDriver<Object, Text, IntWritable, IntDoubleWritable> mapDriver2;
	ReduceDriver<IntWritable, IntDoubleWritable, Text, DoubleWritable> reduceDriver2;
	MapReduceDriver<Object, Text, IntWritable, IntDoubleWritable, Text, DoubleWritable> mapReduceDriver2;
	/**
	static {
	    String OS = System.getProperty("os.name").toLowerCase();

	    if (OS.contains("win")) {
	      System.setProperty("hadoop.home.dir", Paths.get("winutils").toAbsolutePath().toString());
	    } else {
	      System.setProperty("hadoop.home.dir", "/");
	    }
	  }
	  */

	@Before
	public void setup() {	

		LogStichprobe.anteil = 1;
		
		LogStichprobeMapperEins mapper1 = new LogStichprobeMapperEins();
		LogStichprobeMapperZwei mapper2 = new LogStichprobeMapperZwei();
		LogStichprobeReducerEins reducer1 = new LogStichprobeReducerEins();
		LogStichprobeReducerEins reducer1_2 = new LogStichprobeReducerEins();
		LogStichprobeReducerZwei reducer2 = new LogStichprobeReducerZwei();
		LogStichprobeReducerZwei reducer2_2 = new LogStichprobeReducerZwei();

		LogStichprobeSortComparator sortComparator = new LogStichprobeSortComparator();
		LogStichprobeGroupingComparator groupingComparator = new LogStichprobeGroupingComparator();
		LogStichprobePartitioner partitioner = new LogStichprobePartitioner();

		mapDriver1 = MapDriver.newMapDriver(mapper1);
		mapDriver2 = MapDriver.newMapDriver(mapper2);
		reduceDriver1 = ReduceDriver.newReduceDriver(reducer1);
		reduceDriver2 = ReduceDriver.newReduceDriver(reducer2);
		mapReduceDriver1 = MapReduceDriver.newMapReduceDriver(mapper1, reducer1_2);
		mapReduceDriver1.withKeyGroupingComparator(groupingComparator);
		mapReduceDriver1.withKeyOrderComparator(sortComparator);
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper2, reducer2_2);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void mapper1Test() throws IOException {
		//System.setProperty("hadoop.home.dir", "/");
		IntWritable key = new IntWritable();
		Pair[] inputs = { new Pair(key, new Text("16-05-01 00:25 faf9785e eb4222ab")),
				new Pair(key, new Text("16-06-01 00:55 faf4b9be eb438cab")),
				new Pair(key, new Text("16-06-01 00:45 faf3846e eb439cab")),
				new Pair(key, new Text("19-04-01 00:35 fa1532be eb4222ab")),
				new Pair(key, new Text("16-01-01 00:55 fa1527be eb439cab")),
				new Pair(key, new Text("16-00-01 00:12 1111b9be eb437cab")) };

		mapDriver1.addAll(Arrays.asList(inputs));

		Pair[] outputs = {
				new Pair(new LongPairWritable(Long.parseLong("eb4222ab", 16), Long.parseLong("faf9785e", 16)),
						new LongWritable(Long.parseLong("eb4222ab", 16))),
				new Pair(new LongPairWritable(Long.parseLong("eb438cab", 16), Long.parseLong("faf4b9be", 16)),
						new LongWritable(Long.parseLong("eb438cab", 16))),
				new Pair(new LongPairWritable(Long.parseLong("eb439cab", 16), Long.parseLong("faf3846e", 16)),
						new LongWritable(Long.parseLong("eb439cab", 16))),
				new Pair(new LongPairWritable(Long.parseLong("eb4222ab", 16), Long.parseLong("fa1532be", 16)),
						new LongWritable(Long.parseLong("eb4222ab", 16))),
				new Pair(new LongPairWritable(Long.parseLong("eb439cab", 16), Long.parseLong("fa1527be", 16)),
						new LongWritable(Long.parseLong("eb439cab", 16))),
				new Pair(new LongPairWritable(Long.parseLong("eb437cab", 16), Long.parseLong("1111b9be", 16)),
						new LongWritable(Long.parseLong("eb437cab", 16))),
				};

		mapDriver1.addAllOutput(Arrays.asList(outputs));

		mapDriver1.runTest();

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testReducer1() throws IOException {
		LongPairWritable key1 = new LongPairWritable(1,1);
		LongPairWritable key2 = new LongPairWritable(1,2);
		
		LongWritable[] inputs1 = {
				new LongWritable(1),
				new LongWritable(1),
				new LongWritable(1),
				new LongWritable(2),
				new LongWritable(2),
				new LongWritable(3),
				new LongWritable(4)
		};
		
		LongWritable[] inputs2 = {
				new LongWritable(1),
				new LongWritable(2),
				new LongWritable(2),
				new LongWritable(3),
				new LongWritable(3),
				new LongWritable(5),
				new LongWritable(5),
				new LongWritable(6),
				new LongWritable(7),
		};
		
		IntWritable outKey = new IntWritable(2);
		DoubleWritable outValue = new DoubleWritable(1);
		
		reduceDriver1.withInput(key1, Arrays.asList(inputs1));
		reduceDriver1.withInput(key2, Arrays.asList(inputs2));
		reduceDriver1.withOutput(outKey, outValue);
		
		reduceDriver1.runTest();
	}
	
	@SuppressWarnings({ "unused", "unchecked", "rawtypes" })
	@Test
	public void testMapReduce1() throws IOException {
		
		IntWritable key = new IntWritable();
		Pair[] inputs = { 
				new Pair(key, new Text("16-05-01 00:25 ffffffff aaaaaaaa")),
				new Pair(key, new Text("16-06-01 00:55 11111111 44444444")),
				new Pair(key, new Text("19-04-01 00:35 ffffffff aaaaaaaa")),
				new Pair(key, new Text("16-06-01 00:45 ffffffff bbbbbbbb")),
				new Pair(key, new Text("16-06-01 00:55 11111111 bbbbbbbb")),
				new Pair(key, new Text("16-06-01 00:55 11111111 bbbbbbbb")),
				new Pair(key, new Text("16-06-01 00:55 11111111 44444444")),
				new Pair(key, new Text("16-06-01 00:55 11111111 44444444")),
				new Pair(key, new Text("16-01-01 00:55 cccccccc 44444444")),
				new Pair(key, new Text("16-01-01 00:55 cccccccc 11111111")),
		};
		
		IntWritable outKey = new IntWritable(3);
		DoubleWritable outValue = new DoubleWritable(1.5);
		
		mapReduceDriver1.addAll(Arrays.asList(inputs));
		mapReduceDriver1.addOutput(outKey, outValue);
		
		mapReduceDriver1.runTest();
	}
	
	@Test
	public void testMapper2() throws IOException {
		LongWritable key = new LongWritable();
		Pair[] inputs = {
				new Pair(key, new Text("2\t0.5")),
				new Pair(key, new Text("4\t0.9")),
				new Pair(key, new Text("3\t0.2"))
		};
		
		IntWritable outKey = new IntWritable();
		
		Pair[] outputs = {
			new Pair(outKey,new IntDoubleWritable(2,0.5)),
			new Pair(outKey,new IntDoubleWritable(4,0.9)),
			new Pair(outKey,new IntDoubleWritable(3,0.2))
		};
		
		mapDriver2.addAll(Arrays.asList(inputs));
		mapDriver2.addAllOutput(Arrays.asList(outputs));
		
		mapDriver2.runTest();
	}
	
	@Test
	public void testReducer2() throws IOException {
		IntWritable key = new IntWritable();
		IntDoubleWritable[] inputs =  {
				new IntDoubleWritable(2,0.5),
				new IntDoubleWritable(1,0.25)
		};
		
		Text out = new Text("Der Anteil an Mehrfachabfragen beträgt:");
		DoubleWritable outValue = new DoubleWritable(0.25);
		
		reduceDriver2.withInput(key, Arrays.asList(inputs));
		reduceDriver2.withOutput(out, outValue);
		
		reduceDriver2.runTest();
		
	}
	
	@Test
	public void testMapReduce2() throws IOException {
		LongWritable key = new LongWritable();
		Pair[] inputs = {
				new Pair(key, new Text("2\t0.5")), 
				new Pair(key, new Text("4\t0.5")), 
				new Pair(key, new Text("8\t0.75"))    
												   
		};
		
		Text outKey = new Text("Der Anteil an Mehrfachabfragen beträgt:");
		DoubleWritable outValue = new DoubleWritable(0.125);
		
		mapReduceDriver2.addAll(Arrays.asList(inputs));
		mapReduceDriver2.addOutput(outKey, outValue);
		
		mapReduceDriver2.runTest();
	}

}
