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

	MapDriver<Object, Text, IntWritable, DoublePairWritable> mapDriver2;
	ReduceDriver<IntWritable, DoublePairWritable, Text, DoubleWritable> reduceDriver2;
	MapReduceDriver<Object, Text, IntWritable, DoublePairWritable, Text, DoubleWritable> mapReduceDriver2;
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
		
		LogStichprobeMapperEins mapper1 = new LogStichprobeMapperEins();
		LogStichprobeMapperZwei mapper2 = new LogStichprobeMapperZwei();
		LogStichprobeReducerEins reducer1 = new LogStichprobeReducerEins();
		LogStichprobeReducerZwei reducer2 = new LogStichprobeReducerZwei();

		LogStichprobeSortComparator sortComparator = new LogStichprobeSortComparator();
		LogStichprobeGroupingComparator groupingComparator = new LogStichprobeGroupingComparator();
		LogStichprobePartitioner partitioner = new LogStichprobePartitioner();

		mapDriver1 = MapDriver.newMapDriver(mapper1);
		mapDriver2 = MapDriver.newMapDriver(mapper2);
		reduceDriver1 = ReduceDriver.newReduceDriver(reducer1);
		reduceDriver2 = ReduceDriver.newReduceDriver(reducer2);
		mapReduceDriver1 = MapReduceDriver.newMapReduceDriver(mapper1, reducer1);
		mapReduceDriver1.withKeyGroupingComparator(groupingComparator);
		mapReduceDriver1.withKeyOrderComparator(sortComparator);
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper2, reducer2);
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

}
