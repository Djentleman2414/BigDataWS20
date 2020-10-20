package assignment.tmpres;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import assignment.tmpres.TemperatureResolution.TemperatureResolutionCombiner;
import assignment.tmpres.TemperatureResolution.TemperatureResolutionMapper;
import assignment.tmpres.TemperatureResolution.TemperatureResolutionReducer;
import types.IntPairWritable;

public class TmpResTest {
	MapDriver<Object, Text, Text, IntPairWritable> mapDriver;
	ReduceDriver<Text, IntPairWritable, Text, IntPairWritable> combineDriver;
	ReduceDriver<Text, IntPairWritable, Text, FloatWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntPairWritable, Text, FloatWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		TemperatureResolutionMapper mapper = new TemperatureResolutionMapper();
		TemperatureResolutionCombiner combiner = new TemperatureResolutionCombiner();
		TemperatureResolutionReducer reducer = new TemperatureResolutionReducer();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		combineDriver = ReduceDriver.newReduceDriver(combiner);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901011506020+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901010306400-64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901020306004+64333-023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901020106004-64333-023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		
		IntPairWritable oneEntry = new IntPairWritable(1,1);
		
		oneEntry.setX(1);
		mapDriver.withOutput(new Text("190101+64333+02345"), oneEntry);
		oneEntry.setX(15);
		mapDriver.withOutput(new Text("190101+64333+02345"), oneEntry);
		oneEntry.setX(3);
		mapDriver.withOutput(new Text("190101-64333+02345"), oneEntry);
		oneEntry.setX(3);
		mapDriver.withOutput(new Text("190102+64333-02345"), oneEntry);
		oneEntry.setX(1);
		mapDriver.withOutput(new Text("190102-64333-02345"), oneEntry);
		
		mapDriver.runTest();
	}
	
}
