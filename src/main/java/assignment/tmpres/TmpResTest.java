package assignment.tmpres;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import assignment.tmpres.TemperatureResolution.TemperatureResolutionCombiner;
import assignment.tmpres.TemperatureResolution.TemperatureResolutionMapper;
import assignment.tmpres.TemperatureResolution.TemperatureResolutionReducer;

public class TmpResTest {
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> combineDriver;
	ReduceDriver<Text, Text, Text, FloatWritable> reduceDriver;
	MultipleInputsMapReduceDriver<Text, Text, Text, FloatWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		TemperatureResolutionMapper mapper = new TemperatureResolutionMapper();
		TemperatureResolutionCombiner combiner = new TemperatureResolutionCombiner();
		TemperatureResolutionReducer reducer = new TemperatureResolutionReducer();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		combineDriver = ReduceDriver.newReduceDriver(combiner);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(combiner,reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901011506020+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901010306400-64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901020306004+64333-023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		mapDriver.withInput(new IntWritable(), new Text("0029029070999991901020106004-64333-023450FM-12+000599999V0202701N015919999999N0000001N9 -00781"));
		
		Text oneEntry = new Text();
		
		oneEntry.set("02907099999+01+1");
		mapDriver.withOutput(new Text("+64+023190101"), oneEntry);
		oneEntry.set("02907099999+15+1");
		mapDriver.withOutput(new Text("+64+023190101"), oneEntry);
		oneEntry.set("02907099999+03+1");
		mapDriver.withOutput(new Text("-64+023190101"), oneEntry);
		oneEntry.set("02907099999+03+1");
		mapDriver.withOutput(new Text("+64-023190102"), oneEntry);
		oneEntry.set("02907099999+01+1");
		mapDriver.withOutput(new Text("-64-023190102"), oneEntry);
		
		mapDriver.runTest();
	}
	
	@Test
	public void testCombiner() throws IOException {
		List<Text> values1 = new ArrayList<>();
		values1.add(new Text("02907099999+01+1"));
		values1.add(new Text("02907099999+01+1"));
		values1.add(new Text("02907099999+01+1"));
		
		combineDriver.withInput(new Text("190101+64333+02345"), values1);
		combineDriver.withOutput(new Text("190101+64333+02345"), new Text("02907099999+01+3"));
		
		combineDriver.runTest();
	}
	
	
	
	@Test
	public void testReducer() throws IOException {
		List<Text> values1 = new ArrayList<>();
		values1.add(new Text("02907099999+01+1"));
		values1.add(new Text("02907099999+01+2"));
		values1.add(new Text("02907099999+05+2"));
		values1.add(new Text("02908099999+01+2"));
		values1.add(new Text("02908099999+05+3"));
		
		FloatWritable out = new FloatWritable();
		out.set(2.5f);
		reduceDriver.withInput(new Text("+64+023190101"), values1);
		reduceDriver.withInput(new Text("-64-023190101"), values1);
		reduceDriver.withOutput(new Text("64\t023\t1901\t01"), out);
		reduceDriver.withOutput(new Text("-64\t-023\t1901\t01"), out);
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperCombinerReducer() throws IOException {
		TemperatureResolutionMapper mapper = new TemperatureResolutionMapper();
		List<Pair<Object,Text>> values = new ArrayList<>();
		values.add(new Pair<>(new Text(), new Text("0029029070999991901010106004+64334+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		values.add(new Pair<>(new Text(), new Text("0029029070999991901010106004+64335+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		values.add(new Pair<>(new Text(), new Text("0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -99991")));
		values.add(new Pair<>(new Text(), new Text("0029029070999991901010206004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		values.add(new Pair<>(new Text(), new Text("0029029080999991901010206004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		values.add(new Pair<>(new Text(), new Text("0029029080999991901010306004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		values.add(new Pair<>(new Text(), new Text("0029029070999991901010106004-64333-023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		values.add(new Pair<>(new Text(), new Text("0029029070999991901010106004-64333-023450FM-12+000599999V0202701N015919999999N0000001N9 -00781")));
		mapReduceDriver.withAll(mapper, values);
		mapReduceDriver.withMapper(mapper);
		Text key = new Text();
		FloatWritable out = new FloatWritable();
		
		
		
		key.set("64\t023\t1901\t01");
		out.set(1.0f);
		mapReduceDriver.withOutput(key, out);
		
		key.set("-64\t-023\t1901\t01");
		out.set(2.0f);
		mapReduceDriver.withOutput(key, out);
		
		
		mapReduceDriver.runTest();
	}
	
}
