package mrunit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import plndrm.*;

public class PalindromMapperReducerTest {
	
	MapDriver<LongWritable, Text, Text, IntArrayWritable> mapDriver;
	ReduceDriver<Text, IntArrayWritable, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntArrayWritable, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		PalindromMapper mapper = new PalindromMapper();
		PalindromReducer reducer = new PalindromReducer();
		
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(
				"Anna sinkt asdf fdsa"));
		mapDriver.withInput(new LongWritable(), new Text(
				"Anna singt aSdf Fdsa"));
		mapDriver.withInput(new LongWritable(), new Text(
				"Anna! singt asdf!! !f!dsa"));
		mapDriver.withInput(new LongWritable(), new Text(
				"Anna24 singt asdf fds2a fdsa we ew"));
		
		IntArrayWritable annaArray = new IntArrayWritable();
		annaArray.set(new IntWritable[] {new IntWritable(1), new IntWritable(0)});
		IntArrayWritable asdfArray = new IntArrayWritable();
		asdfArray.set(new IntWritable[] {new IntWritable(1), new IntWritable(1)});
		
		mapDriver.withOutput(new Text("sinkt"), annaArray);
		mapDriver.withOutput(new Text("asdf"), asdfArray);
		mapDriver.withOutput(new Text("anna"), annaArray);
		
		mapDriver.withOutput(new Text("singt"), annaArray);
		mapDriver.withOutput(new Text("asdf"), asdfArray);
		mapDriver.withOutput(new Text("anna"), annaArray);
			
		mapDriver.withOutput(new Text("singt"), annaArray);
		mapDriver.withOutput(new Text("asdf"), asdfArray);
		mapDriver.withOutput(new Text("anna"), annaArray);
		
		mapDriver.withOutput(new Text("singt"), annaArray);
		mapDriver.withOutput(new Text("asdf"), asdfArray);
		
		

		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntArrayWritable> values = new ArrayList<>();
		IntArrayWritable arr1 = new IntArrayWritable();
		arr1.set(new IntWritable[] {new IntWritable(1), new IntWritable(0)});
		
		IntArrayWritable arr2 = new IntArrayWritable();
		arr2.set(new IntWritable[] {new IntWritable(1), new IntWritable(1)});
		
		values.add(arr1);
		values.add(arr2);
		
		List<IntArrayWritable> values2 = new ArrayList<>();
		values2.add(arr1);
		
		reduceDriver.withInput(new Text("blub"), values);
		reduceDriver.withInput(new Text("blll"), values2);
		reduceDriver.withOutput(new Text("blub"), new Text("2:1"));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"Anna sinkt asdf fdsa"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"Anna singt aSdf Fdsa"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"Anna! singt asdf!! !f!dsa"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"Anna24 singt asdf fds2a fdsa we ew"));
		
		mapReduceDriver.addOutput(new Text("anna"),new Text("3:0"));
		mapReduceDriver.addOutput(new Text("asdf"),new Text("4:4"));
		
		mapReduceDriver.runTest();
	}
	
}
