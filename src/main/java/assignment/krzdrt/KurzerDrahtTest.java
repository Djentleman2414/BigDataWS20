package assignment.krzdrt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import assignment.krzdrt.KurzerDraht.ContactMapper;
import assignment.krzdrt.KurzerDraht.DistanceCombiner;
import assignment.krzdrt.KurzerDraht.DistanceReducer;
import assignment.krzdrt.KurzerDraht.GroupingComparator;
import assignment.krzdrt.KurzerDraht.MapValue;
import types.IntPairWritable;

public class KurzerDrahtTest {
	
	MapDriver<Object, Text, IntPairWritable, MapValue> mapDriver1;
	ReduceDriver<IntPairWritable, MapValue, IntPairWritable, MapValue> combineDriver;
	ReduceDriver<IntPairWritable, MapValue, IntWritable, IntWritable> reduceDriver1;
	MultipleInputsMapReduceDriver<IntPairWritable, MapValue, IntWritable, IntWritable> mapReduceDriver;
	
	@Before
	public void setup() {
		ContactMapper mapper1 = new ContactMapper();
		DistanceCombiner combiner = new DistanceCombiner();
		DistanceReducer reducer1 = new DistanceReducer();
		GroupingComparator groupingComparator = new GroupingComparator();
		
		mapDriver1 = new MapDriver<Object, Text, IntPairWritable, MapValue>(mapper1);
		combineDriver = new ReduceDriver<IntPairWritable, MapValue, IntPairWritable, MapValue>(combiner);
		reduceDriver1 = new ReduceDriver<IntPairWritable, MapValue, IntWritable, IntWritable>(reducer1);
		mapReduceDriver = new MultipleInputsMapReduceDriver<IntPairWritable, MapValue, IntWritable, IntWritable>(combiner, reducer1);
		mapReduceDriver.withKeyGroupingComparator(groupingComparator);
		
	}
	
	@Test
	public void map1Test() throws IOException {
		IntWritable key = new IntWritable(1);
		
		Text value1 = new Text("10000 5");
		Text value2 = new Text("22 10000");
		Text value3 = new Text("5 22");
		
		mapDriver1.withInput(key, value1);
		mapDriver1.withInput(key, value2);
		mapDriver1.withInput(key, value3);
		
		IntPairWritable outKey1 = new IntPairWritable(1,5);
		IntPairWritable outKey2 = new IntPairWritable(1,22);
		IntPairWritable outKey3 = new IntPairWritable(3,5);
		IntPairWritable outKey4 = new IntPairWritable(3,22);
		
		MapValue outValue1 = new MapValue(1);
		MapValue outValue2 = new MapValue(1);
		MapValue outValue3 = new MapValue(3);
		MapValue outValue4 = new MapValue(3);
		outValue3.addActor(22);
		outValue4.addActor(5);
		
		mapDriver1.withOutput(outKey1, outValue1);
		mapDriver1.withOutput(outKey2, outValue2);
		mapDriver1.withOutput(outKey3, outValue3);
		mapDriver1.withOutput(outKey4, outValue4);
		
		mapDriver1.runTest();
	}
	
	@Test
	public void combineTest() throws IOException {
		IntPairWritable key1 = new IntPairWritable(1,5);
		IntPairWritable key2 = new IntPairWritable(2,5);
		IntPairWritable key3 = new IntPairWritable(3,5);
		
		MapValue value1 = new MapValue(1);
		MapValue value2 = new MapValue(2);
		value2.setDistance(2);
		MapValue value2_5 = new MapValue(2);
		value2_5.setDistance(3);
		MapValue value3 = new MapValue(3);
		HashSet<Integer> actors1 = new HashSet<>();
		actors1.add(1);
		value3.setActors(actors1);
		MapValue value4 = new MapValue(3);
		HashSet<Integer> actors2 = new HashSet<>();
		actors2.add(11);
		actors2.add(12);
		value4.setActors(actors2);
		
		List<MapValue> inputList1 = new ArrayList<>();
		inputList1.add(value1);
		List<MapValue> inputList2 = new ArrayList<>();
		inputList2.add(value2);
		inputList2.add(value2_5);
		List<MapValue> inputList3 = new ArrayList<>();
		inputList3.add(value3);
		inputList3.add(value4);
		
		combineDriver.withInput(key1, inputList1);
		combineDriver.withInput(key2, inputList2);
		combineDriver.withInput(key3, inputList3);
		
		combineDriver.withOutput(key1, value1);
		combineDriver.withOutput(key2, value2);
		HashSet<Integer> combinedActors = new HashSet<>();
		combinedActors.add(1);
		combinedActors.add(11);
		combinedActors.add(12);
		MapValue outValue3 = new MapValue(3);
		outValue3.setActors(combinedActors);
		combineDriver.withOutput(key3, outValue3);
		
		combineDriver.runTest();
	}
	
	@Test
	public void reduceTest1() throws IOException {
		IntPairWritable key = new IntPairWritable(0,5);
		
		MapValue inValue1 = new MapValue(1);
		
		MapValue inValue2_0 = new MapValue(2);
		MapValue inValue2_1 = new MapValue(2);
		
		MapValue inValue3_0 = new MapValue(3);
		MapValue inValue3_1 = new MapValue(3);
		MapValue inValue3_2 = new MapValue(3);
		
		inValue2_0.setDistance(2);
		inValue2_1.setDistance(3);
		
		HashSet<Integer> actors1 = new HashSet<>();
		HashSet<Integer> actors2 = new HashSet<>();
		HashSet<Integer> actors3 = new HashSet<>();
		
		actors1.add(1);
		actors1.add(2);
		actors1.add(3);
		inValue3_0.setActors(actors1);
		
		actors2.add(3);
		actors2.add(4);
		inValue3_1.setActors(actors2);
		
		actors3.add(6);
		actors3.add(1);
		inValue3_2.setActors(actors3);
		
		HashSet<Integer> combinedActors = new HashSet<>();
		combinedActors.add(1);
		combinedActors.add(2);
		combinedActors.add(3);
		combinedActors.add(4);
		combinedActors.add(6);
		
		List<MapValue> inputList = new ArrayList<>();
		inputList.add(inValue1);
		inputList.add(inValue2_0);
		inputList.add(inValue2_1);
		inputList.add(inValue3_0);
		inputList.add(inValue3_1);
		inputList.add(inValue3_2);
		
		
		reduceDriver1.withInput(key, inputList);
		
		IntWritable outKey = new IntWritable(1);
		IntWritable outValue = new IntWritable(2);
		reduceDriver1.withOutput(outKey, outValue);
		
		outKey.set(2);
		reduceDriver1.withOutput(outKey, outValue);
		
		outKey.set(3);
		reduceDriver1.withOutput(outKey, outValue);
		reduceDriver1.withOutput(outKey, outValue);
		
		outKey.set(4);
		reduceDriver1.withOutput(outKey, outValue);
		
		outKey.set(1);
		reduceDriver1.withOutput(outKey, outValue);
		outKey.set(6);
		reduceDriver1.withOutput(outKey, outValue);
		
		
		outKey.set(5);
		outValue.set(1);
		reduceDriver1.withOutput(outKey, outValue);
		
		reduceDriver1.runTest();
	}
}
