package assignment.gpstjn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import assignment.gpstjn.GasPriceJoin.GasPriceCombiner;
import assignment.gpstjn.GasPriceJoin.GasPriceMapper;
import assignment.gpstjn.GasPriceJoin.GasPriceReducer;
import assignment.gpstjn.GasPriceJoin.GasStationMapper;
import types.FloatArrayWritable;

public class GasPriceTest {
	
	MapDriver<Object, Text, Text, FloatArrayWritable> mapDriver1;
	MapDriver<Object, Text, Text, FloatArrayWritable> mapDriver2;
	ReduceDriver<Text, FloatArrayWritable, Text, FloatArrayWritable> combineDriver;
	ReduceDriver<Text, FloatArrayWritable, Text, Text> reduceDriver;
	MultipleInputsMapReduceDriver<Text, FloatArrayWritable, Text, Text> mapReduceDriver;

	
	@Before
	public void setup() {
		GasPriceMapper mapper1 = new GasPriceMapper();
		GasStationMapper mapper2 = new GasStationMapper();
		GasPriceCombiner combiner = new GasPriceCombiner();
		GasPriceReducer reducer = new GasPriceReducer();
		
		mapDriver1 = MapDriver.newMapDriver(mapper1);
		mapDriver2 = MapDriver.newMapDriver(mapper2);
		combineDriver = ReduceDriver.newReduceDriver(combiner);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(combiner,reducer);
	}
	
	@Test
	public void testGasPriceMapper() throws IOException {
		mapDriver1.withInput(new IntWritable(), new Text("52325913\t52d50542-1fc5-43af-a55e-ea8234a0a398\t1379\t1359\t1169\t2015-09-10\t16:02:01+02 20"));
		mapDriver1.withInput(new IntWritable(), new Text("52325914\t52d50542-1fc5-43af-a55e-ea8234a0a398\t1379\t1359\t1175\t2015-09-10\t16:02:01+02 20"));
		mapDriver1.withInput(new IntWritable(), new Text("52325915\t52d50542-1fc5-43af-a55e-ea8234a0a399\t1379\t1359\t1169\t2015-09-10\t16:02:01+02 20"));
		mapDriver1.withInput(new IntWritable(), new Text("52325916\t52d50542-1fc5-43af-a55e-ea8234a0a399\t1379\t1359\t1174\t2015-09-10\t16:02:01+02 20"));
		
		mapDriver1.withOutput(new Text("52d50542-1fc5-43af-a55e-ea8234a0a398"), new FloatArrayWritable(1,116,1));
		mapDriver1.withOutput(new Text("52d50542-1fc5-43af-a55e-ea8234a0a398"), new FloatArrayWritable(1,117,1));
		mapDriver1.withOutput(new Text("52d50542-1fc5-43af-a55e-ea8234a0a399"), new FloatArrayWritable(1,116,1));
		mapDriver1.withOutput(new Text("52d50542-1fc5-43af-a55e-ea8234a0a399"), new FloatArrayWritable(1,117,1));
		
		mapDriver1.runTest();
	}
	
	@Test
	public void testGasStationMapper() throws IOException {
		mapDriver2.withInput(new IntWritable(), new Text("52d50542-1fc5-43af-a55e-ea8234a0a398\t5\t2015-02-15 21:09:54\tAral Tankstelle ARAL\tLange Straße\t39\t38300\tWolfenbüttel\t\\N\t52.1565599999999989\t10.5396757000000001"));
		mapDriver2.withInput(new IntWritable(), new Text("52d50542-1fc5-43af-a55e-ea8234a0a399\t5\t2015-02-15 21:09:54\tAral Tankstelle ARAL\tLange Straße\t39\t38300\tWolfenbüttel\t\\N\t80.1565599999999989\t-10.5396757000000001"));
		
		mapDriver2.withOutput(new Text("52d50542-1fc5-43af-a55e-ea8234a0a398"), new FloatArrayWritable(0f,52.1565599999999989f,10.5396757000000001f));
		mapDriver2.withOutput(new Text("52d50542-1fc5-43af-a55e-ea8234a0a399"), new FloatArrayWritable(0f,80.1565599999999989f,-10.5396757000000001f));
		
		mapDriver2.runTest();	
	}
	
	@Test
	public void testGasPriceCombiner() throws IOException {
		List<FloatArrayWritable> values = new ArrayList<>();
		values.add(new FloatArrayWritable(1,116,1));
		values.add(new FloatArrayWritable(1,116,4));
		values.add(new FloatArrayWritable(0,52.1565599999999989f,10.5396757000000001f));
		values.add(new FloatArrayWritable(1,117,1));
		
		Text key = new Text("52d50542-1fc5-43af-a55e-ea8234a0a398");
		
		combineDriver.addInput(key, values);
		
		combineDriver.withOutput(key, new FloatArrayWritable(0,52.1565599999999989f,10.5396757000000001f));
		combineDriver.withOutput(key, new FloatArrayWritable(1,116,5));
		combineDriver.withOutput(key, new FloatArrayWritable(1,117,1));
		
		combineDriver.runTest();
	}
	
	@Test
	public void testGasPriceReducer() throws IOException {
		List<FloatArrayWritable> values = new ArrayList<>();
		values.add(new FloatArrayWritable(1,116,1));
		values.add(new FloatArrayWritable(1,116,4));
		values.add(new FloatArrayWritable(0,52.1565599999999989f,10.5396757000000001f));
		values.add(new FloatArrayWritable(1,117,1));
		
		Text key = new Text("52d50542-1fc5-43af-a55e-ea8234a0a398");
		
		reduceDriver.addInput(key, values);
		
		reduceDriver.withOutput(new Text("52.15656\t10.539676"), new Text("116:5\t117:1"));
		
		reduceDriver.runTest();
	}
}
