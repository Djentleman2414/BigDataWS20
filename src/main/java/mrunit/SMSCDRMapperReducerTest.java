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



/**
 * Quelle: https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial
 * benötigt: mrunit.jar, avro.jar, avro-mapred-hadoop2.jar, mockito-all.jar und
 * die power-mockito.jars
 * 
 * Writing MRUnit test cases
 * 
 * MRUnit testing framework is based on JUnit and it can test Map Reduce
 * programs written on 0.20 , 0.23.x , 1.0.x , 2.x version of Hadoop.
 * 
 * Following is an example to use MRUnit to unit test a Map Reduce program that
 * does SMS CDR (call details record) analysis.
 * 
 * The records look like
 * 
 * <pre>
 * CDRID;CDRType;Phone1;Phone2;SMS Status Code
 * 655209;22;796764372490213;804422938115889;6
 * 353415;0;356857119806206;287572231184798;4
 * 835699;22;252280313968413;889717902341635;0
 * 000000;22;000000000000000;000000000000000;6
 * 111111;22;111111111111111;111111111111111;6
 * </pre>
 * 
 * The MapReduce program analyzes these records, finds all records with CDRType
 * as 22, and note its corresponding SMS Status Code. For example, the Mapper
 * outputs are
 * 
 * <pre>
 * 6, 1
 * 0, 1
 * 6, 1
 * 6, 1
 * </pre>
 * 
 * The Reducer takes these as inputs and output number of times a particular
 * status code has been obtained in the CDR records.
 * 
 * The mapReduceDriver test, which tests the complete process gets the same
 * input as the mapper and outputs
 * 
 * <pre>
 * 6,3
 * 0,1
 * </pre>
 */

public class SMSCDRMapperReducerTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		SMSCDRMapper mapper = new SMSCDRMapper();
		SMSCDRReducer reducer = new SMSCDRReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(
				"655209;22;796764372490213;804422938115889;6"));
		mapDriver.withInput(new LongWritable(), new Text(
				"353415;0;356857119806206;287572231184798;4"));
		mapDriver.withInput(new LongWritable(), new Text(
				"835699;22;252280313968413;889717902341635;0"));
		mapDriver.withInput(new LongWritable(), new Text(
				"000000;22;000000000000000;000000000000000;6"));
		mapDriver.withInput(new LongWritable(), new Text(
				"111111;22;111111111111111;111111111111111;6"));

		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.withOutput(new Text("0"), new IntWritable(1));
		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.withOutput(new Text("6"), new IntWritable(1));

		mapDriver.runTest();
		
		Counter c = mapDriver.getCounters().findCounter(SMSCDRMapper.CounterType.CALL_COUNTER);
		System.out.println(c.getDisplayName()+":"+c.getValue());
		c = mapDriver.getCounters().findCounter(SMSCDRMapper.CounterType.WRITE_COUNTER);
		System.out.println(c.getDisplayName()+":"+c.getValue());
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("6"), values);
		reduceDriver.withOutput(new Text("6"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"655209;22;796764372490213;804422938115889;6"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"353415;0;356857119806206;287572231184798;4"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"835699;22;252280313968413;889717902341635;0"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"000000;22;000000000000000;000000000000000;6"));
		mapReduceDriver.addInput(new LongWritable(), new Text(
				"111111;22;111111111111111;111111111111111;6"));
		mapReduceDriver.addOutput(new Text("0"), new IntWritable(1));
		mapReduceDriver.addOutput(new Text("6"), new IntWritable(3));

		mapReduceDriver.runTest();
	}
}
