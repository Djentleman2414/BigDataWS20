package crkhsh;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import crkhsh.CrackHash.CrackHashReducer;
import crkhsh.CrackHash.HintReducer;
import crkhsh.CrackHash.IdentityMapper;
import crkhsh.CrackHash.MapValue;
import crkhsh.CrackHash.PermutationMapper;
import crkhsh.CrackHash.SecondMapValue;
import crkhsh.CrackHash.HashMapper;
import crkhsh.CrackHash.HintCombiner;

public class CrackHashTest {
	
	MapDriver<LongWritable, Text, Text, MapValue> mapDriver1_1;
	MapDriver<LongWritable, Text, Text, MapValue> mapDriver1_2;
	MapDriver<LongWritable, Text, IntWritable, SecondMapValue> mapDriver2;
	ReduceDriver<Text,MapValue,IntWritable,Text> reduceDriver1;
	ReduceDriver<IntWritable, SecondMapValue, IntWritable, Text> reduceDriver2;
	ReduceDriver<IntWritable,SecondMapValue, IntWritable,SecondMapValue> combineDriver;
	MultipleInputsMapReduceDriver<Text,MapValue,IntWritable,Text> mapReduceDriver1;
	MultipleInputsMapReduceDriver<IntWritable, SecondMapValue, IntWritable, Text> mapReduceDriver2;
	
	@Before
	public void setUp() {
		PermutationMapper mapper1_1 = new PermutationMapper(); 
		HashMapper mapper1_2 = new HashMapper();
		IdentityMapper mapper2 = new IdentityMapper();
		HintReducer reducer1 = new HintReducer();
		HintCombiner combiner = new HintCombiner();
		CrackHashReducer reducer2 = new CrackHashReducer();
		
		mapDriver1_1 = MapDriver.newMapDriver(mapper1_1);
		mapDriver1_2 = MapDriver.newMapDriver(mapper1_2);
		mapDriver2 = MapDriver.newMapDriver(mapper2);
		reduceDriver1 = ReduceDriver.newReduceDriver(reducer1);
		reduceDriver2 = ReduceDriver.newReduceDriver(reducer2);
		combineDriver = ReduceDriver.newReduceDriver(combiner);
		mapReduceDriver1 = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer1);
		mapReduceDriver2 = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(combiner, reducer2);
	}
	
	@Test
	public void testMapper1_1() throws NoSuchAlgorithmException, IOException {
		LongWritable key = new LongWritable();
		
		char[] alphabet = {'A','B','C'};
		
		mapDriver1_1.getConfiguration().setInt("ALPHABETSIZE", 3);
		
		String[] permutations = {"BC", "CB", "AC", "CA", "AB", "BA"};
		
		for(int i = 0; i < alphabet.length; i++) {
			mapDriver1_1.addInput(new Pair<>(key, new Text("" + alphabet[i])));
		}
		mapDriver1_1.addInput(new Pair<>(key, new Text("" + 'D'))); // Input that should be ignored

		for(int i = 0; i < permutations.length; i++) {
			mapDriver1_1.addOutput(new Text(CrackHash.customHash(permutations[i])), new MapValue(alphabet[i/2]));
		}
		
		mapDriver1_1.runTest();
	}
	
	@Test
	public void testMapper1_2() throws IOException {
		LongWritable key = new LongWritable();
		for(int i = 0; i < 3; i++) {
			mapDriver1_2.addInput(key, new Text("hash" + i + "0,hash" + i + "1,hash" + i + "2," + i));
			mapDriver1_2.addOutput(new Text("hash" + i + "0"), new MapValue(i,0));
			for(int j = 1; j < 3; j++)
				mapDriver1_2.addOutput(new Text("hash" + i + "" + j), new MapValue(i,1));
		}
		
		mapDriver1_2.runTest();
	}
	
	@Test
	public void testReducer1_1() throws IOException {
		reduceDriver1.addInput(new Text("someHash"), Arrays.asList(new MapValue[] {new MapValue('A')}));
		reduceDriver1.runTest();
	}
	
	@Test
	public void testReducer1_2() throws IOException {
		Text text = new Text("someHash");
		IntWritable key = new IntWritable();
		char hint = 'A';
		MapValue[] values = {
				new MapValue(0,0),
				new MapValue(1,1),
				new MapValue(hint),
				new MapValue(2,1),
				new MapValue(3,0)
		};
		
		reduceDriver1.addInput(text, Arrays.asList(values));
		
		key.set(0);
		reduceDriver1.addOutput(key, text);
		key.set(3);
		reduceDriver1.addOutput(key, text);
		key.set(1);
		text.set("" + hint);
		reduceDriver1.addOutput(key, text);
		key.set(2);
		text.set("" + hint);
		reduceDriver1.addOutput(key, text);
		
		reduceDriver1.runTest();
	}
	
	@Test
	public void mapReduceTest1() throws NoSuchAlgorithmException, IOException {
		LongWritable inputKey = new LongWritable();
		IntWritable key = new IntWritable();
		
		
		PermutationMapper mapper1_1 = new PermutationMapper();
		HashMapper mapper1_2 = new HashMapper();
		
		mapReduceDriver1.getConfiguration().setInt("ALPHABETSIZE", 4);
		
		mapReduceDriver1.withMapper(mapper1_1);
		mapReduceDriver1.withInput(mapper1_1, inputKey, new Text("A"));
		mapReduceDriver1.withInput(mapper1_1, inputKey, new Text("B"));
		mapReduceDriver1.withInput(mapper1_1, inputKey, new Text("C"));
		mapReduceDriver1.withInput(mapper1_1, inputKey, new Text("D"));
		mapReduceDriver1.withInput(mapper1_1, inputKey, new Text("E"));
		
		mapReduceDriver1.withMapper(mapper1_2);
		mapReduceDriver1.withInput(mapper1_2, inputKey, new Text("password1," + CrackHash.customHash("ABC") + "," + CrackHash.customHash("ABD") + ",1"));
		mapReduceDriver1.withInput(mapper1_2, inputKey, new Text("password2," + CrackHash.customHash("BCD") + "," + CrackHash.customHash("ACD") + ",2"));
		
		key.set(1);
		mapReduceDriver1.withOutput(key, new Text("password1"));
		key.set(2);
		mapReduceDriver1.withOutput(key, new Text("password2"));
		
		key.set(1);
		mapReduceDriver1.withOutput(key, new Text("D"));
		mapReduceDriver1.withOutput(key, new Text("C"));
		key.set(2);
		mapReduceDriver1.withOutput(key, new Text("A"));
		mapReduceDriver1.withOutput(key, new Text("B"));
		
		mapReduceDriver1.runTest(false);
	}
	
	@Test
	public void testMapper2() throws IOException {
		LongWritable inputKey = new LongWritable();
		IntWritable key = new IntWritable();
		
		String hash1 = "";
		String hash2 = "";
		for(int i = 0; i < 64; i++) {
			hash1 += 'A';
			hash2 += 'B';
		}
		
		String[] inputs = {"1\t" + hash1 , "1\tA","1\tD","2\t" + hash2,"2\tC","2\tB"};
		for(int i = 0; i < inputs.length; i++) {
			mapDriver2.addInput(inputKey, new Text(inputs[i]));
		}
		
		key.set(1);
		mapDriver2.addOutput(key, new SecondMapValue(hash1));
		mapDriver2.addOutput(key, new SecondMapValue('A'));
		mapDriver2.addOutput(key, new SecondMapValue('D'));
		key.set(2);
		mapDriver2.addOutput(key, new SecondMapValue(hash2));
		mapDriver2.addOutput(key, new SecondMapValue('C'));
		mapDriver2.addOutput(key, new SecondMapValue('B'));
		
		mapDriver2.runTest();
	}
	
	@Test
	public void combineTest() throws IOException {
		IntWritable key = new IntWritable(1);
		String hash = "";
		for(int i = 0; i < 64; i++) {
			hash += 'A';
		}
		SecondMapValue hintValue1 = new SecondMapValue();
		hintValue1.addHint('A');
		SecondMapValue hintValue2 = new SecondMapValue();
		hintValue2.addHint('B');
		hintValue2.addHint('D');
		SecondMapValue[] inputs = {hintValue1, new SecondMapValue(hash),hintValue2};
		combineDriver.addInput(key,Arrays.asList(inputs));
		combineDriver.withOutput(key, new SecondMapValue(hash));
		SecondMapValue hintOutValue = new SecondMapValue();
		hintOutValue.addHint('A');
		hintOutValue.addHint('B');
		hintOutValue.addHint('D');
		combineDriver.withOutput(key,hintOutValue);
		
		combineDriver.runTest();
	}
	
	
	@Test
	public void testReducer2() throws NoSuchAlgorithmException, IOException {
		reduceDriver2.getConfiguration().setInt("PASSWORDSIZE", 5);
		reduceDriver2.getConfiguration().setInt("ALPHABETSIZE", 5);
		String hash = CrackHash.customHash("ABBBA");
		SecondMapValue mapValue1 = new SecondMapValue(hash);
		SecondMapValue mapValue2 = new SecondMapValue();
		mapValue2.addHint('C');
		mapValue2.addHint('E');
		SecondMapValue mapValue3 = new SecondMapValue();
		mapValue3.addHint('D');
		
		SecondMapValue[] inputs = {mapValue1, mapValue2, mapValue3};
		
		reduceDriver2.addInput(new IntWritable(1), Arrays.asList(inputs));
		
		reduceDriver2.withOutput(new IntWritable(1), new Text("ABBBA"));
		
		reduceDriver2.runTest();
	}
	
	@Test
	public void testMapReduce2() throws NoSuchAlgorithmException, IOException {
		LongWritable key = new LongWritable();
		mapReduceDriver2.getConfiguration().setInt("PASSWORDSIZE", 5);
		mapReduceDriver2.getConfiguration().setInt("ALPHABETSIZE", 5);
		IdentityMapper mapper = new IdentityMapper();
		
		String hash1 = CrackHash.customHash("ABBBA");
		String hash2 = CrackHash.customHash("AACCC");
		
		mapReduceDriver2.addMapper(mapper);
		mapReduceDriver2.withInput(mapper, key, new Text("1\t" + hash1));
		mapReduceDriver2.withInput(mapper, key, new Text("1\t" + "D"));
		mapReduceDriver2.withInput(mapper, key, new Text("1\t" + "C"));
		mapReduceDriver2.withInput(mapper, key, new Text("2\t" + hash2));
		mapReduceDriver2.withInput(mapper, key, new Text("2\t" + "B"));
		mapReduceDriver2.withInput(mapper, key, new Text("2\t" + "D"));
		
		mapReduceDriver2.withOutput(new IntWritable(1), new Text("ABBBA"));
		mapReduceDriver2.withOutput(new IntWritable(2), new Text("AACCC"));
		
		mapReduceDriver2.runTest();
	}
	
}
