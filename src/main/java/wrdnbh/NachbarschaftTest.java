package wrdnbh;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

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

import types.TextIntWritable;
import wrdnbh.NachbarschaftPhase1.NeighborCountCombiner;
import wrdnbh.NachbarschaftPhase1.NeighborCountReducer;
import wrdnbh.NachbarschaftPhase1.NeighborCounter;
import wrdnbh.NachbarschaftPhase1.OutArrayWritable;
import wrdnbh.NachbarschaftPhase1.SentenceMapper;
import wrdnbh.NachbarschaftPhase1.WordGroupingComparator;
import wrdnbh.NachbarschaftPhase2.BucketMapValue;
import wrdnbh.NachbarschaftPhase2.MinHashMapper;
import wrdnbh.NachbarschaftPhase2.SimilarityReducer;


public class NachbarschaftTest {
	
	private static final int LARGE_PRIME = 2147483647; // 2^31 - 1
	private static final int SMALL_PRIME = 31;
	
	MapDriver<Object, Text, TextIntWritable, NeighborCounter> mapDriver1;
	ReduceDriver<TextIntWritable, NeighborCounter, TextIntWritable, NeighborCounter> combineDriver;
	ReduceDriver<TextIntWritable, NeighborCounter, Text, OutArrayWritable> reduceDriver1;
	MultipleInputsMapReduceDriver<TextIntWritable, NeighborCounter, Text, OutArrayWritable> mapReduceDriver1;
	
	MapDriver<Object, Text, IntWritable, BucketMapValue> mapDriver2;
	ReduceDriver<IntWritable, BucketMapValue, Text, Text> reduceDriver2;
	MapReduceDriver<Object, Text, IntWritable, BucketMapValue, Text, Text> mapReduceDriver2;
	
	Random r;	
	
	@Before
	public void setup() {
		SentenceMapper mapper1 = new SentenceMapper();
		NeighborCountCombiner combiner = new NeighborCountCombiner();
		NeighborCountReducer reducer1 = new NeighborCountReducer();
		
		MinHashMapper mapper2 = new MinHashMapper();
		SimilarityReducer reducer2 = new SimilarityReducer();
		
		mapDriver1 = MapDriver.newMapDriver(mapper1);
		combineDriver = ReduceDriver.newReduceDriver(combiner);
		reduceDriver1 = ReduceDriver.newReduceDriver(reducer1);
		mapReduceDriver1 = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer1);
		
		mapDriver2 = MapDriver.newMapDriver(mapper2);
		reduceDriver2 = ReduceDriver.newReduceDriver(reducer2);
		mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper2, reducer2);
		
		r = new Random(0);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked"})
	@Test
	public void testMapper1() throws IOException {
		Text sentence = new Text("These are1 some words");
		String[] words = {"these", "are", "some", "words"};
		
		byte one = 1;
		
		Pair[] outputs = new Pair[12];
		
		outputs[0] = new Pair(new TextIntWritable(words[0], words[1].hashCode()), new NeighborCounter(words[1].hashCode(),one));
		outputs[1] = new Pair(new TextIntWritable(words[0], words[2].hashCode()), new NeighborCounter(words[2].hashCode(),one));;
		outputs[2] = new Pair(new TextIntWritable(words[0], words[3].hashCode()), new NeighborCounter(words[3].hashCode(),one));;
		outputs[3] = new Pair(new TextIntWritable(words[1], words[0].hashCode()), new NeighborCounter(words[0].hashCode(),one));;
		outputs[4] = new Pair(new TextIntWritable(words[1], words[2].hashCode()), new NeighborCounter(words[2].hashCode(),one));;
		outputs[5] = new Pair(new TextIntWritable(words[1], words[3].hashCode()), new NeighborCounter(words[3].hashCode(),one));;
		outputs[6] = new Pair(new TextIntWritable(words[2], words[0].hashCode()), new NeighborCounter(words[0].hashCode(),one));;
		outputs[7] = new Pair(new TextIntWritable(words[2], words[1].hashCode()), new NeighborCounter(words[1].hashCode(),one));;
		outputs[8] = new Pair(new TextIntWritable(words[2], words[3].hashCode()), new NeighborCounter(words[3].hashCode(),one));;
		outputs[9] = new Pair(new TextIntWritable(words[3], words[0].hashCode()), new NeighborCounter(words[0].hashCode(),one));;
		outputs[10] = new Pair(new TextIntWritable(words[3], words[1].hashCode()), new NeighborCounter(words[1].hashCode(),one));;
		outputs[11] = new Pair(new TextIntWritable(words[3], words[2].hashCode()), new NeighborCounter(words[2].hashCode(),one));;
		
		mapDriver1.addInput(new LongWritable(), sentence);
		mapDriver1.addAllOutput(Arrays.asList(outputs));
		
		mapDriver1.runTest();		
	}
	
	@Test
	public void testCombiner() throws IOException {
		TextIntWritable key = new TextIntWritable("word", 0);
		
		byte one = 1;
		byte two = 2;
		byte four = 4;
		
		NeighborCounter[] inputs = {
				new NeighborCounter(0,one),	
				new NeighborCounter(0,two),	
				new NeighborCounter(0,one)	
		};
		
		NeighborCounter out = new NeighborCounter(0,four);	
		
		combineDriver.addInput(key, Arrays.asList(inputs));
		combineDriver.addOutput(key, out);
		
		combineDriver.runTest();
	}
	
	@Test
	public void testReducer1() throws IOException {
		reduceDriver1.getConfiguration().setInt("MIN_COUNT", 3);
		
		TextIntWritable key = new TextIntWritable("word", 0);
		
		byte one = 1;
		byte two = 2;
		
		NeighborCounter[] inputs = {
				new NeighborCounter(0,one),	
				new NeighborCounter(0,two),	
				new NeighborCounter(0,one),	
				new NeighborCounter(1,one),	
				new NeighborCounter(1,one),	
				new NeighborCounter(2,one),	
				new NeighborCounter(2,two)
		};
		
		int[] hashes = {0,2};
		OutArrayWritable out = new OutArrayWritable(hashes, 2);
		
		reduceDriver1.addInput(key, Arrays.asList(inputs));
		reduceDriver1.addOutput(new Text("word"), out);
		
		reduceDriver1.runTest();	
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testMapReduce1() throws IOException {
		mapReduceDriver1.getConfiguration().setInt("MIN_COUNT", 2);	
		SentenceMapper mapper = new SentenceMapper();
		WordGroupingComparator groupingComparator = new WordGroupingComparator();
		mapReduceDriver1.addMapper(mapper);
		mapReduceDriver1.withKeyGroupingComparator(groupingComparator);
		
		LongWritable key = new LongWritable();
		String[] words = {"worda", "wordb", "wordc", "wordd", "worde", "wordf"};
		
		Text[] inputs = {
				new Text("wordA wordB wordC wordD wordF"),
				new Text("wordA wordB wordE wordF"),
				new Text("wordB wordC wordD"),
		};
		
		/*
		 * wordA: wordBx2, wordCx1, wordDx1, wordEx1, wordFx1
		 * wordB: wordAx2, wordCx2, wordDx2, wordEx1, wordFx2
		 * wordC: wordAx1, wordBx2, wordDx2, wordFx1
		 * wordD: wordAx1, wordBx2, wordCx2, wordFx1
		 * wordE: wordAx1, wordBx1, wordFx1
		 * wordF: wordAx1, wordBx2, wordCx1, wordDx1, wordEx1
		 */
		
		for(int i = 0; i < inputs.length; i++) {
			mapReduceDriver1.addInput(mapper, key, inputs[i]);
		}
		
		int[][] hashes = new int[6][];
		// A = 0, B = 1, C = 2, D = 3, E = 4, F = 5
		hashes[0] = new int[] {words[1].hashCode()};
		hashes[1] = new int[] {words[0].hashCode(), words[2].hashCode(), words[3].hashCode(), words[5].hashCode()};
		hashes[2] = new int[] {words[1].hashCode(), words[3].hashCode()};
		hashes[3] = new int[] {words[1].hashCode(), words[2].hashCode()};
		hashes[4] = new int[0];
		hashes[5] = new int[] {words[1].hashCode()};
		
		for(int i = 0; i < words.length; i++) {
			if(hashes[i].length > 0)
				mapReduceDriver1.addOutput(new Text(words[i]), new OutArrayWritable(hashes[i], hashes[i].length));
		}
		mapReduceDriver1.runTest();
	}
	
	
	@Test
	public void testMapper2() throws IOException {
		r.setSeed(0);
		LongWritable key = new LongWritable();
		Text input1 = new Text("wordA\t1\t2\t3\t4");
		Text input2 = new Text("wordB\t2\t3\t5");
		String word1 = "wordA";
		String word2 = "wordB";
		int[] hashes1 = {1,2,3,4};
		int[] hashes2 = {2,3,5};
		boolean[] neighbors1 = {true, true, true, true, false};
		boolean[] neighbors2 = {false, true, true, false, true};
		
		int numHashes = 6;
		
		mapDriver2.getConfiguration().setInt("NUM_OF_HASHES", numHashes);
		mapDriver2.getConfiguration().setInt("NUM_OF_BANDS", 3);
		
		int[][] h = new int[numHashes][2];
		for(int i = 0; i < numHashes; i++) {
			h[i][0] = r.nextInt(LARGE_PRIME - 1) + 1;
			h[i][1] = r.nextInt(LARGE_PRIME - 1) + 1;
		}
		
		int[] signature1 = new int[numHashes];
		int[] signature2 = new int[numHashes];
		
		Arrays.fill(signature1, Integer.MAX_VALUE);
		Arrays.fill(signature2, Integer.MAX_VALUE);
		
		for(int i = 0; i < neighbors1.length; i++) {
			if(neighbors1[i]) {
				for(int j = 0; j < numHashes; j++)
					signature1[j] = Math.min(signature1[j], h[j][0] * (i + 1) + h[j][1]);
			}
			if(neighbors2[i]) {
				for(int j = 0; j < numHashes; j++)
					signature2[j] = Math.min(signature2[j], h[j][0] * (i + 1) + h[j][1]);
			}
		}
		
		int hash1_1 = 1;
		hash1_1 = SMALL_PRIME * hash1_1 + signature1[0];
		hash1_1 = SMALL_PRIME * hash1_1 + signature1[1];
		int hash1_2 = 1;
		hash1_2 = SMALL_PRIME * hash1_2 + signature1[2];	
		hash1_2 = SMALL_PRIME * hash1_2 + signature1[3];
		int hash1_3 = 1;
		hash1_3 = SMALL_PRIME * hash1_3 + signature1[4];
		hash1_3 = SMALL_PRIME * hash1_3 + signature1[5];
		int hash2_1 = 1;
		hash2_1 = SMALL_PRIME * hash2_1 + signature2[0];
		hash2_1 = SMALL_PRIME * hash2_1 + signature2[1];
		int hash2_2 = 1;
		hash2_2 = SMALL_PRIME * hash2_2 + signature2[2];	
		hash2_2 = SMALL_PRIME * hash2_2 + signature2[3];
		int hash2_3 = 1;
		hash2_3 = SMALL_PRIME * hash2_3 + signature2[4];
		hash2_3 = SMALL_PRIME * hash2_3 + signature2[5];
		
		mapDriver2.addInput(key, input1);
		mapDriver2.addInput(key, input2);
		
		mapDriver2.addOutput(new IntWritable(hash1_1 & Integer.MAX_VALUE), new BucketMapValue(word1, hashes1, hashes1.length));
		mapDriver2.addOutput(new IntWritable(hash1_2 & Integer.MAX_VALUE), new BucketMapValue(word1, hashes1, hashes1.length));
		mapDriver2.addOutput(new IntWritable(hash1_3 & Integer.MAX_VALUE), new BucketMapValue(word1, hashes1, hashes1.length));
		mapDriver2.addOutput(new IntWritable(hash2_1 & Integer.MAX_VALUE), new BucketMapValue(word2, hashes2, hashes2.length));
		mapDriver2.addOutput(new IntWritable(hash2_2 & Integer.MAX_VALUE), new BucketMapValue(word2, hashes2, hashes2.length));
		mapDriver2.addOutput(new IntWritable(hash2_3 & Integer.MAX_VALUE), new BucketMapValue(word2, hashes2, hashes2.length));
		
		mapDriver2.runTest(false);
	}
	
	@Test
	public void testReducer2() throws IOException {
		IntWritable key = new IntWritable();
		
		reduceDriver2.getConfiguration().setDouble("MIN_JACCARD", 0.2);
		
		String word1 = "wordA";
		String word2 = "wordB";
		String word3 = "wordC";
		int[] hashes1 = {1,2,3,4};
		int[] hashes2 = {2,3,5};
		int[] hashes3 = {12};
		
		BucketMapValue value1 = new BucketMapValue(word1, hashes1, hashes1.length);
		BucketMapValue value2 = new BucketMapValue(word2, hashes2, hashes2.length);
		BucketMapValue value3 = new BucketMapValue(word3, hashes3, hashes3.length);
		BucketMapValue[] values = {value1, value2, value3};
		
		reduceDriver2.addInput(key, Arrays.asList(values));
		reduceDriver2.addOutput(new Text(word1), new Text(word2));
		
		reduceDriver2.runTest();
	}
	
	@Test
	public void testMapReduce2() throws IOException {
		r.setSeed(0);
		
		LongWritable key = new LongWritable();
		Text input1 = new Text("wordA\t1\t2\t3\t4");
		Text input2 = new Text("wordB\t2\t3\t5");
		Text input3 = new Text("wordC\t12");
		Text word1 = new Text("wordA");
		Text word2 = new Text("wordB");
		
		int numHashes = 6;
		
		mapReduceDriver2.getConfiguration().setInt("NUM_OF_HASHES", numHashes);
		mapReduceDriver2.getConfiguration().setInt("NUM_OF_BANDS", 3);
		mapReduceDriver2.getConfiguration().setDouble("MIN_JACCARD", 0.2);
		
		mapReduceDriver2.addInput(key, input1);
		mapReduceDriver2.addInput(key, input2);
		mapReduceDriver2.addInput(key, input3);
		mapReduceDriver2.addOutput(word1, word2);
		
		mapReduceDriver2.runTest();
	}

}
