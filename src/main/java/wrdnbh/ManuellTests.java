package wrdnbh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.Text;

import types.TextIntWritable;
import wrdnbh.NachbarschaftPhase1.NeighborCounter;
import wrdnbh.NachbarschaftPhase1.OutArrayWritable;
import wrdnbh.NachbarschaftPhase2.SimilarityReducer.NeighborSet;

public class ManuellTests {
	
	private static Text outKey = new Text();
	private static OutArrayWritable outValue = new OutArrayWritable();

	public static void main(String[] args) {
		int[] set1 = new int[20];
		int entryCount1 = 0;
		int[] set2 = new int[set1.length];
		int entryCount2= 0;
		Random r = new Random();
		
		for(int i = 0; i < set1.length; i++) {
			double d = r.nextDouble();
			if(d <= 0.5) {
				set1[entryCount1++] = i;
				set2[entryCount2++] = i;
			} else if(d <= 0.7) {
				set1[entryCount1++] = i;
			} else if(d <= 0.9) {
				set2[entryCount2++] = i;
			}
		}
		
		NeighborSet ns1 = new NeighborSet("word1", entryCount1, set1);
		NeighborSet ns2 = new NeighborSet("word2", entryCount2, set2);
		
		System.out.println(ns1.jaccard(ns2));
		
		int numHashes = 10;
		MinHash ms = new MinHash(numHashes,r.nextInt());
		for(int i = 0; i < entryCount1; i++) {
			ms.updateSignature(set1[i]);
		}
		int[] signature1 = new int[numHashes];
		System.arraycopy(ms.getSignature(), 0, signature1, 0, signature1.length);		
		ms.reset();
		
		for(int i = 0; i < entryCount2; i++) {
			ms.updateSignature(set2[i]);
		}
		int[] signature2 = new int[numHashes];
		System.arraycopy(ms.getSignature(), 0, signature2, 0, signature1.length);
		ms.reset();
		
		System.out.println(Arrays.toString(signature1));
		System.out.println(Arrays.toString(signature2));
		
		TextIntWritable key1 = new TextIntWritable("word1", 0);
		ArrayList<NeighborCounter> values1 = new ArrayList<>();
		values1.add(new NeighborCounter(0,(byte) 2));
		values1.add(new NeighborCounter(0,(byte) 9));
		values1.add(new NeighborCounter(1,(byte) 5));
		values1.add(new NeighborCounter(1,(byte) 4));
		values1.add(new NeighborCounter(2,(byte) 6));
		values1.add(new NeighborCounter(2,(byte) 10));
		
		reduce(key1, values1);
	}
	
	public static void reduce(TextIntWritable key, Iterable<NeighborCounter> values) {
		int currentHash = 0;
		int count = 0;

		outKey.set(key.getText());
		System.out.println(outKey.toString());
		// initialize
		for (NeighborCounter value : values) {
			currentHash = value.getWordHash();
			count = value.getCount();
			break;
		}

		for (NeighborCounter value : values) {
			if (currentHash != value.getWordHash()) {
				if (count >= NeighborCounter.minCount)
					outValue.add(currentHash);
				currentHash = value.getWordHash();
				count = value.getCount();
				continue;
			}
			count += value.getCount();
		}

		if (count >= NeighborCounter.minCount)
			outValue.add(currentHash);

		if (outValue.getEntryCount() > 0)
			System.out.println(outValue.toString());
		outValue.reset();
	}
}
