package wrdnbh;

import java.util.Arrays;
import java.util.Random;

public class MinHash {
	public static final int LARGE_PRIME = 2147483647; // 2^31 - 1
	
	private int[] signature;
	private int[][] hashCoefs;
	
	public MinHash(int numOfHashes, int seed) {
		this.init(numOfHashes, seed);
	}
	
	private void init(int numOfHashes, int seed) {
		signature = new int[numOfHashes];
		reset();
		Random r = new Random(seed);
		
		// h_i(x) = (a_i * x) + b_i
		hashCoefs = new int[numOfHashes][2];
		for(int i = 0; i < numOfHashes; i++) {
			hashCoefs[i][0] = r.nextInt(LARGE_PRIME) + 1; 
			hashCoefs[i][1] = r.nextInt(LARGE_PRIME) + 1; 
		}
	}
	
	public int[] getSignature() {
		return signature;
	}
	
	public int getvectorSize() {
		return signature.length;
	}
	
	/**
	 * Die Signatur wird aus den n Hashfunktionen h_i(x) = a_i * x + b_i % LARGE_PRIME
	 * berechnet. Die Signatur wird angepasst wenn h_i(x) < s[i]
	 * @param value
	 */
	public void updateSignature(int value) {
		for(int i = 0; i < signature.length; i++) {
			signature[i] = Math.min(signature[i], h(value, i));
		}
	}
	
	private int h(int value, int index) {
		long hash = hashCoefs[index][0] * (long) value + hashCoefs[index][1];
		int i = (int) Math.floorMod(hash,LARGE_PRIME);
		return i;
	}
	
	public void reset() {
		Arrays.fill(signature, Integer.MAX_VALUE);
	}
}
