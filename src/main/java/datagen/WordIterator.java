package datagen;

import java.util.Iterator;
import java.util.Random;

/**
 * random word generator
 * @author hoeppnef
 */

public class WordIterator implements Iterator<String> {

	private long actual,total;
	private Random rand;
	
	public WordIterator(int n) {
		total=n; actual=0;
		rand = new Random(n);
	}
	
	@Override
	public boolean hasNext() {
		return actual<total;
	}

	@Override
	public String next() {
		++actual;
		String s="";
		for (int i=0;i<5;++i) s += (char)('A'+rand.nextInt(26));
		return s;
	}

}
