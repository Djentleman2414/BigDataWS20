package datagen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

/**
 * Simple test case for correct sharding capabilities of WordIterator. 
 * @author hoeppnef
 */

public class TestShardedWordIterator {

	private Set<String> words;
	
	@Before
	public void setup() {
		ShardedIterator wi = new ShardedIterator(new WordIterator(10), 0, 1);
		words = new TreeSet<String>();
		while (wi.hasNext()) words.add(wi.next());
	}
	
	@Test
	public void test1() {
		assertEquals(10, words.size());
	}
	
	@Test
	public void test2() {
		Set<String> w = new TreeSet<String>();
		int n=0;

		ShardedIterator wi =new ShardedIterator( new WordIterator(10), 0, 2);
		while (wi.hasNext()) { ++n; w.add(wi.next()); }

		wi =new ShardedIterator( new WordIterator(10), 1, 2);
		while (wi.hasNext()) { ++n; w.add(wi.next()); }

		assertTrue(words.containsAll(w));
		assertTrue(w.containsAll(words));
		assertEquals(words.size(), n);
	}
	
	@Test
	public void test3() {
		Set<String> w = new TreeSet<String>();
		int n=0;

		ShardedIterator wi =new ShardedIterator( new WordIterator(10), 0, 3);
		while (wi.hasNext()) { ++n; w.add(wi.next()); }

		wi =new ShardedIterator( new WordIterator(10), 1, 3);
		while (wi.hasNext()) { ++n; w.add(wi.next()); }

		wi =new ShardedIterator( new WordIterator(10), 2, 3);
		while (wi.hasNext()) { ++n; w.add(wi.next()); }

		assertTrue(words.containsAll(w));
		assertTrue(w.containsAll(words));
		assertEquals(words.size(), n);
	}

}
