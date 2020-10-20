package datagen;

import java.util.Iterator;

/**
 * Represents a fake dataset of words, which can be splitted into shards. A
 * single iterator iterates over one shard only.
 * 
 * @author hoeppnef
 */

public class ShardedIterator implements Iterator<String> {

	private int shard,shards;
	private long count;
	private boolean hasnext;
	private String word;
	private Iterator<String> base;
	
	public ShardedIterator(Iterator<String> iter,int i,int s) {
		shard=i; shards=s; base=iter; count=0;
		fetch(); 
		if (shard>=shards || shard<0) 
			throw new IllegalArgumentException("illegal shard parameters");
	}

	public int getShard() {
		return shard;
	}

	public long getPosition() {
		return count-1;
	}
	
	@Override
	public boolean hasNext() {
		return hasnext;
	}

	@Override
	public String next() {
		String temp = word;
		fetch();		
		return temp;
	}

	private void fetch() {
		String s="";
		hasnext=false; 
		while (!hasnext && base.hasNext()) {
			s = base.next(); ++count;
			hasnext = s.hashCode()%shards==shard;
		} 
		word = s;
	}

}
