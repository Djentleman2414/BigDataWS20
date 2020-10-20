package types;

import org.apache.hadoop.mapreduce.Partitioner;

public class IntPairPrefixPartitioner<T> extends Partitioner<IntPairWritable, T> {
	@Override
	public int getPartition(IntPairWritable key, T val, int numPartitions) {
		int hash = key.getX();
		int partition = (hash % numPartitions) & Integer.MAX_VALUE;
		return partition;
	}
}

