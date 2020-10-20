package types;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairPrefixComparator extends WritableComparator {
	protected IntPairPrefixComparator() {
		super(IntPairWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntPairWritable k1 = (IntPairWritable) w1;
		IntPairWritable k2 = (IntPairWritable) w2;

		return Integer.compare(k1.getX(), k2.getX());
	}
}

