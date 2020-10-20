package types;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairFullComparator extends WritableComparator {
	protected IntPairFullComparator() {
		super(IntPairWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntPairWritable k1 = (IntPairWritable) w1;
		IntPairWritable k2 = (IntPairWritable) w2;
		// compare a-values; if same, compare b-values
		return k1.compareTo(k2); // already implemented in IntPairWritable
	}
}
