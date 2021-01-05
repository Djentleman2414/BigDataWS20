package wrdnbh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NachbarschaftPhase2 {

	public static class BucketMapValue implements Writable {

		private String word;
		private int[] neighbors;
		private int entryCount;

		public BucketMapValue() {
			neighbors = new int[10000];
		}

		public BucketMapValue(String word, int[] neighbors, int entryCount) {
			this.word = word;
			this.neighbors = neighbors;
			this.entryCount = entryCount;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public void addNeighbor(int n) {
			if (entryCount >= neighbors.length) {
				int[] newNeighbors = new int[entryCount + 1000];
				System.arraycopy(neighbors, 0, newNeighbors, 0, entryCount);
				neighbors = newNeighbors;
			}
			neighbors[entryCount++] = n;
		}

		public int[] getNeighbors() {
			return neighbors;
		}

		public int getEntryCount() {
			return entryCount;
		}

		public void reset() {
			entryCount = 0;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, word);
			out.writeInt(entryCount);
			for (int i = 0; i < entryCount; i++)
				out.writeInt(neighbors[i]);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			word = WritableUtils.readString(in);
			entryCount = in.readInt();
			if (entryCount >= neighbors.length)
				neighbors = new int[entryCount];
			for (int i = 0; i < entryCount; i++)
				neighbors[i] = in.readInt();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + entryCount;
			result = prime * result + Arrays.hashCode(neighbors);
			result = prime * result + ((word == null) ? 0 : word.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BucketMapValue other = (BucketMapValue) obj;
			if (entryCount != other.entryCount)
				return false;
			for (int i = 0; i < entryCount; i++)
				if (neighbors[i] != other.neighbors[i])
					return false;
			if (word == null) {
				if (other.word != null)
					return false;
			} else if (!word.equals(other.word))
				return false;
			return true;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			sb.append("Word: ").append(word).append('\n');
			sb.append("Neighbors: ");
			for (int i = 0; i < entryCount - 1; i++)
				sb.append(neighbors[i]).append(' ');
			if (entryCount > 0)
				sb.append(neighbors[entryCount - 1]);

			return sb.toString();
		}

		public BucketMapValue clone() {
			int[] cloneNeighbors = new int[entryCount];
			System.arraycopy(neighbors, 0, cloneNeighbors, 0, entryCount);
			return new BucketMapValue(word, cloneNeighbors, entryCount);
		}

	}

	public static class MinHashMapper extends Mapper<Object, Text, IntWritable, BucketMapValue> {

		public static int SMALL_PRIME = 103;

		private MinHash mh;
		private int[] buckets;
		private IntWritable outKey = new IntWritable();
		private BucketMapValue outValue = new BucketMapValue();

		private int numOfHashesPerBand;
		private int numOfBands;
		
		private int seed;
		private Random r = new Random();

		public void setup(Context context) {
			numOfHashesPerBand = context.getConfiguration().getInt(Nachbarschaft.NUM_OF_HASHES_PER_BAND, 1);
			numOfBands = context.getConfiguration().getInt(Nachbarschaft.NUM_OF_BANDS, 1);
			mh = new MinHash(numOfHashesPerBand * numOfBands, context.getConfiguration().getInt("SEED", 0));
			buckets = new int[numOfBands];
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] hashStrings = value.toString().split("\t");
			outValue.setWord(hashStrings[0]);

			for (int i = 1; i < hashStrings.length; i++) {
				int hash = Integer.parseInt(hashStrings[i]);
				outValue.addNeighbor(hash);
				mh.updateSignature(hash);
			}
			setBuckets(mh.getSignature());

			for (int bucket : buckets) {
				outKey.set(bucket);
				context.write(outKey, outValue);
			}
			mh.reset();
			outValue.reset();
		}

		private void setBuckets(int[] signature) {
			r.setSeed(seed);
			for (int band = 0; band < buckets.length; band++) {
				int bucketHash = 1;
				int hashNumber = r.nextInt(SMALL_PRIME - 1) + 1; 
				for (int j = 0; j < numOfHashesPerBand; j++) {
					bucketHash = hashNumber * bucketHash + signature[band * numOfHashesPerBand + j];
				}
				buckets[band] = bucketHash & Integer.MAX_VALUE; // make hashes positive again :D
			}
		}
	}

	public static class SimilarityReducer extends Reducer<IntWritable, BucketMapValue, Text, Text> {

		public static double minJaccard;

		private Text outKey = new Text();
		private Text outValue = new Text();

		private NeighborSet[] neighborSets = new NeighborSet[10000];
		private int entryCount;

		public void setup(Context context) {
			minJaccard = context.getConfiguration().getDouble(Nachbarschaft.MIN_JACCARD_INDEX, 0.5);
		}

		public void reduce(IntWritable key, Iterable<BucketMapValue> values, Context context)
				throws IOException, InterruptedException {
			for (BucketMapValue value : values) {
				if (entryCount == neighborSets.length)
					expandNeighborSetArray();
				if (neighborSets[entryCount] == null)
					neighborSets[entryCount] = new NeighborSet(value.getWord(), value.getEntryCount(),
							value.getNeighbors());
				else
					neighborSets[entryCount].set(value.getWord(), value.getEntryCount(), value.getNeighbors());
				entryCount++;
			}

			// word1, word2, word3, ...
			for (int i = 0; i < entryCount - 1; i++) {
				outKey.set(neighborSets[i].getWord());

				for (int j = i + 1; j < entryCount; j++) {
					double jaccard = neighborSets[i].jaccard(neighborSets[j]);
					if (jaccard >= minJaccard) {
						outValue.set(neighborSets[j].getWord());
						context.write(outKey, outValue);
					}
				}
			}
			entryCount = 0;
		}

		private void expandNeighborSetArray() {
			NeighborSet[] neighborSets = new NeighborSet[entryCount + 1000];
			System.arraycopy(this.neighborSets, 0, neighborSets, 0, entryCount);
			this.neighborSets = neighborSets;
		}

		public static class NeighborSet {

			String word;
			int[] neighbors;

			public NeighborSet(String word, int entryCount, int[] neighborSet) {
				this.word = word;
				neighbors = new int[entryCount];
				System.arraycopy(neighborSet, 0, neighbors, 0, entryCount);
			}

			public String getWord() {
				return word;
			}

			public void setWord(String word) {
				this.word = word;
			}

			public void set(String word, int entryCount, int[] neighbors) {
				this.word = word;
				if (this.neighbors.length < entryCount)
					this.neighbors = new int[entryCount];
				System.arraycopy(neighbors, 0, this.neighbors, 0, entryCount);
			}

			public double jaccard(NeighborSet other) {
				if (other == null)
					return 0;
				return jaccard(other.neighbors);
			}

			public double jaccard(int[] otherNeighbors) {
				if (otherNeighbors == null)
					return 0;

				int union = 0;
				int intersection = 0;

				int i = 0;
				int j = 0;

				while (i < neighbors.length && j < otherNeighbors.length) {
					while (i < neighbors.length && j < otherNeighbors.length && neighbors[i] == otherNeighbors[j]) {
						union++;
						intersection++;
						i++;
						j++;
					}
					if (j < otherNeighbors.length)
						while (i < neighbors.length && neighbors[i] < otherNeighbors[j]) {
							union++;
							i++;
						}

					if (i < neighbors.length)
						while (j < otherNeighbors.length && neighbors[i] > otherNeighbors[j]) {
							union++;
							j++;
						}
				}

				if (i < neighbors.length)
					union += neighbors.length - i;
				if (j < otherNeighbors.length)
					union += otherNeighbors.length - j;
				System.out.println((double) intersection / union);
				return (double) intersection / union;
			}
		}
	}
}
