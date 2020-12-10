package wrdnbh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ncdc.IntArrayWritable;
import types.IntPairWritable;
import wrdnbh.NachbarschaftPhase1.OutArrayWritable;

public class NachbarschaftPhase2 {
	
	public static class BucketMapValue implements Writable {
		
		private int hash;
		private int[] neighbors = new int[10000];
		private int entryCount;
		
		public void setHash(int hash) {
			this.hash = hash;
		}
		
		public int getHash() {
			return hash;
		}
		
		public void addNeighbor(int n) {
			if(entryCount >= neighbors.length) {
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
			out.writeInt(hash);
			out.writeInt(entryCount);
			for(int i = 0; i < entryCount; i++)
				out.writeInt(neighbors[i]);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			hash = in.readInt();
			entryCount = in.readInt();
			if(entryCount >= neighbors.length)
				neighbors = new int[entryCount];
			for(int i = 0; i < entryCount; i++)
				neighbors[i] = in.readInt();
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + entryCount;
			result = prime * result + hash;
			result = prime * result + Arrays.hashCode(neighbors);
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
			if (hash != other.hash)
				return false;
			if (!Arrays.equals(neighbors, other.neighbors))
				return false;
			return true;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append("Hash: ").append(hash).append('\n');
			sb.append("Neighbors: ");
			for(int i = 0; i < entryCount - 1; i++)
				sb.append(neighbors[i]).append(' ');
			sb.append(neighbors[entryCount]);
			
			return sb.toString();
		}
		
		
	}

	public static class MinHashMapper extends Mapper<Object, Text, IntWritable, BucketMapValue> {
		
		public static int SMALL_PRIME = 31;

		private MinHash mh;
		private int[] buckets;
		private IntWritable outKey = new IntWritable();
		private BucketMapValue outValue = new BucketMapValue();

		private int numOfHashes;
		private int numOfBands;

		public void setup(Context context) {
			numOfHashes = context.getConfiguration().getInt("NUM_OF_HASHES", 1);
			numOfBands = context.getConfiguration().getInt("NUM_OF_BANDS", 1);
			mh = new MinHash(numOfHashes, context.getConfiguration().getInt("SEED", 0));
			buckets = new int[numOfBands];
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] hashStrings = value.toString().split("\t");
			outValue.setHash(Integer.parseInt(hashStrings[0]));
			
			
			for (int i = 1; i < hashStrings.length; i++) {
				int hash = Integer.parseInt(hashStrings[i]);
				outValue.addNeighbor(hash);
				mh.updateSignature(hash);
			}
			setBuckets(mh.getSignature());
			for(int bucket : buckets) {
				outKey.set(bucket);
				context.write(outKey, outValue);
			}
			mh.reset();
			outValue.reset();
		}
		
		private void setBuckets(int[] signature) {
			int hashesPerBand = signature.length / buckets.length;
			
			for(int i = 0; i < buckets.length; i++) {
				int bucket = 0;
				for(int j = 0; j < hashesPerBand && i * hashesPerBand + j < signature.length; j++) {
					bucket += (j + 1) * SMALL_PRIME + signature[i * hashesPerBand + j];
				}
				buckets[i] = bucket & Integer.MAX_VALUE; // make hashes positive again :D
			}
		}
	}

	public static class SimilarityReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntWritable> {
		
		public static class NeighborSet {
			
			int wordHash;
			int[] neighbors;
			
			public NeighborSet(int wordHash, int entryCount, int[] neighborSet) {
				this.wordHash = wordHash;
				neighbors = new int[entryCount];
				System.arraycopy(neighborSet, 0, neighbors, 0, entryCount);
			}
			
			public double jaccard(NeighborSet other) {
				if(other ==  null)
					return 0;
				return jaccard(other.neighbors);
			}
			
			public double jaccard(int[] otherNeighbors) {
				if(otherNeighbors == null)
					return 0;
				
				int union = 0;
				int intersection = 0;
				
				int i = 0;
				int j = 0;
				
				while(true) {
					while(i < neighbors.length && j < otherNeighbors.length && neighbors[i] == otherNeighbors[j]) {
						union++;
						intersection++;
						i++;
						j++;
					}
					
					if(neighbors[i] < otherNeighbors[j]) {
						while(i < neighbors.length &&  neighbors[i] != otherNeighbors[j]) {
							union++;
							i++;
						}
					}
					
					if(neighbors[i] > otherNeighbors[j]) {
						while(j < otherNeighbors.length && neighbors[i] != otherNeighbors[j]) {
							union++;
							j++;
						}
					}
					
					if(i == neighbors.length || j == otherNeighbors.length)
						break;
				}
				
				if(i < neighbors.length)
					union += neighbors.length - i;
				if(j < otherNeighbors.length)
					union += otherNeighbors.length - j;
				
				return (double) intersection / union;
			}
		}
	}
}
