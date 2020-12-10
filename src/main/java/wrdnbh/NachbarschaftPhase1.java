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

import types.IntPairWritable;

public class NachbarschaftPhase1 {

	public static class NeighborCounter implements Writable {

		public static int minCount = 10;

		private int wordHash;
		private byte count;
		
		public void set(int wordHash, byte count) {
			this.wordHash = wordHash;
			this.count = count;
		}

		public int getWordHash() {
			return wordHash;
		}

		public void setWordHash(int wordHash) {
			this.wordHash = wordHash;
		}

		public byte getCount() {
			return count;
		}

		public void setCount(byte count) {
			this.count = count;
		}

		public static void setMinCount(int minCount) {
			NeighborCounter.minCount = minCount;
		}

		public void increment() {
			if (count < minCount) {
				count++;
			}
		}

		public void add(byte b) {
			count = (byte) Math.min(minCount, count + b);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(wordHash);
			out.writeByte(count);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			wordHash = in.readInt();
			count = in.readByte();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + count;
			result = prime * result + wordHash;
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
			NeighborCounter other = (NeighborCounter) obj;
			if (count != other.count)
				return false;
			if (wordHash != other.wordHash)
				return false;
			return true;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			sb.append("WordHash: ").append(wordHash).append('\n');
			sb.append("Count: ").append(count);

			return sb.toString();
		}

	}
	
	
	public static class OutArrayWritable implements Writable {

		int[] hashes = new int[100000];
		int entryCount = 0;
		
		public int size() {
			return hashes.length;
		}
		
		public int getEntryCount() {
			return entryCount;
		}
		
		public boolean hasSpace() {
			return hashes.length > entryCount;
		}
		
		public void expandSpace() {
			int[] newHashes = new int[hashes.length + 1000];
			System.arraycopy(hashes, 0, newHashes, 0, entryCount);
		}
		
		public void add(int i) {
			hashes[entryCount++] = i;
		}
		
		public int get(int i) {
			return hashes[i];
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(entryCount);
			for(int i = 0; i < entryCount; i++)
				out.writeInt(hashes[i]);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			entryCount = in.readInt();
			hashes = new int[Math.max(100000, entryCount)];
			for(int i = 0; i < entryCount; i++)
				hashes[i] = in.readInt();
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			for(int i = 0; i < entryCount - 1; i++)
				sb.append(hashes[i]).append('\t');
			
			sb.append(hashes[entryCount - 1]);
			
			return sb.toString();
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + entryCount;
			result = prime * result + Arrays.hashCode(hashes);
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
			OutArrayWritable other = (OutArrayWritable) obj;
			if (entryCount != other.entryCount)
				return false;
			if (!Arrays.equals(hashes, other.hashes))
				return false;
			return true;
		}
		
	}

	public static class SentenceMapper extends Mapper<Object, Text, IntPairWritable, NeighborCounter> {
		
		public static int maxDistance = 3;

		private IntPairWritable outKey = new IntPairWritable();
		private NeighborCounter outValue = new NeighborCounter();
		
		public void setup(Context context) {
			maxDistance = context.getConfiguration().getInt("MAXDISTANCE", 3);
			NeighborCounter.setMinCount(context.getConfiguration().getInt("MINCOUNT", 10));
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] words = cleanLine(value.toString()).split(" ");
			
			outValue.setCount((byte) 1);
			
			for(int i = 0; i < words.length; i++) {
				outKey.setX(words[i].hashCode());
				for(int j = Math.max(0, i - maxDistance); j < Math.min(words.length, i + maxDistance); j++) {
					if(j != i) {
						outKey.setY(words[j].hashCode());
						outValue.setWordHash(words[j].hashCode());
						context.write(outKey, outValue);
					}
				}
			}			
		}

		public String cleanLine(String line) {
			return line.replaceAll("[^a-zA-Z ]", "").toLowerCase();
		}
	}
	
	public static class NeighborCountCombiner extends Reducer<IntPairWritable, NeighborCounter, IntPairWritable, NeighborCounter> {
		
		private IntPairWritable outKey = new IntPairWritable();
		private NeighborCounter outValue = new NeighborCounter();
		
		public void reduce(IntPairWritable key, Iterable<NeighborCounter> values, Context context) throws IOException, InterruptedException {
			for(NeighborCounter value : values) {
				outKey.set(key.getX(), value.getWordHash());
				outValue.set(value.getWordHash(), value.getCount());
				break;
			}
			
			for(NeighborCounter value : values) {
				if(outValue.getWordHash() != value.getWordHash()) {
					context.write(outKey, outValue);
					outKey.setY(value.getWordHash());
					outValue.set(value.getWordHash(), value.getCount());
					continue;
				}
				outValue.add(value.getCount());
			}
			context.write(outKey, outValue);
		}	
	}
	
	public static class NeighborCountReducer extends Reducer<IntPairWritable, NeighborCounter, IntWritable, OutArrayWritable> {
		
		private IntWritable outKey = new IntWritable();
		private OutArrayWritable outValue = new OutArrayWritable();
		
		public void reduce(IntPairWritable key, Iterable<NeighborCounter> values, Context context) throws IOException, InterruptedException {
			int currentHash = 0;
			int count = 0;
			
			for(NeighborCounter value : values) {
				currentHash = value.getWordHash();
				count = value.getCount();
				break;
			}
			
			for(NeighborCounter value : values) {
				if(currentHash != value.getWordHash()) {
					if(count >= NeighborCounter.minCount) {
						if(!outValue.hasSpace())
							outValue.expandSpace();
						outValue.add(currentHash);
					}					
					currentHash = value.getWordHash();
					count = value.getCount();
					continue;
				}
				count += value.getCount();
			}
			if(count >= NeighborCounter.minCount) {
				if(!outValue.hasSpace())
					outValue.expandSpace();
				outValue.add(currentHash);
			}
			context.write(outKey, outValue);
		}	
	}
	
	public static class WordGroupingComparator extends WritableComparator {
		
		public WordGroupingComparator() {
			super(IntPairWritable.class);
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			return Integer.compare(((IntPairWritable) a).getX(), ((IntPairWritable) b).getX());
		}
	}

}
