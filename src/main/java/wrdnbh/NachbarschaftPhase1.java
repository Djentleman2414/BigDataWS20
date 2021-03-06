package wrdnbh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import types.TextIntWritable;

public class NachbarschaftPhase1 {

	/**
	 * Es wird ein Hash eines Wortes und die H�ufigkeit des Vorkommens gespeichert
	 * Um Platz zu sparen ist die H�ufigkeit als byte gespeichert. Ist die
	 * H�ufigkeit bereits >= der Mindesth�ufigkeit, wird als H�ufigkeit nur noch die
	 * Mindesth�ufigkeit gespeichert (sonst k�me es bei einem byte schnell zu einem
	 * �berlauf)
	 */
	public static class NeighborCounter implements Writable {

		public static int minCount = 10; // h aus der Aufgabe

		private int wordHash;
		private byte count;

		public NeighborCounter() {
		}

		public NeighborCounter(int wordHash, byte count) {
			this.wordHash = wordHash;
			this.count = count;
		}

		public void set(int wordHash, byte count) {
			this.wordHash = wordHash;
			this.count = count;
		}

		public int getWordHash() {
			return wordHash;
		}

		public void setWordHash(int wordHash) {
			this.wordHash = wordHash;
			count = 1;
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

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			sb.append("WordHash: ").append(wordHash).append('\n');
			sb.append("Count: ").append(count);

			return sb.toString();
		}

		public NeighborCounter clone() {
			return new NeighborCounter(wordHash, count);
		}

	}

	/**
	 * Ein dynamisches "Array" um die Hashes der Nachbarn zu speichern Es wird
	 * einmal ein (hoffentlich ausreichend) gro�es Array erstellt, welches in der
	 * Gr��e aber dynamisch anpassbar ist �ber entryCount wird gespeichert, wie
	 * viele Hashes aktuell gespeichert sind So kann das selbe Array f�r beliebig
	 * viele reduce Aufrufe genutzt werden ohne jedes mal ein neues Objekt erstellen
	 * zu m�ssen
	 */
	public static class OutArrayWritable implements Writable {

		int[] hashes;
		int entryCount = 0;

		private StringBuilder sb = new StringBuilder();

		public OutArrayWritable() {
			hashes = new int[1000000];
		}

		public OutArrayWritable(int[] hashes, int entryCount) {
			this.hashes = hashes;
			this.entryCount = entryCount;
		}

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
			if (hasSpace())
				expandSpace();
			hashes[entryCount++] = i;
		}

		public int get(int i) {
			return hashes[i];
		}

		public void reset() {
			entryCount = 0;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(entryCount);
			for (int i = 0; i < entryCount; i++)
				out.writeInt(hashes[i]);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			entryCount = in.readInt();
			if (entryCount >= hashes.length)
				hashes = new int[entryCount];
			for (int i = 0; i < entryCount; i++)
				hashes[i] = in.readInt();
		}

		public String toString() {
			sb.setLength(0);

			for (int i = 0; i < entryCount - 1; i++)
				sb.append(hashes[i]).append('\t');

			if (entryCount > 0)
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
			for (int i = 0; i < entryCount; i++)
				if (hashes[i] != other.hashes[i])
					return false;

			return true;
		}

		public OutArrayWritable clone() {
			int[] cloneHashes = new int[entryCount];
			System.arraycopy(hashes, 0, cloneHashes, 0, entryCount);
			return new OutArrayWritable(cloneHashes, entryCount);
		}

	}

	public static class SentenceMapper extends Mapper<Object, Text, TextIntWritable, NeighborCounter> {

		public static final String[] commonWords = { "the", "a", "an", "is", "was", "and", "or", "to", "in", "of", "st",
				"nd", "rd", "th" };
		public final HashSet<String> commonWordSet = new HashSet<>();

		public static int maxDistance;

		private TextIntWritable outKey = new TextIntWritable();
		private NeighborCounter outValue = new NeighborCounter();

		private StringBuilder sb = new StringBuilder();
		private String[] words = new String[50];
		private int wordCount;

		// Das suchen nach h�ufigen W�rten ist in einer HashMap am schnellsten
		public void setup(Context context) {
			maxDistance = context.getConfiguration().getInt(Nachbarschaft.MAX_DISTANCE, 3);
			for (String word : commonWords)
				commonWordSet.add(word);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			cleanLine(value.toString());

			outValue.setCount((byte) 1);

			for (int i = 0; i < wordCount; i++) {
				if (words[i].length() < 2 || commonWordSet.contains(words[i]))
					continue;
				outKey.setText(words[i]);
				for (int j = Math.max(0, i - maxDistance); j < Math.min(wordCount, i + maxDistance + 1); j++) {
					if (j != i && !commonWordSet.contains(words[j])) {
						int hash = words[j].hashCode();
						// So sind alle Hashes zwischen 0..LARGE_PRIME und bilden eine Menge, in der
						// jedes Element teilerfremd zu LARGE_PRIME ist.
						// Das macht es einfacher Permutationen zu erzeugen
						hash = Math.floorMod(hash, MinHash.LARGE_PRIME);
						outKey.setInt(hash);
						outValue.setWordHash(hash);
						context.write(outKey, outValue);
					}
				}
			}
		}

		public void cleanLine(String line) {
			int index = 0;
			sb.setLength(0);

			for (int i = 0; i < line.length(); i++) {
				char ch = line.charAt(i);
				if (ch == ' ') {
					if (!(index < words.length)) {
						String[] newWords = new String[words.length + 20];
						System.arraycopy(words, 0, newWords, 0, words.length);
						words = newWords;
					}
					words[index++] = sb.toString();
					sb.setLength(0);
				} else if (Character.getType(ch) == Character.LOWERCASE_LETTER) {
					sb.append(ch);
				} else if (Character.getType(ch) == Character.UPPERCASE_LETTER) {
					sb.append((char) (ch + 32));
				}
			}
			wordCount = index;
		}
	}

	public static class NeighborCountCombiner
			extends Reducer<TextIntWritable, NeighborCounter, TextIntWritable, NeighborCounter> {

		private NeighborCounter outValue = new NeighborCounter();

		public void setup(Context context) {
			NeighborCounter.setMinCount(context.getConfiguration().getInt(Nachbarschaft.MIN_NEIGHBOR_COUNT, 10));
		}

		public void reduce(TextIntWritable key, Iterable<NeighborCounter> values, Context context)
				throws IOException, InterruptedException {

			outValue.set(key.getInt(), (byte) 0);
			for (NeighborCounter value : values) {
				outValue.add(value.getCount());
			}
			context.write(key, outValue);
		}
	}

	public static class NeighborCountReducer extends Reducer<TextIntWritable, NeighborCounter, Text, OutArrayWritable> {

		private Text outKey = new Text();
		private OutArrayWritable outValue = new OutArrayWritable();

		public void setup(Context context) {
			NeighborCounter.setMinCount(context.getConfiguration().getInt(Nachbarschaft.MIN_NEIGHBOR_COUNT, 10));
		}

		public void reduce(TextIntWritable key, Iterable<NeighborCounter> values, Context context)
				throws IOException, InterruptedException {
			int currentHash = 0;
			int count = 0;

			outKey.set(key.getText());

			// initialize
			for (NeighborCounter value : values) {
				currentHash = value.getWordHash();
				count = value.getCount();
				break;
			}

			for (NeighborCounter value : values) {
				// Hier wird die Sortierung der Hashes ausgenutzt. Ist der neue Hash ungleich
				// dem akutellen, kann der aktuelle in Liste nicht mehr vorkommen
				if (currentHash != value.getWordHash()) {
					if (count >= NeighborCounter.minCount)
						outValue.add(currentHash);
					currentHash = value.getWordHash();
					count = value.getCount();
					continue;
				}
				count += value.getCount();
			}

			if (count >= NeighborCounter.minCount)
				outValue.add(currentHash);

			if (outValue.getEntryCount() > 0)
				context.write(outKey, outValue);
			outValue.reset();
		}
	}

	public static class WordPartitioner extends Partitioner<TextIntWritable, NeighborCounter> {

		@Override
		public int getPartition(TextIntWritable key, NeighborCounter value, int numPartitions) {
			return (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}

	public static class CombinerGroupingComparator extends WritableComparator {

		public CombinerGroupingComparator() {
			super(TextIntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			TextIntWritable left = (TextIntWritable) a;
			TextIntWritable right = (TextIntWritable) b;
			int dist = 0;
			dist = left.getText().compareTo(right.getText());
			if (dist != 0)
				return dist;
			return Integer.compare(left.getInt(), right.getInt());
		}
	}

	public static class WordGroupingComparator extends WritableComparator {

		public WordGroupingComparator() {
			super(TextIntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			TextIntWritable left = (TextIntWritable) a;
			TextIntWritable right = (TextIntWritable) b;
			return left.getText().compareTo(right.getText());
		}
	}

}
