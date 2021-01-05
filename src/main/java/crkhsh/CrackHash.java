package crkhsh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import crkhsh.AlphabetGenerator.AlphabetInputFormat;
import crkhsh.AlphabetGenerator.CharWritable;

public class CrackHash extends Configured implements Tool {

	private static final String PASSWORDSIZE = "passwordsize";

	public static class MapValue implements Writable {

		public static final int PASSWORDTYPE = 0;
		public static final int HINTTYPE = 1;
		public static final int PERMUTATIONTYPE = 2;

		private int valueType;
		private int id;
		private char hint;
		
		

		public MapValue() {
		}

		public MapValue(int id, int type) {
			setId(id, type);
		}

		public MapValue(char hint) {
			setHint(hint);
		}

		public int getValueType() {
			return valueType;
		}

		public void setId(int id, int type) {
			valueType = type;
			this.id = id;
		}

		public int getId() {
			return id;
		}

		public void setHint(char hint) {
			valueType = PERMUTATIONTYPE;
			this.hint = hint;
		}

		public char getHint() {
			return hint;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(valueType);
			if (valueType < PERMUTATIONTYPE)
				out.writeInt(id);
			else
				out.writeChar(hint);

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			valueType = in.readInt();
			if (valueType < PERMUTATIONTYPE)
				id = in.readInt();
			else
				hint = in.readChar();
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			if (valueType < PERMUTATIONTYPE)
				sb.append("ID: ").append(id);
			else
				sb.append("HINT: ").append(hint);

			return sb.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + hint;
			result = prime * result + id;
			result = prime * result + valueType;
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
			MapValue other = (MapValue) obj;
			if (hint != other.hint)
				return false;
			if (id != other.id)
				return false;
			if (valueType != other.valueType)
				return false;
			return true;
		}

		public MapValue clone() {
			MapValue copy = new MapValue();
			if (valueType < PERMUTATIONTYPE)
				copy.setId(id, valueType);
			else
				copy.setHint(hint);
			return copy;
		}

	}

	public static class SecondMapValue implements Writable {

		private boolean hashType;
		private String hash;
		private ArrayList<Character> hints;

		public SecondMapValue() {
		}

		public SecondMapValue(String hash) {
			this.setHash(hash);
		}

		public SecondMapValue(char c) {
			this.addHint(c);
		}

		public SecondMapValue(Collection<Character> c) {
			this.addHints(c);
		}

		public boolean isHashType() {
			return hashType;
		}

		public void setHash(String hash) {
			this.hash = hash;
			hashType = true;
		}

		public String getHash() {
			return hash;
		}

		public void setHints(ArrayList<Character> hints) {
			this.hints = hints;
			hashType = false;
		}

		public void addHint(char c) {
			hashType = false;
			if (hints == null)
				hints = new ArrayList<>();
			hints.add(c);
		}

		public void addHints(Collection<Character> c) {
			hashType = false;
			if (hints == null)
				hints = new ArrayList<>(c.size());
			this.hints.addAll(c);
		}

		public ArrayList<Character> getHints() {
			return hints;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeBoolean(hashType);
			if (hashType)
				out.write(hash.getBytes());
			else {
				out.writeInt(hints.size());
				for (char c : hints)
					out.writeChar(c);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			hashType = in.readBoolean();
			if (hashType) {
				byte[] bytes = new byte[64];
				for (int i = 0; i < 64; i++)
					bytes[i] = in.readByte();
				hash = new String(bytes);
			} else {
				int hintCount = in.readInt();
				hints = new ArrayList<>(hintCount);
				for (int i = 0; i < hintCount; i++) {
					hints.add(in.readChar());
				}
			}
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			if (hashType)
				sb.append("Hash: ").append(hash);
			else {
				sb.append("Hints: ");
				if (hints != null)
					sb.append(hints.toString());
				else
					sb.append("[]");
			}
			return sb.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((hash == null) ? 0 : hash.hashCode());
			result = prime * result + (hashType ? 1231 : 1237);
			result = prime * result + ((hints == null) ? 0 : hints.hashCode());
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
			SecondMapValue other = (SecondMapValue) obj;
			if (hash == null) {
				if (other.hash != null)
					return false;
			} else if (!hash.equals(other.hash))
				return false;
			if (hashType != other.hashType)
				return false;
			if (hints == null) {
				if (other.hints != null)
					return false;
			} else if (!hints.equals(other.hints))
				return false;
			return true;
		}
	}

	public static class PermutationMapper extends Mapper<LongWritable, CharWritable, Text, MapValue> {

		private int alphabetSize;
		private char[] subset;

		private Text outKey = new Text();
		private MapValue outValue = new MapValue();

		public void setup(Context context) {
			alphabetSize = context.getConfiguration().getInt(AlphabetGenerator.ALPHABETSIZE, 0);
			subset = new char[alphabetSize - 1];
		}

		public void map(LongWritable key, CharWritable value, Context context)
				throws IOException, InterruptedException {
			char c = value.get();
			System.out.println("ABCD " + c);
			int i = 0;
			int j = 0;
			while (i < alphabetSize) {
				if (c != (char) (65 + i)) {
					subset[j] = (char) ('A' + i);
					j++;
				}
				i++;
			}

			outValue.setHint(c);
			try {
				generateAllPermutations(subset, subset.length, c, context);
			} catch (NoSuchAlgorithmException e) {
				throw new IOException(e);
			}
		}

		public void generateAllPermutations(char[] array, int k, char missingChar, Context context)
				throws NoSuchAlgorithmException, IOException, InterruptedException {
			if (k == 1) {
				outKey.set(customHash(implodeCharArray(array)));
				context.write(outKey, outValue);
			}

			for (int i = 0; i < k; i++) {
				generateAllPermutations(array, k - 1, missingChar, context);

				if ((k & 1) == 1)
					swapElements(array, 0, k - 1);
				else
					swapElements(array, i, k - 1);
			}
		}

		public static void swapElements(char[] array, int a, int b) {
			if (a >= array.length || b >= array.length)
				return;

			char temp = array[a];
			array[a] = array[b];
			array[b] = temp;
		}
	}

	public static class HashMapper extends Mapper<LongWritable, Text, Text, MapValue> {

		private Text outKey = new Text();
		private MapValue outValue = new MapValue();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			int id = Integer.parseInt(tokens[tokens.length - 1]);
			outKey.set(tokens[0]);
			outValue.setId(id, MapValue.PASSWORDTYPE);
			context.write(outKey, outValue);

			outValue.setId(id, MapValue.HINTTYPE);
			for (int i = 1; i < tokens.length - 1; i++) {
				outKey.set(tokens[i]);
				context.write(outKey, outValue);
			}
		}
	}

	public static class HintReducer extends Reducer<Text, MapValue, IntWritable, Text> {

		private IntWritable outKey = new IntWritable();
		private Text outValue = new Text();

		ArrayList<Integer> ids = new ArrayList<>();

		public void reduce(Text key, Iterable<MapValue> values, Context context)
				throws IOException, InterruptedException {
			char hint = (char) 0;

			for (MapValue value : values) {
				if (value.getValueType() == MapValue.PASSWORDTYPE) {
					outKey.set(value.getId());
					outValue.set(key);
					context.write(outKey, outValue);
				} else if (value.getValueType() == MapValue.HINTTYPE)
					ids.add(value.getId());
				else
					hint = value.getHint();
			}

			if (hint != 0) {
				outValue.set("" + hint);
				while (!ids.isEmpty()) {
					outKey.set(ids.get(0));
					context.write(outKey, outValue);
					ids.remove(0);
				}
			}
		}
	}

	public static class IdentityMapper extends Mapper<LongWritable, Text, IntWritable, SecondMapValue> {

		private IntWritable outKey = new IntWritable();
		private SecondMapValue outValue = new SecondMapValue();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split("\t");
			outKey.set(Integer.parseInt(splits[0]));
			if (splits[1].length() > 1)
				outValue.setHash(splits[1]);
			else {
				outValue.setHints(null);
				outValue.addHint(splits[1].charAt(0));
			}

			context.write(outKey, outValue);
		}
	}

	public static class HintCombiner extends Reducer<IntWritable, SecondMapValue, IntWritable, SecondMapValue> {

		private SecondMapValue outValue = new SecondMapValue();

		public void reduce(IntWritable key, Iterable<SecondMapValue> values, Context context)
				throws IOException, InterruptedException {
			for (SecondMapValue value : values) {
				if (value.isHashType())
					context.write(key, value);
				else
					outValue.addHints(value.getHints());
			}

			if (outValue.getHints() != null) {
				context.write(key, outValue);
				outValue.setHints(null);
			}

		}
	}

	public static class CrackHashReducer extends Reducer<IntWritable, SecondMapValue, IntWritable, Text> {

		private char[] passwordAlphabet = new char[2];
		private char[] password;
		private int[] toggles;
		private boolean[] charToggles;

		private int alphabetSize;

		private String hash;
		private Text outValue = new Text();

		public void setup(Context context) {
			int passwordSize = context.getConfiguration().getInt(PASSWORDSIZE, 0);
			password = new char[passwordSize];
			toggles = new int[passwordSize];

			alphabetSize = context.getConfiguration().getInt(AlphabetGenerator.ALPHABETSIZE, 0);
			charToggles = new boolean[alphabetSize];
		}

		public void reduce(IntWritable key, Iterable<SecondMapValue> values, Context context)
				throws IOException, InterruptedException {

			Arrays.fill(charToggles, true);
			boolean toggle = true;

			for (SecondMapValue value : values) {
				if (value.isHashType())
					hash = value.getHash();
				else {
					for (char c : value.getHints())
						charToggles[c - 65] = false;
				}
			}

			for (int i = 0; i < alphabetSize; i++) {
				if (charToggles[i]) {
					if (toggle) {
						passwordAlphabet[0] = (char) ('A' + i);
						toggle = false;
					} else {
						passwordAlphabet[1] = (char) ('A' + i);
						break;
					}
				}
			}

			try {
				outValue.set(crackPassword());
			} catch (Exception e) {
				throw new IOException(e);
			}
			context.write(key, outValue);
		}

		private String crackPassword() throws NoSuchAlgorithmException, UnsupportedEncodingException {
			Arrays.fill(password, passwordAlphabet[0]);
			Arrays.fill(toggles, 0);
			String passwordString = null;
			while (passwordString == null) {
				String tempPassword = implodeCharArray(password);
				if (hash.equals(customHash(tempPassword)))
					passwordString = tempPassword;

				toggles[0] = (toggles[0] + 1) % 2;
				password[0] = passwordAlphabet[toggles[0]];
				int i = 1;
				while (i < password.length && toggles[i - 1] == 0) {
					toggles[i] = (toggles[i] + 1) % 2;
					password[i] = passwordAlphabet[toggles[i]];
					i++;
				}
			}
			return passwordString;
		}
	}

	public static String implodeCharArray(char[] array) {
		StringBuilder sb = new StringBuilder();
		for (char c : array) {
			sb.append(c);
		}
		return sb.toString();
	}

	public static String customHash(String code) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		byte[] hashedBytes = digest.digest(code.getBytes("UTF-8"));
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < hashedBytes.length; i++)
			stringBuffer.append(Integer.toHexString((hashedBytes[i] & 0xff) + 0x100).substring(1));
		return stringBuffer.toString();
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		String[] splitPath = args[0].split("/");
		parseFilename(splitPath[splitPath.length - 1]);

		Job job = getFirstJob(args);

		if (!job.waitForCompletion(true))
			return -1;

		job = getSecondJob(args[1]);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	private Job getFirstJob(String[] args) throws IOException {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName() + "_firstJob");
		Path in = new Path(args[0]);
		Path empty = new Path("empty.txt");
		if (!args[1].endsWith("/"))
			args[1] += '/';

//		try(FileSystem fs = in.getFileSystem(getConf())) {
//			fs.createNewFile(empty);
//		}

		MultipleInputs.addInputPath(job, empty, AlphabetInputFormat.class, PermutationMapper.class);
		MultipleInputs.addInputPath(job, in, TextInputFormat.class, HashMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "temp"));

		job.setJarByClass(CrackHash.class);
		job.setReducerClass(HintReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapValue.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(10);

		return job;
	}

	private Job getSecondJob(String folder) throws IOException {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName() + "_secondJob");

		if (!folder.endsWith("/"))
			folder += '/';

		FileInputFormat.addInputPath(job, new Path(folder + "temp"));
		FileOutputFormat.setOutputPath(job, new Path(folder + "final"));

		job.setJarByClass(CrackHash.class);
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(CrackHashReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SecondMapValue.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(10);

		return job;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CrackHash(), args));
	}

	public void parseFilename(String filename) {
		String[] splitFilename = filename.split("[-.]");
		getConf().setInt(AlphabetGenerator.ALPHABETSIZE, splitFilename[1].charAt(0) - 64);
		getConf().setInt(PASSWORDSIZE, Integer.parseInt(splitFilename[1].substring(1)));
	}

}
