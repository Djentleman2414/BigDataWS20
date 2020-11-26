package crkhsh;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class CrackHash extends Configured implements Tool {

	public static class CrkHshMapValue implements Writable {

		private boolean isHashValue;
		private String hash;
		private char hint;

		public CrkHshMapValue() {
			// TODO Auto-generated constructor stub
		}

		public CrkHshMapValue(String hash) {
			this.hash = hash;
		}

		public boolean isHashValue() {
			return isHashValue;
		}

		public void setHash(String hash) {
			isHashValue = true;
			this.hash = hash;
		}

		public String getHash() {
			return hash;
		}

		public void setHint(char hint) {
			isHashValue = false;
			this.hint = hint;
		}

		public char getHint() {
			return hint;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeBoolean(isHashValue);
			if(isHashValue)
				out.writeBytes(hash);
			else
				out.writeChar(hint);

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			isHashValue = in.readBoolean();
			if(isHashValue) {
				byte[] bytes = new byte[32];
				for(int i = 0; i < 32; i++) {
					bytes[i] = in.readByte();
				}
				hash = new String(bytes);
			} else {
				hint = in.readChar();
			}
		}
		
		public CrkHshMapValue clone() {
			CrkHshMapValue value = new CrkHshMapValue();
			if(isHashValue)
				value.setHash(hash);
			else
				value.setHint(hint);
			
			return value;
		}

	}

	public static class HashMapper extends Mapper<Object, Text, IntWritable, CrkHshMapValue> {

		private static int listSize = 1000;

		private int alphabetSize;

		private char[] alphabet;
		private char[][] subsets;

		private ArrayList<Integer> ids;
		private ArrayList<String> hashes;

		private IntWritable outKey = new IntWritable();
		private CrkHshMapValue outValue = new CrkHshMapValue();

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			alphabetSize = context.getConfiguration().getInt("ALPHABETSIZE", 0);

			alphabet = new char[alphabetSize];

			for (int i = 0; i < alphabetSize; i++) {
				alphabet[i] = (char) ('A' + i);
			}

			subsets = getAllSubsets(alphabet);

			ids = new ArrayList<>(listSize);
			hashes = new ArrayList<>(listSize);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if (hashes.size() == listSize) {
				for (int i = 0; i < alphabetSize; i++)
					try {
						generateAllPermutations(subsets[i], subsets[i].length, alphabet[i], context);
					} catch (NoSuchAlgorithmException e) {
						throw new IOException(e);
					}
			}

			String[] values = value.toString().split(",");
			int id = Integer.parseInt(values[values.length - 1]);
			outKey.set(id);
			outValue.setHash(values[0]);
			context.write(outKey, outValue);

			for (int i = 1; i < values.length - 1; i++) {
				ids.add(id);
				hashes.add(values[i]);
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < alphabetSize; i++)
				try {
					generateAllPermutations(subsets[i], subsets[i].length, alphabet[i], context);
				} catch (NoSuchAlgorithmException e) {
					throw new IOException(e);
				}
		}

		public char[][] getAllSubsets(char[] array) {
			char[][] subsets = new char[array.length][];

			for (int i = 0; i < array.length; i++) {
				char[] subset = new char[array.length - 1];
				for (int j = 1; j < array.length; j++) {
					subset[j - 1] = array[(i + j) % array.length];
				}
				subsets[i] = subset;
			}
			return subsets;
		}

		public void generateAllPermutations(char[] array, int k, char missingChar, Context context)
				throws NoSuchAlgorithmException, IOException, InterruptedException {
			if (k == 1) {
				String tempHash = customHash(implodeCharArray(array));
				for (int i = 0; i < hashes.size(); i++) {
					if (tempHash.equals(hashes.get(i))) {
						outKey.set(ids.get(i));
						outValue.setHint(missingChar);
						context.write(outKey, outValue);
						hashes.remove(i);
						ids.remove(i);
						i--;
					}
				}
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

	public static class CrackHashReducer extends Reducer<IntWritable, CrkHshMapValue, IntWritable, Text> {

		private char[] passwordAlphabet = new char[2];
		private char[] password;
		private int[] toggles;
		private int passwordSize;

		private String hash;
		private Text outValue = new Text();

		public void setup(Context context) {
			passwordSize = context.getConfiguration().getInt("PASSWORDSIZE", 0);
			password = new char[passwordSize];
			toggles = new int[passwordSize];
		}

		public void reduce(IntWritable key, Iterable<CrkHshMapValue> values, Context context)
				throws IOException, InterruptedException {
			passwordAlphabet[0] = 'A';
			passwordAlphabet[0] = 'A';
			boolean toggle = true;

			for (CrkHshMapValue value : values) {
				if (value.isHashValue)
					hash = value.getHash();
				else {
					if (passwordAlphabet[0] != value.getHint() || passwordAlphabet[1] != value.getHint()) {
						if (toggle)
							passwordAlphabet[0]++;
						passwordAlphabet[1]++;
					} else {
						toggle = false;
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
			String passwordString = null;
			while (passwordString == null) {
				String tempPassword = implodeCharArray(password);
				if (hash.equals(customHash(tempPassword)))
					passwordString = tempPassword;

				toggles[0] = (toggles[0] + 1) % 2;
				password[0] = passwordAlphabet[toggles[0]];
				int i = 1;
				while (i < passwordSize && toggles[i - 1] == 0) {
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
		// TODO Auto-generated method stub
		return 0;
	}

}
