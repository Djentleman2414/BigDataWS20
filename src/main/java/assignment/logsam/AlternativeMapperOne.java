package assignment.logsam;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import types.LongPairWritable;

public class AlternativeMapperOne extends Mapper<Object, Text, LongPairWritable, LongWritable> {

	public static double percentage = 0.1;

	private HashMap<Long, Integer> callMap = new HashMap<>();
	private long[] productIds = new long[100000];
	private boolean full;
	private int lastEntries = 100;
	
	private LongPairWritable outKey = new LongPairWritable();
	private LongWritable outValue = new LongWritable();
	
	public void setup(Context context) {
		percentage = context.getConfiguration().getDouble("PERCENTAGE", 0.1);
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if(full) {
			turnToIterator();
			writeToContext(context);
		}
		
		String[] values = value.toString().split(" ");
		addCall(Long.parseLong(values[2], 16), Long.parseLong(values[3], 16));
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		turnToIterator();
		writeToContext(context);
	}

	private void addCall(long userId, long productId) {
		int callIndex = callMap.containsKey(userId) ? callMap.get(userId) : callMap.size() * 100;
		productIds[callIndex] = productId;

		callIndex++;

		if (callMap.size() >= 900)
			lastEntries--;

		full = callIndex % 100 == 0 || lastEntries == 0;

		callMap.put(userId, callIndex);
	}

	private boolean turnToIterator() {
		for (Entry<Long, Integer> e : callMap.entrySet()) {
			int startIndex = callMap.get(e.getKey()) / 100 * 100;
			int numOfCalls = e.getValue() % 100;
			numOfCalls = numOfCalls != 0 ? numOfCalls : 100;
			int k = Math.max(1, (int) Math.round(percentage * numOfCalls));
			for (int n = k; n < numOfCalls; n++) {
				if ((int) (Math.random() * (n + 1)) < k) {
					productIds[startIndex + (int) (Math.random() * k)] = productIds[startIndex + n];
					productIds[startIndex + n] = 0;
				}
			}
		}
		return true;
	}
	
	private void writeToContext(Context context) throws IOException, InterruptedException {
		for(long user : callMap.keySet()) {
			outKey.setY(user);
			int index = callMap.get(user) / 100 * 100;
			while(productIds[index] != 0) {
				outKey.setX(productIds[index]);
				outValue.set(productIds[index]);
				context.write(outKey, outValue);
				productIds[index] = 0;
				index++;
			}
		}
		callMap.clear();
	}
}
