package plndrm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PalindromMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		
		String text = value.toString().toLowerCase().replaceAll("[^a-z0-9 äöüß]", "");
		StringTokenizer st = new StringTokenizer(text);
		String token;
		String reversedToken;
		
		int[] newTokenValue;
		int[] oldTokenValue;
		
		
		HashMap<String,int[]> wordMap = new HashMap<String, int[]>();
		while(st.hasMoreTokens()) {
			token = st.nextToken();
			if(token.matches(".*\\d.*") || token.length() < 3) 
				continue;
			
			reversedToken = reverseString(token);
			boolean reversed = token.compareTo(reversedToken) > 0;
			if(reversed) {
				token = reversedToken;
				newTokenValue = new int[] {0,1};
			} else {
				newTokenValue = new int[] {1,0};
			}
			oldTokenValue = wordMap.get(token);
			if(oldTokenValue == null) {
				wordMap.put(token, newTokenValue);
			} else {
				// if the token didn't get reversed, newTokenValue is {1,0} and we need to increment oldTokenValue[0]
				// if the token got reversed, newTokenValue is {0,1} and we need to increment oldTokenValue[1]
				// in both cases the index, that needs to be incremented, is equal to newTokenValue[1]
				oldTokenValue[newTokenValue[1]]++;
			}
		}
		
		IntArrayWritable writableValue = new IntArrayWritable();
		Iterator<String> it = wordMap.keySet().iterator();
		while(it.hasNext()) {
			token = it.next();			
			writableValue.set(intArrayToWritableArray(wordMap.get(token)));
			context.write(new Text(token), writableValue);
		}
	}
	
	private String reverseString(String token) {
		return new StringBuilder(token).reverse().toString();
	}
	
	private IntWritable[] intArrayToWritableArray(int[] arr) {
		IntWritable[] writableArray = new IntWritable[arr.length];	
		for(int i = 0; i < arr.length; i++) {;
			writableArray[i] = new IntWritable(arr[i]);
		}
		return writableArray;
	}
}
