package plndrm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PalindromReducer extends Reducer<Text, IntArrayWritable, Text, Text> {
	
	protected void reduce(Text key, Iterable<IntArrayWritable> values, 
			Context context) throws java.io.IOException, InterruptedException {
		
		String token = key.toString();
		
		int[] tokenValue = {0,0};
		IntWritable[] valueArray;
		for(IntArrayWritable value: values) {
			 valueArray = (IntWritable[]) value.toArray();
			 tokenValue[0] += valueArray[0].get();
			 tokenValue[1] += valueArray[1].get();
		}
		
		if((tokenValue[0] > 0 && tokenValue[1] > 0) || isPalindrom(token)) {
			if(tokenValue[0] < tokenValue[1]) {
				token = reverseString(token);
				context.write(new Text(token), new Text(tokenValue[1] + ":" + tokenValue[0]));
			} else {
				context.write(new Text(token), new Text(tokenValue[0] + ":" + tokenValue[1]));
			}
			
		}
	}

	private String reverseString(String token) {
		return new StringBuilder(token).reverse().toString();
	}

	// usually token.toLowecase() would be the first step. Since this is done in the Mapper it isn't necessary
	private boolean isPalindrom(String token) {
		return token.compareTo(reverseString(token)) == 0;
	}
}
