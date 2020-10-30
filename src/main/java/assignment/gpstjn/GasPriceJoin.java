package assignment.gpstjn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import types.FloatArrayWritable;

public class GasPriceJoin extends Configured implements Tool {
	
	public static class GasPriceMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {
		
		private static final Text outKey = new Text();
		private static final FloatArrayWritable outValue = new FloatArrayWritable(1,0,1);
		
		public void map (Object key, Text value, Context c) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			float gasPrice = Integer.parseInt(values[4])/10;
			outKey.set(values[1]);
			outValue.set(1, gasPrice);		
			c.write(outKey,outValue);
		}
	}
	
	public static class GasStationMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {
		
		private static final Text outKey = new Text();
		private static final FloatArrayWritable outValue = new FloatArrayWritable(0,0,0);
		
		public void map (Object key, Text value, Context c) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			outKey.set(values[0]);
			outValue.set(1, Float.parseFloat(values[9]));
			outValue.set(2, Float.parseFloat(values[10]));
			c.write(outKey,outValue);
		}
	}
	
	public static class GasPriceReducer extends Reducer<Text, FloatArrayWritable, Text, Text> {
		
		private static final Text outKey = new Text();
		private static final Text outValue = new Text();
		private static final StringBuilder outStringBuilder = new StringBuilder();
		
		public void reduce (Text key, Iterable<FloatArrayWritable> values, Context c) throws IOException, InterruptedException {
			TreeMap<Float,Float> gasPriceMap = new TreeMap<>();
			float latitude = 0;
			float longitude = 0;
			
			for (FloatArrayWritable value : values) {
				if(value.get(0) == 0) {
					latitude = value.get(1);
					longitude = value.get(2);
				} else {
					float price = value.get(1);
					float count = value.get(2);
					if (!gasPriceMap.containsKey(price))
						gasPriceMap.put(price, count);
					else
						gasPriceMap.put(price, gasPriceMap.get(price) + count);
				}
			}
			
			Iterator<Float> gasPrices = gasPriceMap.keySet().iterator();
			
			while(gasPrices.hasNext()) {
				float price = gasPrices.next();
				outStringBuilder.append((int) price).append(':').append(gasPriceMap.get(price).intValue()).append('\t');
			}
			outStringBuilder.deleteCharAt(outStringBuilder.length() - 1);
			
			outKey.set(latitude + "\t" + longitude);
			outValue.set(outStringBuilder.toString());
			
			c.write(outKey, outValue);
			
			outStringBuilder.setLength(0);
		}
	}
	
	public static class GasPriceCombiner extends Reducer<Text, FloatArrayWritable, Text, FloatArrayWritable> {
		
		private static final FloatArrayWritable outValue = new FloatArrayWritable(0,0,0);
		
		public void reduce (Text key, Iterable<FloatArrayWritable> values, Context c) throws IOException, InterruptedException {
			HashMap<Float,Float> gasPriceMap = new HashMap<>();
			
			for (FloatArrayWritable value : values) {
				if(value.get(0) == 0) {
					c.write(key, value);
				} else {
					float price = value.get(1);
					float count = value.get(2);
					if (!gasPriceMap.containsKey(price))
						gasPriceMap.put(price, count);
					else
						gasPriceMap.put(price, gasPriceMap.get(price) + count);
				}
			}
			
			Iterator<Float> gasPrices = gasPriceMap.keySet().iterator();
			outValue.set(0, 1);
			while(gasPrices.hasNext()) {
				float price = gasPrices.next();
				outValue.set(1, price);
				outValue.set(2, gasPriceMap.get(price));
				c.write(key, outValue);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
