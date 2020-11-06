package assignment.gpstjn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.FloatArrayWritable;

public class GasPriceJoin extends Configured implements Tool {
	
	/*
	 * The FloatArrayWritable consists of 3 entries: (type, latitude/price, longitude/count)
	 * type: 0 = latitude/longitude
	 * 		 1 = price/count
	 */
	
	protected static final int typeIndex = 0, latPriceIndex = 1, longCountIndex = 2;
	
	public static class GasPriceMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {
		
		private static final Text outKey = new Text();
		private static final FloatArrayWritable outValue = new FloatArrayWritable(1,0,1);
		
		public void map (Object key, Text value, Context c) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			float gasPrice = Integer.parseInt(values[4])/10;
			outKey.set(values[1]);
			outValue.set(latPriceIndex, gasPrice);		
			c.write(outKey,outValue);
		}
	}
	
	public static class GasStationMapper extends Mapper<Object, Text, Text, FloatArrayWritable> {
		
		private static final Text outKey = new Text();
		private static final FloatArrayWritable outValue = new FloatArrayWritable(0,0,0);
		
		public void map (Object key, Text value, Context c) throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			String latitude;
			String longitude;
			if(values.length == 11) {
				latitude = values[9];
				longitude = values[10];
			} else {
				latitude = values[10];
				longitude = values[11];
			}
			outKey.set(values[0]);
			outValue.set(latPriceIndex, Float.parseFloat(latitude));
			outValue.set(longCountIndex, Float.parseFloat(longitude));
			c.write(outKey,outValue);
		}
	}
	
	public static class GasPriceReducer extends Reducer<Text, FloatArrayWritable, Text, Text> {
		
		private static final Text outKey = new Text();
		private static final Text outValue = new Text();
		private static final TreeMap<Float,Float> gasPriceMap = new TreeMap<>();
		private static final StringBuilder outStringBuilder = new StringBuilder();
		
		public void reduce (Text key, Iterable<FloatArrayWritable> values, Context c) throws IOException, InterruptedException {
			gasPriceMap.clear();
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
			
			// there was no entry for the gasstation
			if(gasPriceMap.size() == 0) return;
			
			Iterator<Float> gasPrices = gasPriceMap.keySet().iterator();
			
			float price = gasPrices.next();
			while(gasPrices.hasNext()) {		
				outStringBuilder.append((int) price).append(':').append(gasPriceMap.get(price).intValue()).append('\t');
				price = gasPrices.next();
			}
			
			outStringBuilder.append((int) price).append(':').append(gasPriceMap.get(price).intValue());
			
			outKey.set(latitude + "\t" + longitude);
			outValue.set(outStringBuilder.toString());
			
			c.write(outKey, outValue);
			
			outStringBuilder.setLength(0);
			
		}
	}
	
	public static class GasPriceCombiner extends Reducer<Text, FloatArrayWritable, Text, FloatArrayWritable> {
		
		private static final FloatArrayWritable outValue = new FloatArrayWritable(0,0,0);
		private static final HashMap<Float,Float> gasPriceMap = new HashMap<>();
		
		public void reduce (Text key, Iterable<FloatArrayWritable> values, Context c) throws IOException, InterruptedException {
			gasPriceMap.clear();
			
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
				outValue.set(latPriceIndex, price);
				outValue.set(longCountIndex, gasPriceMap.get(price));
				c.write(key, outValue);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length!=3) {
			System.err.printf("Usage: %s [generic options] <input1> <input2> <output>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, GasPriceJoin.class.getSimpleName());
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GasPriceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GasStationMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setJarByClass(GasPriceJoin.class);
		job.setCombinerClass(GasPriceCombiner.class);
		job.setReducerClass(GasPriceReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		return (job.isSuccessful() ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new GasPriceJoin(), args);
		System.exit(exitcode);
	}

}
