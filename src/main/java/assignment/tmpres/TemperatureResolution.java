package assignment.tmpres;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import types.IntPairWritable;

public class TemperatureResolution extends Configured implements Tool {
	
	public static class TemperatureResolutionMapper extends Mapper<Object, Text, Text, IntPairWritable> {
		
		private final static IntPairWritable oneEntry = new IntPairWritable(1,1);
		private Text date = new Text();
		
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if(!line.subSequence(88, 92).equals("9999")) {
				String yearMonth = line.substring(15, 21);
				int day = Integer.parseInt(line.substring(21, 23));
				String latLong = line.substring(28,40);
				date.set(new StringBuilder(yearMonth).append(latLong).toString());
				oneEntry.setX(day);
				context.write(date, oneEntry);
			}
			
		}
	}
	
	public static class TemperatureResolutionReducer extends Reducer<Text, IntPairWritable, Text, FloatWritable> {	
		
		private FloatWritable mean = new FloatWritable();
		
		public void reduce (Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
			String keyString = key.toString();
			String year = keyString.substring(0, 4);
			String month = keyString.substring(4,6);
			String latitude = keyString.substring(6,11);
			String longitude = keyString.substring(11,16);
			
			int numOfDays = 0;
			int numOfMeasurements = 0;
			HashSet<Integer> days = new HashSet<>();
			for (IntPairWritable value : values) {
				int day = value.getX();
				if(!days.contains(day)) numOfDays++;
				numOfMeasurements += value.getY();
			}
			StringBuilder sb = 
					new StringBuilder(latitude)
					.append('\t')
					.append(longitude)
					.append('\t')
					.append(year)
					.append('\t')
					.append(month);
			
			key.set(sb.toString());
			mean.set(((float) numOfMeasurements) / numOfDays);
			
			context.write(key, mean);
		}
	}
	
	public static class TemperatureResolutionCombiner extends Reducer<Text, IntPairWritable, Text, IntPairWritable> {
		
		private IntPairWritable combinedMeasurements = new IntPairWritable();
		
		public void reduce (Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
			HashMap<Integer, Integer> dayMap = new HashMap<>();
			
			for (IntPairWritable value : values) {
				int day = value.getX();
				if (dayMap.containsKey(day)) dayMap.put(day, dayMap.get(day) + 1);
				else dayMap.put(day, 1);
			}
			
			Iterator<Integer> days = dayMap.keySet().iterator();
			
			while (days.hasNext()) {
				int day = days.next();
				combinedMeasurements.set(day, dayMap.get(day));
				context.write(key, combinedMeasurements);
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

}
