package assignment.tmpres;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TemperatureResolution extends Configured implements Tool {
	
	public static class TemperatureResolutionMapper extends Mapper<Object, Text, Text, Text> {
		
		private final static Text oneEntry = new Text();
		private Text date = new Text();
		
		/*
		 * The output key has the form: LatLongyyyymm
		 * The output value has the form: StationID+dd+1
		 */
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if(!line.subSequence(88, 92).equals("9999")) {
				// get all relevant information from the input-line -> see 3:43 in big.pdf
				String latitude = line.substring(28,31);
				String longitude = line.substring(34,38);
				String latLong = latitude + longitude;
				String yearMonth = line.substring(15, 21);
				String stationId = line.substring(4, 15);
				String day = line.substring(21, 23);
				date.set(new StringBuilder(latLong).append(yearMonth).toString());
				oneEntry.set(new StringBuilder(stationId).append('+').append(day).append('+').append(1).toString());
				context.write(date, oneEntry);
			}
			
		}
	}
	
	
	/*
	 * Each value is split at '+', all unique stationIds and days are counted
	 * and the numberOfMeasurements at the end of the value string (valueArray[2]) is added up
	 * The total number of Measurements is then divided by (number of station ids * number of days)
	 * to get the mean-value.
	 * The output key is Lat Long yyyy mm divided by \t
	 */
	public static class TemperatureResolutionReducer extends Reducer<Text, Text, Text, FloatWritable> {	
		
		private FloatWritable mean = new FloatWritable();
		
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String keyString = key.toString();
			
			String latitude = keyString.substring(0,3).replaceAll("[+]", "");
			String longitude = keyString.substring(3,7).replaceAll("[+]", "");
			String year = keyString.substring(7,11);
			String month = keyString.substring(11,13);
			
			int numOfStations = 0;
			int numOfDays = 0;
			int numOfMeasurements = 0;
			HashSet<String> stations = new HashSet<>();
			HashSet<Integer> days = new HashSet<>();
			String[] valueArray;
			for (Text value : values) {
				valueArray = value.toString().split("[+]");
				String stationId = valueArray[0];
				int day = Integer.parseInt(valueArray[1]);
				
				
				if(!stations.contains(stationId)) {
					stations.add(stationId);
					numOfStations++;
				}
					
				if(!days.contains(day)) {
					days.add(day);
					numOfDays++;
				}
					
				numOfMeasurements += Integer.parseInt(valueArray[2]);
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
			mean.set(((float) numOfMeasurements) / (numOfStations * numOfDays));
			
			context.write(key, mean);
		}
	}
	
	/*
	 * For each unique combination of stationId+dd the number of measurements (at the end of value)
	 * is added up and written back as the new value (stationId+dd+numOfMeasurements)
	 */
	public static class TemperatureResolutionCombiner extends Reducer<Text, Text, Text, Text> {
		
		private Text combinedMeasurements = new Text();
		
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> dayMap = new HashMap<>();

			for (Text value : values) {
				String valueString = value.toString();
				String dayStationId = valueString.substring(0,14);
				int previousValue = Integer.parseInt(valueString.substring(15, valueString.length()));

				if (dayMap.containsKey(dayStationId)) dayMap.put(dayStationId, dayMap.get(dayStationId) + previousValue);
				else dayMap.put(dayStationId, previousValue);
			}
			
			Iterator<String> dayStationIds = dayMap.keySet().iterator();
			while (dayStationIds.hasNext()) {
				String dayStationId = dayStationIds.next();
				String value = new StringBuilder(dayStationId).append('+').append(dayMap.get(dayStationId)).toString();
				combinedMeasurements.set(value);
				context.write(key, combinedMeasurements);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new TemperatureResolution(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length!=2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(TemperatureResolution.class);
		job.setMapperClass(TemperatureResolutionMapper.class);
		job.setCombinerClass(TemperatureResolutionCombiner.class);
		job.setReducerClass(TemperatureResolutionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}
