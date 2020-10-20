package ncdc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Beispiel für einfachen M/R Job auf NCDC Daten.
 * 
 * Aufruf des M/R Jobs:
 * 
 * <pre>
 * yarn jar hdp.jar hadoop.ncdc.MaxTemp /user/ncdc/190?.gz /user/myname/result
 * </pre>
 * 
 * Anzeige des Ergebnisses:
 * 
 * <pre>
 * hdfs dfs -cat /user/result/part-00000
 * </pre>
 * 
 * Ausgabe:
 * 
 * <pre>
 * 1901	317
 * 1902	244
 * 1903	289
 * 1904	256
 * 1905	283
 * </pre>
 * 
 * Aufräumen vor Neustart
 * 
 * <pre>
 * hdfs dfs -rm -f /user/dbtWXX/result/* 
 * hdfs dfs -rmdir /user/dbtWXX/result
 * </pre>
 */

public class MaxTemp {

	private static final int MISSING = 9999;

	public static class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15, 19);
			String quality = line.substring(92, 93);

			int airTemperature;
			if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}

	public static class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MaxTempMapper.class);
		job.setCombinerClass(MaxTempReducer.class);
		job.setReducerClass(MaxTempReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(MaxTemp.class);

		job.waitForCompletion(true);
	}

}
