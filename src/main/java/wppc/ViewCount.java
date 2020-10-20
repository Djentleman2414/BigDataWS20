package wppc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Für die Wikipedia PageCount-Daten steckt das Datum und der Zeitpunkt im
 * Dateinamen, wird aber nicht in jeder Zeile wiederholt. Also benötigen wir den
 * Dateinamen im Mapper.
 * 
 * Beispielzeilen:
 * 
 * <pre>
 *       fr.b Special:Recherche/Achille_Baraguey_d%5C%27Hilliers 1 624
 *       fr.b Special:Recherche/Acteurs_et_actrices_N 1 739
 *       fr.b Special:Recherche/Agrippa_d/%27Aubign%C3%A9 1 743
 * </pre>
 * 
 * Achtung: Programm macht noch nichts wirklich Sinnvolles.
 */

public class ViewCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// fr.b page 1 624 -> [fr.b,page,1,624]
			String[] values = value.toString().split("\\s"); // white-space split
			
			FileSplit split = (FileSplit) context.getInputSplit();
			String filename = split.getPath().getName();
			// pagecounts-20130109-120000.gz -> [pagecounts,20130109,120000,gz]
			String[] parts = filename.split("[-\\.]");  

			// (Uhrzeit,Clicks)
			context.write(new Text(parts[2]), new IntWritable(Integer.parseInt(values[3])));
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "view count");
		job.setJarByClass(ViewCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
