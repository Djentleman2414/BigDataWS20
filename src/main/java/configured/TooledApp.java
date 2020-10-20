package configured;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Beispiel für ToolRunner-Einsatz. Die eigentliche Anwendung wird als Tool 
 * implementiert, die Funktionalität in run.
 * yarn jar hadoop-0.0.1-SNAPSHOT.jar configured.TooledApp linelengths x
 * 
 * Damit können folgende Kommandozeilenoptionen behandelt werden (werden aber
 * selten benötigt, daher Tool nicht unbedingt zwingend). 
<pre>
    -archives <comma separated list of archives>- Specify comma separated archives to be unarchived on the compute machines. Applies only to job.
    -conf <configuration file>- Specify an application configuration file.
    -D <property>=<value>- Use value for given property.
    -files <comma separated list of files>- Specify comma separated files to be copied to the map reduce cluster. Applies only to job.
    -fs <file:///> or <hdfs://namenode:port>- Specify default filesystem URL to use. Overrides ‘fs.defaultFS’ property from configurations.
    -jt <local> or <resourcemanager:port>- Specify a ResourceManager. Applies only to job.
    -libjars <comma seperated list of jars>- Specify comma separated jar files to include in the classpath. Applies only to job.
</pre>	
 */

public class TooledApp extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.printGenericCommandUsage(System.out);

		int result = -1;
		if (ToolRunner.confirmPrompt("Shall we start?")) {
			result = ToolRunner.run(new TooledApp(), args);
		}

		System.exit(result);
	}

	public static class DummyMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context ctx) {
		}
	}

	public static class DummyReducer
		extends Reducer<Text, Text, Text, Text> {
	}

	public int run(String[] args) throws Exception {
		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "job name");
		job.setJarByClass(getClass());

		job.setMapperClass(DummyMapper.class);
		job.setReducerClass(DummyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int result = job.waitForCompletion(true) ? 0 : 1;

		// remove empty output path
		FileSystem.get(conf).delete(new Path(args[1]),true);

		return result;
	}

}
