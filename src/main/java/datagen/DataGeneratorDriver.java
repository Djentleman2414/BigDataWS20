package datagen;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Beispiel zur Erzeugung von Fake-Daten. Hier: simulierte Datei (beliebiger
 * Größe) mit einem neuen 5-stelligen Wort (alle 7 Positionen in der
 * Eingabedatei).
 */

public class DataGeneratorDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		Path out = new Path("datagen");
		
		int numberOfMappers = 5;
		int numberOfRecords = 100_000;
		conf.setInt(FakeInputFormat.FAKESPLITS, numberOfMappers); 
		conf.setInt(FakeInputFormat.FAKERECORDS, numberOfRecords); 
		conf.setInt(FakeRecordReader.FAKERECORDSIZE, 6); 
		
		job.setNumReduceTasks(0);
		job.setJarByClass(DataGeneratorDriver.class);
		job.setInputFormatClass(FakeInputFormat.class);
		FileOutputFormat.setOutputPath(job, out);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
	}
}
