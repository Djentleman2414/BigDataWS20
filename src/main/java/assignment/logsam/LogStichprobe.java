package assignment.logsam;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Iterables;

import org.apache.hadoop.mapreduce.Mapper.Context;

import types.DoublePairWritable;
import types.FloatArrayWritable;
import types.IntDoubleWritable;
import types.IntTrippleWritable;
import types.LongPairWritable;

public class LogStichprobe extends Configured implements Tool {

	public static double anteil = 0.1;

	/**
	 * In der VL wurden zwei Varianten für "faire" Stichproben vorgestellt. Für die
	 * erste Variante muss bekannt sein, wie groß die Stichprobe absolut sein soll
	 * (Wert k). Für die zweite Variante muss bekannt sein, welchen Anteil die
	 * Stichproble an der Gesamtmenge hat (in dieser aufgabe gegeben). Deswegen habe
	 * ich einfach diese Art der Stichprobenziehung gewählt.
	 * 
	 */
	public static class LogStichprobeMapperEins extends Mapper<Object, Text, LongPairWritable, LongWritable> {
		// id in long parsen dann longpairwritable

		private static final LongPairWritable outKey = new LongPairWritable();
		private static final LongWritable outValue = new LongWritable();

		public void map(Object key, Text value, Context c) throws IOException, InterruptedException {
			if (Math.random() <= anteil) {
				String[] values = value.toString().split(" ");
				long kunde = Long.parseLong(values[2], 16);
				long produkt = Long.parseLong(values[3], 16);
				/**
				 * Key: "ProduktHash KundenHash"
				 * 
				 * Value: "ProduktHash"
				 * 
				 * Grund: Ich will, dass identische Values zusammen vorkommen (ich hoffe das
				 * kann der GroupingComparator)
				 */
				outKey.set(produkt, kunde);
				outValue.set(produkt);
				c.write(outKey, outValue);
			}
		}

	}

	/**
	 * Hier gibt es jetzt prinzipiell zwei Möglichkeiten:
	 * 
	 * 1. Bloom Filter: Es gibt 2 Bitfolgen. Die Eine steht für alle angeschauten
	 * Produkte und die andere steht für alle min. 2 mal angeschaute Produkte. Bei
	 * dieser Lösung ist die Sortierung der Values egal.
	 * 
	 * 2. Secondary Sort: Es wird kein Bloom Filter verwendet. Die Values kommen
	 * sortiert an. Die Art der Sortierung selbst spielt keine Rolle. Es ist nur
	 * wichtig, dass gleiche Values direkt hintereinander kommen. Dann wird
	 * gecheckt, ob der Value gleich dem letzten Value ist.
	 * 
	 * 
	 * Ich habe den Reducer jetzt erstmal so erstellt, dass er mit der zweiten
	 * Variante arbeitet. Vorteil: Skalierbar, da ich nicht wissen muss, wie viele
	 * Produkte pro Kunde wahrscheiinlich betrachtet werden.
	 * 
	 * In zwei Phasen splitten
	 *
	 */
	public static class LogStichprobeReducerEins
			extends Reducer<LongPairWritable, LongWritable, IntWritable, DoubleWritable> {

		private int summeKunden = 0;
		private double summeAnteile = 0;

		public void reduce(LongPairWritable key, Iterable<LongWritable> values, Context c)
				throws IOException, InterruptedException {
			int angeschauteProdukte = 0;
			int mehrfachabfragen = 0;

			/**
			 * Beispiel: 0 1 2 2 2 3 4 4
			 * 
			 * Ergebnis:
			 * 
			 * angeschauteProdukte = 5
			 * 
			 * mehrfachabfragen= 2
			 * 
			 * Dat müsste so funktionieren
			 */

			long vorherigesProdukt = 0;
			boolean schongesehen = false;
			for (LongWritable value : values) {
				long v = value.get();
				if (v == vorherigesProdukt) {
					if (!schongesehen) {
						schongesehen = true;
						mehrfachabfragen++;
					}
				} else {
					schongesehen = false;
					angeschauteProdukte++;
					vorherigesProdukt = v;
				}
			}

			double anteilKunde = 0;
				anteilKunde = (double) mehrfachabfragen / angeschauteProdukte;
			summeAnteile += anteilKunde;
			summeKunden++;

		}

		/**
		 * Hier wird dann das Ergebnis überschrieben
		 */
		public void cleanup(Context c) throws IOException, InterruptedException {
			IntWritable outKey = new IntWritable(summeKunden);
			DoubleWritable outValue = new DoubleWritable(summeAnteile);
			c.write(outKey, outValue);
		}
	}

	public static class LogStichprobeMapperZwei extends Mapper<Object, Text, IntWritable, IntDoubleWritable> {

		private static final IntWritable outKey = new IntWritable();
		private static final IntDoubleWritable outValue = new IntDoubleWritable();

		public void map(Object key, Text value, Context c) throws IOException, InterruptedException {

			String[] values = value.toString().split("\t");
			int summeKunden = Integer.parseInt(values[0]);
			double summeAnteile = Double.parseDouble(values[1]);

			outValue.set(summeKunden, summeAnteile);
			c.write(outKey, outValue);
		}
	}

	public static class LogStichprobeReducerZwei
			extends Reducer<IntWritable, IntDoubleWritable, Text, DoubleWritable> {

		private double summeKunden = 0;
		private double summeAnteile = 0;

		public void reduce(IntWritable key, Iterable<IntDoubleWritable> values, Context c)
				throws IOException, InterruptedException {
			for (IntDoubleWritable dp : values) {
				summeKunden += dp.getInt();
				summeAnteile += dp.getDouble();
			}
			
			Text outKey = new Text("Der Anteil an Mehrfachabfragen beträgt:");
			DoubleWritable outValue = new DoubleWritable(summeAnteile / summeKunden);
			c.write(outKey, outValue);

		}
	}

	/**
	 * Zuerst Vergleich nach Kunde und anschließend nach Produkt, damit identische
	 * Produkte direkt hintereinander in den Reducer kommen
	 *
	 */
	public static class LogStichprobeSortComparator extends WritableComparator {

		public LogStichprobeSortComparator() {
			super(LongPairWritable.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			LongPairWritable left = (LongPairWritable) a;
			LongPairWritable right = (LongPairWritable) b;
			int kundenVergleich = Long.compare(left.getY(), right.getY());
			if (kundenVergleich != 0) {
				return kundenVergleich;
			}
			return Long.compare(left.getX(), right.getX());
		}
	}

	/**
	 * Für das Grouping ist ein Vergleich des Kunden ausreichend
	 */
	public static class LogStichprobeGroupingComparator extends WritableComparator {

		public LogStichprobeGroupingComparator() {
			super(LongPairWritable.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			LongPairWritable left = (LongPairWritable) a;
			LongPairWritable right = (LongPairWritable) b;
			// Nur Vergleich nach Kunde für's Grouping nötig
			return Long.compare(left.getY(), right.getY());
		}
	}

	/**
	 * Quasi ein StandardPartitioner
	 */
	public static class LogStichprobePartitioner extends Partitioner<LongPairWritable, LongWritable> {

		@Override
		public int getPartition(LongPairWritable key, LongWritable value, int numPartitions) {
			return (int) (key.getY() % numPartitions);
		}

	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LogStichprobe(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Job job = getFirstJob(args);
		
		if(!job.waitForCompletion(true))
			return -1;
		
		job = getSecondJob(args);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	private Job getFirstJob(String[] args) throws IllegalArgumentException, IOException {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		
		if(!args[1].endsWith("/"))
			args[1] += "/";
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "temp"));
		
		job.setJarByClass(LogStichprobe.class);
		job.setMapperClass(LogStichprobeMapperEins.class);
		job.setReducerClass(LogStichprobeReducerEins.class);
		
		job.setMapOutputKeyClass(LongPairWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(10);
		
		return job;		
	}
	
	private Job getSecondJob(String[] args) throws IOException {
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		
		if(!args[1].endsWith("/"))
			args[1] += "/";
		FileInputFormat.addInputPath(job, new Path(args[1] + "temp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "final"));
		
		job.setJarByClass(LogStichprobe.class);
		job.setMapperClass(LogStichprobeMapperZwei.class);
		job.setReducerClass(LogStichprobeReducerZwei.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntDoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		return job;
	}

}
