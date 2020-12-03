package assignment.logsam;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

import org.apache.hadoop.mapreduce.Mapper.Context;

import types.DoublePairWritable;
import types.FloatArrayWritable;
import types.IntDoubleWritable;
import types.IntTrippleWritable;
import types.LongPairWritable;

public class LogStichprobe {

	protected static final double anteil = 0.1;

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

		private static int summeKunden = 0;
		private static double summeAnteile = 0;

		public void reduce(LongPairWritable key, Iterable<LongWritable> values, Context c)
				throws IOException, InterruptedException {
			int angeschauteProdukte = 0;
			int mehrfachabfragen = 0;

			int i = Iterables.size(values);

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
			if (mehrfachabfragen != 0) {
				anteilKunde = (double) angeschauteProdukte / mehrfachabfragen;
			} else {
				anteilKunde = 1;
			}
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

	public static class LogStichprobeMapperZwei extends Mapper<Object, Text, IntWritable, DoublePairWritable> {

		private static final IntWritable outKey = new IntWritable();
		private static final DoublePairWritable outValue = new DoublePairWritable();

		public void map(Object key, Text value, Context c) throws IOException, InterruptedException {

			String[] values = value.toString().split(" ");
			int summeKunden = Integer.parseInt(values[0]);
			double summeAnteile = Double.parseDouble(values[1]);

			outKey.set(0);
			outValue.set(summeKunden, summeAnteile);
			c.write(outKey, outValue);
		}
	}

	public static class LogStichprobeReducerZwei
			extends Reducer<IntWritable, DoublePairWritable, Text, DoubleWritable> {

		private static double summeKunden = 0;
		private static double summeAnteile = 0;

		public void reduce(IntWritable key, Iterable<DoublePairWritable> values, Context c)
				throws IOException, InterruptedException {
			for (DoublePairWritable dp : values) {
				summeKunden += dp.getX();
				summeAnteile += dp.getY();
			}

		}

		/**
		 * Hier wird dann das Ergebnis überschrieben
		 */
		public void cleanup(Context c) throws IOException, InterruptedException {
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
			int kundenVergleich = (int) (((LongPairWritable) a).getY() - ((LongPairWritable) b).getY());
			if (kundenVergleich != 0) {
				return kundenVergleich;
			}
			return (int) (((LongPairWritable) a).getX() - ((LongPairWritable) b).getX());
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
			// Nur Vergleich nach Kunde für's Grouping nötig
			return (int) (((LongPairWritable) a).getY() - ((LongPairWritable) b).getY());
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

}
