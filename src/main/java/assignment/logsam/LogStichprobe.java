package assignment.logsam;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import types.FloatArrayWritable;
import types.IntDoubleWritable;
import types.IntTrippleWritable;

public class LogStichprobe {

	protected static final double anteil = 0.1;

	/**
	 * 
	 * In der VL wurden zwei Varianten für "faire" Stichproben vorgestellt. Für die
	 * erste Variante muss bekannt sein, wie groß die Stichprobe absolut sein soll
	 * (Wert k). Für die zweite Variante muss bekannt sein, welchen Anteil die
	 * Stichproble an der Gesamtmenge hat (in dieser aufgabe gegeben). Deswegen habe
	 * ich einfach diese Art der Stichprobenziehung gewählt.
	 * 
	 */
	public static class LogStichprobeMapper extends Mapper<Object, Text, Text, Text> {

		private static final Text outKey = new Text();
		private static final Text outValue = new Text();

		public void map(Object key, Text value, Context c) throws IOException, InterruptedException {
			if (Math.random() <= anteil) {
				String[] values = value.toString().split(" ");
				/**
				 * Key: "ProduktHash KundenHash"
				 * 
				 * Value: "ProduktHash"
				 * 
				 * Grund: Ich will, dass identische Values zusammen vorkommen
				 * (GroupingComparator)
				 */
				outKey.set(values[3] + " " + values[2]);
				outValue.set(values[3]);
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
	 * wichtig, dass gleiche Values dirket hintereinander kommen. Dann wird
	 * gecheckt, ob der Value gleich dem letzten Value ist.
	 * 
	 * 
	 * Ich habe den Reducer jetzt erstmal so erstellt, dass er mit der zweiten
	 * Variante arbeitet. Den SecondarSort an sich muss ich aber noch
	 * implementieren. Vorteil: Skalierbar, da ich nicht wissen muss, wie viele
	 * Produkte pro Kunde wahrscheiinlich betrachtet werden.
	 *
	 */
	public static class LogStichprobeReducer extends Reducer<Text, Text, Text, Text> {

		private static int summeKunden = 0;
		private static double summeAnteile = 0;

		public void reduce(Text key, Iterable<Text> values, Context c) throws IOException, InterruptedException {
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

			String vorherigesProdukt = "";
			boolean schongesehen = false;

			for (Text value : values) {
				String v = value.toString();
				if (v.equals(vorherigesProdukt)) {
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
		 * Hier wird dann das Ergebnis überchrieben
		 */
		public void cleanup(Context c) throws IOException, InterruptedException {
			Text outKey = new Text("Anteil an Mehrfachabfragen: " + ((summeAnteile * 100) / summeKunden) + "%");
			Text outValue = new Text("");
			c.write(outKey, outValue);
		}
	}

	/**
	 * Ich habe etwas am SecondarySort rumprobiert, aber das Ganze bis jetzt noch
	 * nicht wirklich durchdrungen
	 * 
	 * An Position [,x] im Key steht der Kunde. Ich will, dass ein Aufruf von
	 * reduce() alle Key-Value-Paare desselben Kunden bekommt. Gleichzeitig will
	 * ich, dass alle Einträge desselbem Produkts (Position [x,]) im Reducer direkt
	 * hintereinander kommen.
	 *
	 */
	public static class LogStichprobeGroupingComparator extends WritableComparator {

		public LogStichprobeGroupingComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			String[] left = ((Text) a).toString().split(" ");
			String[] right = ((Text) b).toString().split(" ");
			int dist = left[0].compareTo(right[0]);
			if (dist != 0)
				return dist;

			return left[1].compareTo(right[1]);
		}
	}

	/**
	 * 
	 * public static class LogStichprobePartitioner extends Partitioner<Text, Text>
	 * {
	 * 
	 * @Override public int getPartition(Text key, Text value, int numPartitions) {
	 *           return key.toString().charAt(0) % numPartitions; }
	 * 
	 *           }
	 * 
	 * 
	 * 
	 **/

}
