package testing;
import org.apache.hadoop.conf.Configuration;

import matmul.MatMul;

public class Dimensionen {

	public static void main(String[] args) {
		Configuration c = new Configuration();
		MatMul m = new MatMul();
		testGroessenZuordnung(100, 100, c, m);
		testGroessenZuordnung(200, 200, c, m);
		testGroessenZuordnung(500, 500, c, m);
		testGroessenZuordnung(1000, 1000, c, m);
		testGroessenZuordnung(2000, 2000, c, m);
		testGroessenZuordnung(4000, 4000, c, m);
		testGroessenZuordnung(5000, 5000, c, m);
		testGroessenZuordnung(10000, 10000, c, m);
		
	}
	
	public static void testGroessenZuordnung(int spalten, int zeilen, Configuration c, MatMul m) {
		m.getMatrixDimensions(c, generateInput(spalten, zeilen));
		System.out.println("Spalten: " + spalten);
		System.out.println("Zeilen: " + zeilen);
		System.out.println("Elemente: " + zeilen * spalten);
		System.out.println("Anzahl Buckets: " + c.getInt("NUM_OF_BUCKETS", 0));
		System.out.println("Bucketgröße: " + c.getInt("MAX_BUCKET_SIZE", 0));
	}
	
	public static String generateInput(int spalten, int zeilen) {
		String s = zeilen + "-" + spalten + ".";
		return s;
	}


}
