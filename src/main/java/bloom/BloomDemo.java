package bloom;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomDemo {

	// liefert m = Anzahl Bits
	public static int getBloomFilterOptimalSize(long numElements, float falsePosRate) {
		return (int) (-numElements * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2));
	}

	// liefert k = Anzahl Hashfunktionen
	public static int getOptimalK(float numElements, float vectorSize) {
		return (int) Math.round(vectorSize * Math.log(2) / numElements);
	}

	public static void main(String[] args) throws IOException {

		String[] vals = new String[] { "hello", "world" };

		int numbermembers = vals.length;
		float fprate = 0.1f;
		int filtersize = getBloomFilterOptimalSize(numbermembers, fprate);
		int numberhashes = getOptimalK(numbermembers, filtersize);
		System.out.println("n="+numbermembers+" p="+fprate+" m="+filtersize+" k="+numberhashes);

		BloomFilter filter = new BloomFilter(filtersize, numberhashes, Hash.MURMUR_HASH);
		for (String word : vals) {
			filter.add(new Key(word.getBytes()));
		}
		
		System.out.println(filter);

		System.out.println(filter.membershipTest(new Key("test".getBytes())));
		System.out.println(filter.membershipTest(new Key("hello".getBytes())));
		
		// serialize to local file system: read/write(DataIn/OutputStream)
		DataOutputStream dataOut = new DataOutputStream(new FileOutputStream("filter.bloom"));
		filter.write(dataOut);
		dataOut.flush();
		dataOut.close();
		
		// serialize to HDFS
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("bloom.filter"), conf);
		DataOutputStream out = new DataOutputStream(fs.create(new Path("bloom.filter")));
		filter.write(out);
		out.close();

	}

}
