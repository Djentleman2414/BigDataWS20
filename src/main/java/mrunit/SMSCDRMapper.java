package mrunit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static enum CounterType { CALL_COUNTER, WRITE_COUNTER };
	
	private Text status = new Text();
	private final static IntWritable addOne = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		// 655209;1;796764372490213;804422938115889;6 is the Sample record
		// format
		String[] line = value.toString().split(";");
		// If record is of SMS CDR
		if (Integer.parseInt(line[1]) == 22) {
			status.set(line[4]);
			context.write(status, addOne);
			context.getCounter(CounterType.WRITE_COUNTER).increment(1);
		}
		
		context.getCounter(CounterType.CALL_COUNTER).increment(1);
	}
}