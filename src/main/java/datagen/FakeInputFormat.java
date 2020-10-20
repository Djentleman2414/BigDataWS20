package datagen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FakeInputFormat extends InputFormat<LongWritable, Text> {

	public static final String FAKERECORDS = "fake.records";
	public static final String FAKESPLITS = "fake.splits";

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		int noOfSplits = context.getConfiguration().getInt(FAKESPLITS, 1);
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		for (int i=0;i<noOfSplits;++i) {
			FakeInputSplit fis = new FakeInputSplit();
			fis.setShard(i);
			splits.add(fis); 
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FakeRecordReader frr = new FakeRecordReader();
		frr.initialize(split,context);

		int records = context.getConfiguration().getInt(FAKERECORDS, 1);
		int shards = context.getConfiguration().getInt(FAKESPLITS, 1);
		int shard = ((FakeInputSplit) split).getShard();
		frr.setIterator(new ShardedIterator(new WordIterator(records),shard,shards));

		return frr;
	}

}
