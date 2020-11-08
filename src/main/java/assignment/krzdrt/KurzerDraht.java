package assignment.krzdrt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import types.IntPairWritable;

public class KurzerDraht extends Configured implements Tool {
	
	public static final int TARGET_ID = 10000;
	
	public static class MapValue implements WritableComparable<MapValue>, Cloneable {	
		public static final int adjacentToTarget = 1;
		public static final int distanceType = 2;
		public static final int adjacentActorsType = 3;
		
		private int valueType;
		private int distance;
		private HashSet<Integer> adjacentActors;
		
		public MapValue() {
		}
		
		public MapValue(int valueType) {
			setValueType(valueType);
		}
		
		public void setValueType(int valueType) {
			this.valueType = valueType;
			
			if(valueType == adjacentActorsType && adjacentActors ==  null)
				adjacentActors = new HashSet<>();
		}
		
		public int getValueType() {
			return valueType;
		}
		
		public void setAdjacentToTarget() {
			valueType = adjacentToTarget;
		}
		
		public boolean isAdjacentToTarget() {
			return valueType == adjacentToTarget;
		}
		
		public void setDistance(int distance) {
			valueType = distanceType;
			this.distance = distance;
		}
		
		public int getDistance() {
			if(valueType != distanceType)
				return -1;
			return distance;
		}
		
		public void setActors(HashSet<Integer> adjacentActors) {
			setValueType(adjacentActorsType);
			this.adjacentActors = adjacentActors;
		}
		
		public void addActors(HashSet<Integer> actors) {
			adjacentActors.addAll(actors);
		}
		
		public void addActor(int actor) {
			adjacentActors.add(actor);
		}
		
		public HashSet<Integer> getAdjacentActors() {
			return adjacentActors;
		}
		
		

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(valueType);
			switch(valueType) {
			case distanceType:
				out.writeInt(distance);
				break;
			case adjacentActorsType:
				writeAdjacentActors(out);
				break;
			}
		}
		
		private void writeAdjacentActors(DataOutput out) throws IOException {
			if(adjacentActors == null) {
				out.writeInt(0);
				return;
			}
			
			out.writeInt(adjacentActors.size());
			for(int actor : adjacentActors) {
				out.writeInt(actor);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			valueType = in.readInt();
			
			switch(valueType) {
			case distanceType:
				distance = in.readInt();
				break;
			case adjacentActorsType:
				readAdjacentActors(in);
				break;
			}
		}
		
		private void readAdjacentActors(DataInput in) throws IOException {
			adjacentActors = new HashSet<>();
			int size = in.readInt();
			
			for(int i = 0; i < size; i++) {
				adjacentActors.add(in.readInt());
			}
		}

		@Override
		public int compareTo(MapValue other) {
			int dist = valueType - other.valueType;
			if(dist != 0)
				return dist;
			dist = distance - other.distance;
			if(dist != 0)
				return dist;
			dist = adjacentActors.size() - other.adjacentActors.size();
			return dist;
		}
		
		@Override
		public boolean equals(Object o) {
			if(this == o) {
				return true;
			}
			if(o == null)
				return false;
			if(o.getClass() != this.getClass())
				return false;
			MapValue other = (MapValue) o;
			if(this.valueType != other.valueType)
				return false;
			if(this.distance != other.distance)
				return false;
			if(this.adjacentActors == null && other.adjacentActors != null ||
					this.adjacentActors != null && other.adjacentActors ==  null)
				return false;
			if(this.adjacentActors != other.adjacentActors && !this.adjacentActors.equals(other.adjacentActors))
				return false;
			return true;
		}
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(valueType);
			switch(valueType) {
			case 2:
				sb.append(':').append(distance);
				break;
			case 3:
				sb.append(':');
				for(int actor : adjacentActors) {
					sb.append(actor).append(',');
				}
				break;
			}
			return sb.toString();
		}
		
		
		public MapValue clone() {	
			MapValue copy = new MapValue(valueType);
			switch(valueType) {
			case 2:
				copy.setDistance(distance);
				break;
			case 3:
				for(int actor : adjacentActors) {
					copy.addActor(actor);
				}
				break;
			}
			
			return copy;
		}
		
	}
	
	public static class ContactMapper extends Mapper<Object, Text, IntPairWritable, MapValue> {
		
		private final IntPairWritable outKey = new IntPairWritable();
		private final MapValue outValue = new MapValue();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			if(value.getLength() == 0 || value.charAt(0) == '%')
				return;
			
			String[] idStrings = value.toString().split(" ");
			int id1 = Integer.parseInt(idStrings[0]);
			int id2 = Integer.parseInt(idStrings[1]);
			
			if(id2 == TARGET_ID) {
				outKey.set(1, id1);
				outValue.setAdjacentToTarget();
				context.write(outKey, outValue);
				return;
			} else {
				outKey.set(3, id1);
				HashSet<Integer> actorSet = new HashSet<>();
				actorSet.add(id2);
				outValue.setActors(actorSet);
				context.write(outKey, outValue);
			}		
		}
	}
	
	public static class DistanceMapper extends Mapper<Object, Text, IntPairWritable, MapValue> {
		
		private final IntPairWritable outKey = new IntPairWritable(2,0);
		private final MapValue outValue = new MapValue(2);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] valueArray = value.toString().split("\t");
			int id = Integer.parseInt(valueArray[0]);
			int distance = Integer.parseInt(valueArray[1]);
			
			outKey.setY(id);
			outValue.setDistance(distance);
			
			context.write(outKey, outValue);
		}
	}
	
	public static class MyPartitioner extends Partitioner<IntPairWritable, MapValue> {

		@Override
		public int getPartition(IntPairWritable key, MapValue value, int numPartitions) {
			return key.getY() % numPartitions;
		}
		
	}
	
	public static class KeyOrderComparator extends WritableComparator {
		
		public KeyOrderComparator() {
			super(IntPairWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPairWritable left = (IntPairWritable) a;
			IntPairWritable right = (IntPairWritable) b;
			
			int dist = Integer.compare(left.getY(), right.getY());
			if(dist != 0)
				return dist;
			return Integer.compare(left.getX(), right.getX());
		}
	}
	
	public static class GroupingComparator extends WritableComparator {
		public GroupingComparator() {
			super(IntPairWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPairWritable left = (IntPairWritable) a;
			IntPairWritable right = (IntPairWritable) b;
			
			return new Integer(left.getY()).compareTo(new Integer(right.getY()));
		}
	}
	
	public static class DistanceReducer extends Reducer<IntPairWritable, MapValue, IntWritable, IntWritable> {
		
		private final IntWritable outKey = new IntWritable();
		private final IntWritable outValue = new IntWritable();
		
		public void reduce(IntPairWritable key, Iterable<MapValue> values, Context context) throws IOException, InterruptedException {
			int id = key.getY();
			int distance = -1;
			Iterator<MapValue> valueIterator = values.iterator();
			
			while(valueIterator.hasNext() && distance == -1) {
				MapValue value = valueIterator.next();
				switch(value.getValueType()) {
				case 1:
					distance = 1;
					break;
				case 2:
					distance = value.getDistance();
					break;
				case 3:
					return;
				}
			}
			while(valueIterator.hasNext()) {
				MapValue value = valueIterator.next();
				switch(value.getValueType()) {
				case 2:
					distance = distance <= value.getDistance() ? distance : value.getDistance();
					break;
				case 3:
					for(int actor : value.getAdjacentActors()) {				
						outKey.set(actor);
						outValue.set(distance + 1);
						context.write(outKey, outValue);
					}
					break;
				}
			}
			
			outKey.set(id);
			outValue.set(distance);
			context.write(outKey, outValue);
			
		}
	}
	
	public static class DistanceCombiner extends Reducer<IntPairWritable, MapValue, IntPairWritable, MapValue> {		
		
		private final MapValue outValue = new MapValue();
		
		public void reduce(IntPairWritable key, Iterable<MapValue> values, Context context) throws IOException, InterruptedException {
			
			Iterator<MapValue> valueIterator = values.iterator();
			
			while(valueIterator.hasNext()) {
				MapValue value = valueIterator.next();
				
				switch(value.getValueType()) {
				case 1:
					outValue.setAdjacentToTarget();
					context.write(key, outValue);
					break;
				case 2:
					combineDistance(value, valueIterator);
					context.write(key, outValue);
					break;
				case 3:
					combineAdjacentActors(value, valueIterator);
					context.write(key, outValue);
					break;
				}
			}		
		}

		private void combineDistance (MapValue value, Iterator<MapValue> values)  {
			int distance = value.getDistance();
			
			while(values.hasNext() && value.getValueType() == 2) {
				value = values.next();
				distance = distance <= value.getDistance()? distance : value.getDistance();
			}
			outValue.setDistance(distance);
		}

		private void combineAdjacentActors (MapValue value, Iterator<MapValue> values) {
			
			outValue.setValueType(3);
			outValue.setActors(value.getAdjacentActors());
			
			while(values.hasNext() && value.getValueType() == 3) {
				value = values.next();
				outValue.addActors(value.getAdjacentActors());
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length!=2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, KurzerDraht.class.getSimpleName());
		Path input = new Path(args[0]);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "job0/"));
		
		job.setJarByClass(KurzerDraht.class);
		job.setMapperClass(ContactMapper.class);
		job.setCombinerClass(DistanceCombiner.class);
		job.setReducerClass(DistanceReducer.class);
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(KeyOrderComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(MapValue.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		
		for(int i = 0; i < 2; i++) {
			
			Path tempInput = new Path(args[1] + "job" + i);
			job = Job.getInstance(config, KurzerDraht.class.getSimpleName());
			MultipleInputs.addInputPath(job, input, TextInputFormat.class, ContactMapper.class);
			MultipleInputs.addInputPath(job, tempInput, TextInputFormat.class, DistanceMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1] + "job" + (i+1) + "/"));
			
			job.setJarByClass(KurzerDraht.class);
			job.setCombinerClass(DistanceCombiner.class);
			job.setReducerClass(DistanceReducer.class);
			
			job.setPartitionerClass(MyPartitioner.class);
			job.setSortComparatorClass(KeyOrderComparator.class);
			job.setGroupingComparatorClass(GroupingComparator.class);
			
			job.setMapOutputKeyClass(IntPairWritable.class);
			job.setMapOutputValueClass(MapValue.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.waitForCompletion(true);
			
			try(FileSystem fs = input.getFileSystem(getConf())) {
				fs.delete(tempInput, true);
			}
			
		}
		
		
		
		job = Job.getInstance(config, KurzerDraht.class.getSimpleName());
		
		FileInputFormat.addInputPath(job, new Path(args[1] + "job2"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "final/"));
		
		job.setJarByClass(KurzerDraht.class);
		job.setMapperClass(DistanceMapper.class);
		job.setCombinerClass(DistanceCombiner.class);
		job.setReducerClass(DistanceReducer.class);
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(KeyOrderComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(MapValue.class);
		job.setOutputKeyClass(IntWritable.class);
		
		int complete = job.waitForCompletion(true) ? 0 : 1;
		
		try(FileSystem fs = input.getFileSystem(getConf())) {
			fs.delete(new Path(args[1] + "job2"), true);
		}
		
		return complete;
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new KurzerDraht(), args);
		System.exit(exitcode);

	}

}
