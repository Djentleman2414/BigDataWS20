package assignment.krzdrt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPairWritable;

public class KurzerDraht {
	
	public static final int TARGET_ID = 10000;
	
	public static class MapValue implements WritableComparable<MapValue>, Cloneable {
		
		// 10000 5
		// 5 22 -> (5, (22)), (22, (5))
		// 10 5 -> (5, (10)), (10, (5))
		// 10 12 -> (10, (12))
		// =>
		// (5 (10,22)), ...
		// 10000 22
		
		// 5 1 -> (5,(2,1))
		// 5 2
		// 10 2
		// 22 1
		// 22 2
		// 12 3
		// => min_reducer
		
		
		
		
		public static final int adjacentToTarget = 1;
		public static final int distanceType = 2;
		public static final int adjacentActorsType = 3;
		
		private int valueType;
		private int distance;
		private HashSet<Integer> adjacentActors;
		
		public MapValue() {
			// TODO Auto-generated constructor stub
		}
		
		public MapValue(int valueType) {
			this.valueType = valueType;
			
			if(valueType == adjacentActorsType)
				adjacentActors = new HashSet<>();
		}
		
		public void setValueType(int valueType) {
			this.valueType = valueType;
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
			int i = 0;
			for(int actor : adjacentActors) {
				i++;
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
				for(int actor : adjacentActors) {
					sb.append(':').append(actor);
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
			
			if(value.charAt(0) == '%')
				return;
			
			String[] idStrings = value.toString().split(" ");
			int id1 = Integer.parseInt(idStrings[0]);
			int id2 = Integer.parseInt(idStrings[1]);
			
			if(id1 == TARGET_ID) {
				outKey.set(1, id2);
				outValue.setAdjacentToTarget();
				context.write(outKey, outValue);
				return;
			}
			if(id2 == TARGET_ID) {
				outKey.set(1, id1);
				outValue.setAdjacentToTarget();
				context.write(outKey, outValue);
				return;
			}
			
			outKey.set(3, id1);
			HashSet<Integer> actorSet = new HashSet<>();
			actorSet.add(id2);
			outValue.setActors(actorSet);
			context.write(outKey, outValue);
			
			outKey.set(3, id2);
			actorSet = new HashSet<>();
			actorSet.add(id1);
			outValue.setActors(actorSet);
			context.write(outKey, outValue);
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
			
			return left.getY() - right.getY();
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
				}
			}
			
			outKey.set(id);
			outValue.set(distance);
			context.write(outKey, outValue);
			
		}
	}
	
	public static class DistanceCombiner extends Reducer<IntPairWritable, MapValue, IntPairWritable, MapValue> {
		
		private final IntPairWritable outKey = new IntPairWritable(1,1);
		private final MapValue outValue1 = new MapValue();
		private final MapValue outValue2 = new MapValue(3);
		
		public void reduce(IntPairWritable key, Iterable<MapValue> values, Context context) throws IOException, InterruptedException {
			int id = key.getY();
			int distance = -1;
			Iterator<MapValue> valueIterator = values.iterator();
			
			while(valueIterator.hasNext()) {
				MapValue value = valueIterator.next();
				switch(value.getValueType()) {
				case 1:
					distance = 1;
					break;
				case 2:
					if(distance == -1)
						distance = value.getDistance();
					distance = distance <= value.getDistance() ? distance : value.getDistance();
					break;
				case 3:
					outValue2.addActors(value.getAdjacentActors());
				}
			}
			if(distance == 1) {
				outKey.set(1, id);
				outValue1.setAdjacentToTarget();
				context.write(outKey, outValue1);
			} else if(distance != -1) {
				outKey.set(2, id);
				outValue1.setDistance(distance);
				context.write(outKey, outValue1);
			} else {
				outKey.set(3, id);
				context.write(outKey, outValue2);
			}
			
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
