package matmul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPairWritable;

public class FirstPhase {

	public static class MatrixEntry implements Writable, Cloneable {

		private int row;
		private int column;
		private double value;
		private boolean left;

		public MatrixEntry() {
		}

		public MatrixEntry(int row, int column, double value) {
			set(row, column, value, false);
		}

		public MatrixEntry(int row, int column, double value, boolean left) {
			set(row, column, value, left);
		}

		public void set(int row, int column, double value) {
			this.row = row;
			this.column = column;
			this.value = value;
		}

		public void set(int row, int column, double value, boolean left) {
			this.row = row;
			this.column = column;
			this.value = value;
			this.left = left;
		}

		public void setCoordinates(int row, int column) {
			this.row = row;
			this.column = column;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public void setRow(int row) {
			this.row = row;
		}

		public int getRow() {
			return row;
		}

		public void setColumn(int column) {
			this.column = column;
		}

		public int getColumn() {
			return column;
		}

		public void setValue(double value) {
			this.value = value;
		}

		public double getValue() {
			return value;
		}

		public boolean isLeft() {
			return left;
		}

		public void setLeft(boolean left) {
			this.left = left;
		}

		public void addValue(double add) {
			value += add;
		}

		public void addValue(MatrixEntry other) {
			if (other == null || row != other.row || column != other.column)
				throw new IllegalArgumentException("Coordinates must be the same");
			value += other.value;
		}

		public int compareTo(MatrixEntry other) {
			if (other == null)
				return -1;
			int dist = Integer.compare(row, other.row);
			if (dist != 0)
				return dist;
			return Integer.compare(column, other.column);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(row);
			out.writeInt(column);
			out.writeDouble(value);
			out.writeBoolean(left);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			row = in.readInt();
			column = in.readInt();
			value = in.readDouble();
			left = in.readBoolean();
		}

		public MatrixEntry copy() {
			return new MatrixEntry(row, column, value, left);
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			sb.append("Row:" + row + " Column:" + column + " Value:" + value);

			return sb.toString();
		}
	}

	public static class MapKeyClass implements WritableComparable<MapKeyClass>, Cloneable {

		private int bucket;
		private boolean left;
		private int row;
		private int column;

		public MapKeyClass() {
			// do nothing
		}

		public MapKeyClass(int bucket, boolean left, int row, int column) {
			this.bucket = bucket;
			this.left = left;
			this.row = row;
			this.column = column;
		}

		public void set(int bucket, int row, int column) {
			this.bucket = bucket;
			this.row = row;
			this.column = column;
		}

		public int getBucket() {
			return bucket;
		}

		public void setBucket(int bucket) {
			this.bucket = bucket;
		}

		public boolean isLeft() {
			return left;
		}

		public void setLeft(boolean left) {
			this.left = left;
		}

		public int getRow() {
			return row;
		}

		public void setRow(int row) {
			this.row = row;
		}

		public int getColumn() {
			return column;
		}

		public void setColumn(int column) {
			this.column = column;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(bucket);
			out.writeBoolean(left);
			out.writeInt(row);
			out.writeInt(column);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			bucket = in.readInt();
			left = in.readBoolean();
			row = in.readInt();
			column = in.readInt();
		}

		@Override
		public int compareTo(MapKeyClass other) {
			int dist = Integer.compare(bucket, other.bucket);
			if (dist != 0)
				return dist;
			dist = Boolean.compare(left, other.left);
			if (dist != 0)
				return dist;
			dist = Integer.compare(row, other.row);
			if (dist != 0)
				return dist;
			return Integer.compare(column, other.column);
		}

		public MapKeyClass clone() {
			return new MapKeyClass(bucket, left, row, column);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + bucket;
			result = prime * result + column;
			result = prime * result + (left ? 1231 : 1237);
			result = prime * result + row;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MapKeyClass other = (MapKeyClass) obj;
			if (bucket != other.bucket)
				return false;
			if (column != other.column)
				return false;
			if (left != other.left)
				return false;
			if (row != other.row)
				return false;
			return true;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			sb.append("Bucket:" + bucket + " Row:" + row + " Column:" + column);

			return sb.toString();
		}
	}

	public static abstract class MatrixMapper extends Mapper<Object, Text, MapKeyClass, MatrixEntry> {

		MapKeyClass outKey = new MapKeyClass();
		MatrixEntry outValue = new MatrixEntry();

		protected int maxBucketSize;
		protected int numOfBuckets;

		protected int numOfColumns;

		// wo werden die benutzt?
		protected int wroteToContextLeft;
		protected int wroteToContextRight;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			maxBucketSize = conf.getInt(MatMul.CONF_MAX_BUCKET_SIZE, 1);
			numOfBuckets = conf.getInt(MatMul.CONF_NUM_OF_BUCKETS, 1);
			numOfColumns = conf.getInt(MatMul.CONF_NUM_OF_COLUMNS_LEFT, 0);
			// System.out.println("ABCD setup: " + maxBucketSize + " " + numOfBuckets + " "
			// + numOfColumns);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] args = value.toString().split("\t");
			int rowIndex = Integer.parseInt(args[0]);
			int colIndex = Integer.parseInt(args[1]);
			double val = Double.parseDouble(args[2]);
			// val kann ja eigentlich nie 0 sein an der Stelle
			if (val != 0) {
				outValue.setValue(val);
				writeToContext(rowIndex, colIndex, context);
			}
		}

		protected abstract void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException;
	}

	public static class LeftMatrixMapper extends MatrixMapper {

		public void setup(Context context) {
			super.setup(context);
			outValue.setLeft(true);
		}

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException {
			outKey.setBucket((rowIndex * numOfColumns + colIndex) / maxBucketSize);
			outKey.setLeft(true);
			outKey.setRow(rowIndex);
			outKey.setColumn(colIndex);
			outValue.setCoordinates(rowIndex, colIndex);
			context.write(outKey, outValue);
		}
	}

	public static class RightMatrixMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException {
			outKey.setRow(rowIndex);
			outKey.setColumn(colIndex);
			outValue.setCoordinates(rowIndex, colIndex);
			for (int i = 0; i < numOfBuckets; i++) {
				outKey.setBucket(i);
				context.write(outKey, outValue);
			}
		}
	}

	public static class MatrixPartitioner extends Partitioner<MapKeyClass, MatrixEntry> {

		@Override
		public int getPartition(MapKeyClass key, MatrixEntry value, int numPartitions) {
			return key.getBucket() % numPartitions;
		}
	}

	public static class MatrixGroupingComparator extends WritableComparator {

		public MatrixGroupingComparator() {
			super(MapKeyClass.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			MapKeyClass first = (MapKeyClass) a;
			MapKeyClass second = (MapKeyClass) b;
			return Integer.compare(first.getBucket(), second.getBucket());
		}
	}

	public static class MatrixSortingComparator extends WritableComparator {

		public MatrixSortingComparator() {
			super(MapKeyClass.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			MapKeyClass first = (MapKeyClass) a;
			MapKeyClass second = (MapKeyClass) b;
			int dist = Integer.compare(first.getBucket(), second.getBucket());
			if (dist != 0)
				return dist;
			dist = -Boolean.compare(first.isLeft(), second.isLeft());
			if (dist != 0)
				return dist;
			dist = Integer.compare(first.getRow(), second.getRow());
			if (dist != 0)
				return dist;
			return Integer.compare(first.getColumn(), second.getColumn());
		}
	}

	/**
	 * Änderungen ggü. vorher:
	 * 
	 * Es wird auf ein Target verzichtet, da es auch kein BinarySearch mehr gibt.
	 * "picking Search" ist eine ausgedachte Funktion mit der der erste Eintrag der
	 * gesuchten Spalte im Bucket gefunden werden kann. Eigentlich wird nicht
	 * gesucht, sondern berechnet (O(1)). Es wird auf diverse Grenzwertfälle
	 * geschaut (alle die mir aufgefallen sind). Damit sind wir in der Lage
	 * unbegrenzt große Matrizen aufzunehmen bei endlich großen Buckets.
	 * 
	 * Nachdem der richtige erste Index gefunden wurde, wird für die nächsten
	 * Kandidaten einfach + NUM_OF_COLUMNS gerechnet. Viel mehr passiert auch nicht.
	 * 
	 *
	 */
	public static class MatMulReducer extends Reducer<MapKeyClass, MatrixEntry, IntPairWritable, DoubleWritable> {

		IntPairWritable outKey = new IntPairWritable();
		DoubleWritable outValue = new DoubleWritable();

		private MatrixEntry[] leftMatrixEntries;

		private int firstRow;
		private int firstColumn;
		private int numOfColumns;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			int maxBucketSize = conf.getInt(MatMul.CONF_MAX_BUCKET_SIZE, 0);
			leftMatrixEntries = new MatrixEntry[maxBucketSize];
			numOfColumns = conf.getInt(MatMul.CONF_NUM_OF_COLUMNS_LEFT, 0);
		}

		public void reduce(MapKeyClass key, Iterable<MatrixEntry> values, Context context)
				throws IOException, InterruptedException {

			for (MatrixEntry value : values) {
				firstRow = value.getRow();
				firstColumn = value.getColumn();
				if (leftMatrixEntries[0] == null)
					leftMatrixEntries[0] = new MatrixEntry();
				leftMatrixEntries[0].set(value.getRow(), value.getColumn(), value.getValue());
				break;
			}

			for (MatrixEntry value : values) {
				if (!value.isLeft()) {
					findPairs(value.getRow(), value.getColumn(), value.getValue(), context);
					break;
				}
				int index = getIndex(value.getRow(), value.getColumn());
				if (leftMatrixEntries[index] == null)
					leftMatrixEntries[index] = new MatrixEntry();
				leftMatrixEntries[index].set(value.getRow(), value.getColumn(), value.getValue());
			}

			for (MatrixEntry value : values) {
				findPairs(value.getRow(), value.getColumn(), value.getValue(), context);
			}

		}

		private int getIndex(int row, int column) {
			return (row - firstRow) * numOfColumns + column - firstColumn;
		}

		private void findPairs(int rightRow, int rightColumn, double value, Context context)
				throws IOException, InterruptedException {		
			int index = getIndex(firstColumn <= rightRow ? firstRow : firstRow + 1, rightRow);		
			while(index < leftMatrixEntries.length) {
				MatrixEntry leftEntry = leftMatrixEntries[index];
				if(leftEntry != null) {
					outKey.set(leftEntry.getRow(), rightColumn);
					outValue.set(leftEntry.getValue() * value);
					context.write(outKey, outValue);
				}
				index += numOfColumns;
			}
		}
	}
}