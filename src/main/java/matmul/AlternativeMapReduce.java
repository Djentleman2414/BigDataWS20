package matmul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPairWritable;

public class AlternativeMapReduce {

	public static class AltMapValue implements WritableComparable<AltMapValue>, Cloneable {

		private boolean valueFromLeftMatrix;
		private int column;
		private int positionInSum;
		private double value;

		public AltMapValue() {
			// Do nothing;
		}

		public AltMapValue(boolean valueFromLeftMatrix, int positionInSum, double value) {
			this(valueFromLeftMatrix, 0, positionInSum, value);
		}

		public AltMapValue(boolean valueFromLeftMatrix, int column, int positionInSum, double value) {
			this.valueFromLeftMatrix = valueFromLeftMatrix;
			this.column = column;
			this.positionInSum = positionInSum;
			this.value = value;
		}
		
		public void set(boolean valueFromLeftMatrix, int column, int positionInSum, double value) {
			this.valueFromLeftMatrix = valueFromLeftMatrix;
			this.column = column;
			this.positionInSum = positionInSum;
			this.value = value;
		}

		public void setValueFromLeftMatrix(boolean valueFromLeftMatrix) {
			this.valueFromLeftMatrix = valueFromLeftMatrix;
		}

		public boolean isValueFromLeftMatrix() {
			return valueFromLeftMatrix;
		}

		public void setColumn(int column) {
			this.column = column;
		}

		public int getColumn() {
			return column;
		}

		public void setPostionInSum(int positionInSum) {
			this.positionInSum = positionInSum;
		}

		public int getPositionInSum() {
			return positionInSum;
		}

		public void setValue(double value) {
			this.value = value;
		}

		public double getValue() {
			return value;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeBoolean(valueFromLeftMatrix);
			if (valueFromLeftMatrix)
				out.writeInt(column);
			out.writeInt(positionInSum);
			out.writeDouble(value);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			valueFromLeftMatrix = in.readBoolean();
			if (valueFromLeftMatrix)
				column = in.readInt();
			positionInSum = in.readInt();
			value = in.readDouble();
		}

		@Override
		public int compareTo(AltMapValue o) {
			if(valueFromLeftMatrix && !o.valueFromLeftMatrix) return -1;
			if(!valueFromLeftMatrix && o.valueFromLeftMatrix) return 1;
			int dist = Integer.compare(column, o.column);
			if(dist != 0) return dist;
			dist = Integer.compare(positionInSum, o.positionInSum);
			if(dist != 0) return dist;
			return Double.compare(value, o.value);
		}
		
		public boolean equals(Object o) {
			if(o == null) return false;
			if(this == o) return true;
			if(this.getClass() != o.getClass()) return false;
			AltMapValue other = (AltMapValue) o;
			
			if(valueFromLeftMatrix != other.valueFromLeftMatrix) return false;
			if(column != other.column) return false;
			if(positionInSum != other.positionInSum) return false;
			if(value != other.value) return false;
			return true;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append(valueFromLeftMatrix ? "Left Matrix; " : "Right Matix; Column:");
			if(!valueFromLeftMatrix) sb.append(column).append("; ");
			sb.append("Position in Sum:").append(positionInSum).append("; ");
			sb.append("Value:").append(value).append("; ");
			
			return sb.toString();
		}

		public AltMapValue clone() {
			return new AltMapValue(valueFromLeftMatrix, column, positionInSum, value);
		}

	}

	public static abstract class MatrixMapper extends Mapper<Object, Text, IntPairWritable, AltMapValue> {

		protected final IntPairWritable outKey = new IntPairWritable();
		protected final AltMapValue outValue = new AltMapValue();

		protected int rowCount;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			rowCount = conf.getInt("ROWCOUNT", 0);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] args = value.toString().split("\t");
			int rowIndex = Integer.parseInt(args[0]);
			int colIndex = Integer.parseInt(args[1]);
			double val = Double.parseDouble(args[2]);
			if (val != 0) {
				outValue.setValue(val);
				writeToContext(rowIndex, colIndex, context);
			}
		}

		protected abstract void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException;
	}

	public static class AltLeftMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex,
				Mapper<Object, Text, IntPairWritable, AltMapValue>.Context context)
				throws IOException, InterruptedException {
			outValue.setValueFromLeftMatrix(true);
			outValue.setPostionInSum(colIndex);
			outKey.set(rowIndex, -1);
			context.write(outKey, outValue);
		}
	}

	public static class AltRightMapper extends MatrixMapper {

		@Override
		protected void writeToContext(int rowIndex, int colIndex,
				Mapper<Object, Text, IntPairWritable, AltMapValue>.Context context)
				throws IOException, InterruptedException {
			outValue.setColumn(colIndex);
			outValue.setPostionInSum(rowIndex);
			outKey.setY(colIndex);
			for (int i = 0; i < rowCount; i++) {
				outKey.setX(i);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class AltMatMulPartitioner extends Partitioner<IntPairWritable, AltMapValue> {

		@Override
		public int getPartition(IntPairWritable key, AltMapValue value, int numPartitions) {
			return key.getX() % numPartitions;
		}
		
	}
	
	public static class AltMatMulGroupingComparator extends WritableComparator {
		
		public AltMatMulGroupingComparator() {
			super(IntPairWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			IntPairWritable left = (IntPairWritable) a;
			IntPairWritable right = (IntPairWritable) a;
			
			return Integer.compare(left.getX(), right.getX());
		}
	}
	
	public static class AltMatrixReducer extends Reducer<IntPairWritable, AltMapValue, IntPairWritable, DoubleWritable> {
		
		public double[] leftMatrixRow;
		private final IntPairWritable outKey = new IntPairWritable();
		private final DoubleWritable outValue = new DoubleWritable();
		
		private AltMapValue value = new AltMapValue();
		private AltMapValue lastValue = new AltMapValue();
		
		public int sharedDimensionSize = 0;
		
		public void setup(Context context) {
			sharedDimensionSize = context.getConfiguration().getInt("SHAREDDIMENSION", 0);
			leftMatrixRow = new double[sharedDimensionSize];
		}
		
		public void reduce(IntPairWritable key, Iterable<AltMapValue> values, Context context) throws IOException, InterruptedException {
			Arrays.fill(leftMatrixRow, 0);
			outKey.setX(key.getX());
			
			Iterator<AltMapValue> valueIt = values.iterator();
			
			while(valueIt.hasNext()) {
				value = valueIt.next();
				lastValue = value;
				if(!value.isValueFromLeftMatrix()) {
					break;
				}
				System.out.println(value.toString());
				leftMatrixRow[value.getPositionInSum()] = value.getValue();
			}
			
			System.out.println(lastValue.toString());
			double sum = leftMatrixRow[lastValue.getPositionInSum()] * lastValue.getValue();
			System.out.println("Column: " + lastValue.getColumn() + " Intermediate-Sum: " + sum);
			
			while(valueIt.hasNext()) {
				value = valueIt.next();
				System.out.println(value.toString());
				if(value.getColumn() != lastValue.getColumn()) {
					outKey.setY(lastValue.getColumn());
					outValue.set(sum);
					//context.write(outKey, outValue);
					System.out.println(outKey.toString() + ", " + outValue.toString());
					System.out.println("Column: " + lastValue.getColumn() + " Sum: " + sum);
					sum = leftMatrixRow[value.getPositionInSum()] * value.getValue();
					lastValue = value;
				} else {
					System.out.println("Column: " + lastValue.getColumn() + " Intermediate-Sum: " + sum);
					sum += leftMatrixRow[value.getPositionInSum()] * value.getValue();
					
					lastValue = value;
				}
			}
			
			System.out.println("Column: " + lastValue.getColumn() + " Sum: " + sum);
			
			outKey.setY(lastValue.getColumn());
			outValue.set(sum);
			//context.write(outKey, outValue);
			System.out.println(outKey.toString() + ", " + outValue.toString());
		}
	}
	
	
}
