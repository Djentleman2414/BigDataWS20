package matmul;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import matmul.FirstPhase.MapKeyClass;
import matmul.FirstPhase.MatrixEntry;
import matmul.FirstPhase.MatrixMapper;
import types.IntPairWritable;

public class AlternativeMapReduce {

	/*
	 * Einträge der linken Matrix werden immer nur an einen Reducer geschickt Dazu
	 * werden die Zeilen in Buckets aufgeteilt
	 */
	public static class LeftMatrixMapper extends MatrixMapper {

		private int rowsPerBucket;

		public void setup(Context context) {
			super.setup(context);
			outValue.setLeft(true);
			outKey.setLeft(true);
			rowsPerBucket = context.getConfiguration().getInt(MatMul.CONF_ROWS_PER_BUCKET, 1);
		}

		@Override
		protected void writeToContext(int rowIndex, int colIndex, Context context)
				throws IOException, InterruptedException {
			outKey.setBucket(rowIndex / rowsPerBucket);
			outKey.setRow(rowIndex);
			outKey.setColumn(colIndex);
			outValue.setCoordinates(rowIndex, colIndex);
			context.write(outKey, outValue);
		}
	}

	/*
	 * Die Einträge der rechten Matrix werden n mal weiter geschickt, wobei n gleich
	 * die Anzahl an Buckets ist
	 */
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

	/*
	 * Im Reducer kommen die Werte so sortiert an, dass zuerst alle Werte der linken
	 * dann alle Werte der rechten Matrix, jeweils aufsteigend sortiert nach Zeile
	 * und Spalte ankommen
	 */
	public static class MatMulReducer extends Reducer<MapKeyClass, MatrixEntry, IntPairWritable, DoubleWritable> {

		IntPairWritable outKey = new IntPairWritable();
		DoubleWritable outValue = new DoubleWritable();

		private MatrixEntry[] leftMatrixEntries;
		private MatrixEntry[] resultMatrixEntries;

		private int rowsPerBucket;
		private int firstRow;
		private int numOfColumnsLeft;
		private int numOfColumnsRight;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			rowsPerBucket = conf.getInt(MatMul.CONF_ROWS_PER_BUCKET, 1);
			numOfColumnsLeft = conf.getInt(MatMul.CONF_NUM_OF_COLUMNS_LEFT, 0);
			numOfColumnsRight = conf.getInt(MatMul.CONF_NUM_OF_COLUMNS_RIGHT, 0);
			leftMatrixEntries = new MatrixEntry[rowsPerBucket * numOfColumnsLeft];
			resultMatrixEntries = new MatrixEntry[rowsPerBucket * numOfColumnsRight];
		}

		public void setup(int rowsPerBucket, int numOfColumnsLeft, int numOfColumnsRight) {
			this.rowsPerBucket = rowsPerBucket;
			this.numOfColumnsLeft = numOfColumnsLeft;
			this.numOfColumnsRight = numOfColumnsRight;
			leftMatrixEntries = new MatrixEntry[rowsPerBucket * numOfColumnsLeft];
			resultMatrixEntries = new MatrixEntry[rowsPerBucket * numOfColumnsRight];
		}

		public void reduce(MapKeyClass key, Iterable<MatrixEntry> values, Context context)
				throws IOException, InterruptedException {

			firstRow = -1;

			for (MatrixEntry value : values) {
				if (firstRow == -1)
					firstRow = value.getRow();
				if (value.isLeft()) {
					int index = getIndexLeft(value.getRow(), value.getColumn());
					if (leftMatrixEntries[index] == null)
						leftMatrixEntries[index] = new MatrixEntry();
					leftMatrixEntries[index].set(value.getRow(), value.getColumn(), value.getValue());
				} else {
					findPairs(value.getRow(), value.getColumn(), value.getValue());
				}
			}

			for (MatrixEntry e : resultMatrixEntries) {
				if (e != null && e.getRow() != -1) {
					outKey.set(e.getRow(), e.getColumn());
					outValue.set(e.getValue());
					context.write(outKey, outValue);
					// da immer wieder das selbe Array benutzt wird, muss gekennzeichnet werden
					// dass dieser Eintrag nicht verwendet wurde (für 0 Einträge relevant)
					e.set(-1, -1, 0);
				}
			}
		}

		/*
		 * In dem Array der linken Matrix stehen die Einträge aufsteigend nach Zeile und
		 * Spalte sortiert. Da immer ganze Zeilen verarbeitet werden hat der Erste
		 * Eintrag die Matrix-Koordinaten (firstRow,0) Ein Eintrag mit den Koordinaten
		 * (firstRow + x,y) steht also x * Länge einer rechten Zeile (=
		 * numOfColumnsLeft) + y vor dem ersten Eintrag
		 */
		private int getIndexLeft(int row, int column) {
			return (row - firstRow) * numOfColumnsLeft + column;
		}

		/*
		 * Gleich der Funktion für die linke Matrix, aber die Länge einer Zeile ist hier
		 * die Anzahl an Spalten der rechen Matrix
		 */
		private int getIndexResult(int row, int column) {
			return (row - firstRow) * numOfColumnsRight + column;
		}

		/*
		 * Jeder Eintrag der rechten Matrix wird mit den zugehörigen Einträgen der
		 * linken Matrix (in leftMatrixEntries) mulitpliziert. Ein Eintrag der rechten
		 * matrix mit den Koordinaten (x,y) muss mit den Einträgen (firstRow,x),
		 * (firstRow + 1, x) usw. multipliziert werde Das Ergebnis wird auf die Einträge
		 * der Ergebnismatrix (firstRow,y), (firstRow+1,y) usw addiert
		 */
		private void findPairs(int rightRow, int rightColumn, double value) {
			int leftIndex = getIndexLeft(firstRow, rightRow);
			for (int i = 0; i < rowsPerBucket; i++) {
				MatrixEntry leftEntry = leftMatrixEntries[leftIndex];
				if (leftEntry != null && leftEntry.getRow() != -1) {
					int resultIndex = getIndexResult(leftEntry.getRow(), rightColumn);
					if (resultMatrixEntries[resultIndex] == null) {
						resultMatrixEntries[resultIndex] = new MatrixEntry(leftEntry.getRow(), rightColumn,
								value * leftEntry.getValue());
					} else if (resultMatrixEntries[resultIndex].getRow() == -1) {
						resultMatrixEntries[resultIndex].set(leftEntry.getRow(), rightColumn,
								value * leftEntry.getValue());
					} else {
						resultMatrixEntries[resultIndex].addValue(value * leftEntry.getValue());
					}
				}
				leftIndex += numOfColumnsLeft;
			}
		}
	}
}
