package matmul;

import java.io.IOException;
import java.util.ArrayList;

import matmul.AlternativeMapReduce.MatMulReducer;
import matmul.FirstPhase.MatrixEntry;

public class ManuelleTests {

	public static void main(String[] args) throws IOException, InterruptedException {
		ArrayList<MatrixEntry> al = new ArrayList<>();
		al.add(new MatrixEntry(0,0,1));
		al.add(new MatrixEntry(0,1,2));
		al.add(new MatrixEntry(0,2,3));
		al.add(new MatrixEntry(1,0,4));
		al.add(new MatrixEntry(1,1,5));
		al.add(new MatrixEntry(1,2,6));
		al.add(new MatrixEntry(2,0,7));
		al.add(new MatrixEntry(2,1,8));
		al.add(new MatrixEntry(2,2,9));
		for(MatrixEntry e : al)
			e.setLeft(true);
		
		al.add(new MatrixEntry(0,0,10));
		al.add(new MatrixEntry(0,1,20));
		al.add(new MatrixEntry(0,2,30));
		al.add(new MatrixEntry(0,3,40));
		al.add(new MatrixEntry(1,0,10));
		al.add(new MatrixEntry(1,1,20));
		al.add(new MatrixEntry(1,2,30));
		al.add(new MatrixEntry(1,3,40));
		al.add(new MatrixEntry(2,0,10));
		al.add(new MatrixEntry(2,1,20));
		al.add(new MatrixEntry(2,2,30));
		al.add(new MatrixEntry(2,3,40));
		
		MatMulReducer reducer = new MatMulReducer();
		reducer.setup(3,3,4);
		reducer.reduce(null, al, null);
	}

}
