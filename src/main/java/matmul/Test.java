package matmul;

public class Test {

	public static void main(String[] args) {
		MatMul matmul = new MatMul();
		
		matmul.getMatrixDimensions("SA-120-100.txt", "SB-100-80.txt");
		
		System.out.println("Rows: " + MatMul.leftRowCount + " Cols: " + MatMul.rightColumnCount);

	}

}
