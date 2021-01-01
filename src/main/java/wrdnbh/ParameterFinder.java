package wrdnbh;

public class ParameterFinder {

	public static void main(String[] args) {
		for(int b = 30; b <= 50; b++) {
			for(int r = 3; r <= 7; r++) {
				double p = getPercentages(0.5, r, b);
				double p_sub = getPercentages(0.4, r, b);
				if(p >= 0.8 && p_sub < 0.6) {
					System.out.println("p: " + p + " p_sub: " + p_sub + " r: " + r + " b: " + b);
				}
					
			}
		}
	}
	
	public static double getPercentages(double s, int r, int b) {
		double match = Math.pow(s, r);
		
		return 1-Math.pow(1-match, b);
	}


}
