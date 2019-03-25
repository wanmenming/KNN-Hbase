/*
 * Self-defined Class Pair to store distance and delay label for train data
 * */
package cs62240.knn_hbase;

public class Pair implements Comparable<Pair>{
	// Distance from one train data to one test data
	private long dist;
	// Delay label of this test data
	private boolean delayed;
	
	public Pair(long dist, boolean delayed) {
		this.dist = dist;
		this.delayed = delayed;
	}
	
	public long getDist() {
		return dist;
	}
	public void setDist(long dist) {
		this.dist = dist;
	}
	public boolean isDelayed() {
		return delayed;
	}
	public void setDelayed(boolean delayed) {
		this.delayed = delayed;
	}

	@Override
	// Self-defined comparator for class Pair
	public int compareTo(Pair o) {
		if (this.getDist() < o.getDist()) {
			return 1;
		}
		
		if (this.getDist() == o.getDist()) {
			return 0;
		}
		
		return -1;
	}
	
}
