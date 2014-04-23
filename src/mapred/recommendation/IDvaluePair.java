/*
 * 18645 Term Project
 * 
 * Recommendation System Based on Hadoop MapReduce
 * 
 * Fangxiaoyu Feng, Andi Ni, Yichao Xue
 */

package mapred.recommendation;

/**
 * 
 * @author Fangxiaoyu Feng
 * 
 */

public class IDvaluePair {
	public String ID;
	public int value;
	public IDvaluePair(String ID, int value){
		this.ID = ID;
		this.value = value;
	}
	
	
}