/*
 * 18645 Term Project
 * 
 * Recommendation System Based on Hadoop MapReduce
 * 
 * Fangxiaoyu Feng, Andi Ni, Yichao Xue
 */

package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Fangxiaoyu Feng
 * 
 */

public class RecVectorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] keyvalues = line.split("\t");

		if (keyvalues.length < 2) return;

		String combinedvalues = keyvalues[1];
		String[] valuesArray = combinedvalues.split("#");
		String uservecString = valuesArray[0];
		String itemvecString = valuesArray[1];
		String[] uservecArray = uservecString.split(":");
		String[] itemvecArray = itemvecString.split(":");
		
		for(int i = 0; i < uservecArray.length; i+=2){
			int userValue = Integer.parseInt(uservecArray[i+1]);
			String partialProduct = getMultiResult(itemvecArray, userValue);
			context.write(new Text(uservecArray[i]), new Text(partialProduct));
		}
			
	}
	
	public String getMultiResult(String[] itemvecArray, int userValue){
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < itemvecArray.length; i+=2){
			int itemvalue = Integer.parseInt(itemvecArray[i+1]);
			builder.append(itemvecArray[i]).append(":").append(itemvalue * userValue).append(":");
		}
		return builder.toString();
	}
}
