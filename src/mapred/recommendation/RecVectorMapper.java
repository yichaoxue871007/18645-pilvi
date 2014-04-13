/* author: Fangxiaoyu Feng (fangxiaf) */

package mapred.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecVectorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] keyvalues = line.split("\t");

		String combinedvalues = keyvalues[1];
		String[] valuesArray = combinedvalues.split("#");
		String uservecString = valuesArray[0];
		String itemvecString = valuesArray[1];
		String[] uservecArray = uservecString.split(";");
		String[] itemvecArray = itemvecString.split(";");

		IDvaluePair[] itemvec = new IDvaluePair[itemvecArray.length];

		for(int i = 0; i < itemvecArray.length; i++){
			String[] temp = itemvecArray[i].split(":");
			int tempv = Integer.parseInt(temp[1]);
			IDvaluePair newitem = new IDvaluePair(temp[0] , tempv);
			itemvec[i] = newitem;
		}
		
		for(int i = 0; i < uservecArray.length; i++){
			String[] temp = uservecArray[i].split(":");
			int tempv = Integer.parseInt(temp[1]);
			String partialProduct = getMultiResult(itemvec, tempv);
			context.write(new Text(temp[0]), new Text(partialProduct));
		}
			
	}
	
	public String getMultiResult(IDvaluePair[] itemvec, int userValue){
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < itemvec.length; i++){
			builder.append(itemvec[i].ID).append(":").append(itemvec[i].value*userValue).append(";");
		}
		return builder.toString();		
	}
}
