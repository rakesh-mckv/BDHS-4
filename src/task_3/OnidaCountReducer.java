package task_3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnidaCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text state, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException{
		
		int sum = 0;
		for(IntWritable value:values){
			sum+= value.get();
		}
		context.write(state,new IntWritable(sum));
		
	}
}