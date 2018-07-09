package task_3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class OnidaCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] line = value.toString().split("\\|");
		if(line[0].equals("Onida")){
			Text state = new Text(line[3]);
			IntWritable unit = new IntWritable(1);
			context.write(state, unit);
		}
	}
}
