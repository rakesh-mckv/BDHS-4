package task_2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class UnitCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	IntWritable value;
	Text tvname;
	
	public void setup(Context context){
		value = new IntWritable(1);
		tvname = new Text();
	}
	
	public void map(LongWritable key1, Text company, Context context)
	            throws IOException, InterruptedException{
		String[] lineArray = company.toString().split("\\|");
		
		if(!(lineArray[0].equals("NA") || (lineArray[1].equals("NA")))){
			tvname.set((lineArray[0]));
			context.write(tvname,value);
		}
	}
}
