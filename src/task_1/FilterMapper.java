package task_1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FilterMapper extends Mapper<LongWritable, Text,NullWritable,Text>{
	
	NullWritable key;
	Text line;
	
	public void setup(Context context){
		line = new Text();
	}
	
	public void map(LongWritable key1, Text value, Context context)
					throws IOException, InterruptedException{
		String lineArray = value.toString();
		line = new Text(lineArray);
		
		if(!lineArray.contains("NA")){
			context.write(key,line);
		}		
	}
}