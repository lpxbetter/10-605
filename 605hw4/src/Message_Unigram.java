import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Message_Unigram {
	//Mapper
	public static class MuMap extends Mapper<Text, AggregateWritable, Text, MessageWritable>{
		//map
		public void map(Text key, AggregateWritable value, Context context) throws IOException, InterruptedException{
				String[] arr = key.toString().split(" ");
				Boolean isBigram = (arr.length > 1)?true:false;
				
				if(isBigram){ //split to x and y to generate message
					MessageWritable outw = new MessageWritable();
					outw.setMessage(key.toString());
					context.write(new Text(arr[0]), outw);
					
					outw = new MessageWritable();
					outw.setMessage(key.toString());
					context.write(new Text(arr[1]), outw);
				}
				else{
					MessageWritable outw = new MessageWritable();
					outw.setMessage("");
					outw.setBx(value.getBx());
					outw.setCx(value.getCx());
					context.write(key, outw);
				}
		}
	}
	//Reducer
	public static class MuReduce extends Reducer<Text, MessageWritable, Text, AggregateWritable>{
		public void reduce(Text key, Iterable<MessageWritable> values, Context context) throws IOException, InterruptedException{
			AggregateWritable aggwXZ = new AggregateWritable(); // for xz
			AggregateWritable aggwZX = new AggregateWritable(); // for zx
			ArrayList<String> bigramsOfKey = new ArrayList<String>();
			
			for(MessageWritable val:values){
				if(!val.getMessage().equals("")){
					bigramsOfKey.add(val.getMessage()); // all bigrams that contains key,which was splited in mapper
				}
				else{
					aggwXZ.setBx(val.getBx());
					aggwXZ.setCx(val.getCx());
					aggwZX.setBy(val.getBx());
					aggwZX.setCy(val.getCx());
				}
			}
			for(String s : bigramsOfKey){
				String[] arr = s.split(" ");
				if(key.toString().trim().equals(arr[0]) ) context.write(new Text(s), aggwXZ);  // xz
				if(key.toString().trim().equals(arr[1]) ) context.write(new Text(s), aggwZX);  // xz
			}
		}
	} 
	
	//Step2, mapper
	public static class MuMap2 extends Mapper<Text, AggregateWritable, Text, AggregateWritable>{
		public void map(Text key, AggregateWritable value, Context context) throws IOException, InterruptedException{
			String[] arr = key.toString().split(" ");
			Boolean isBigram = (arr.length > 1)?true:false;
			if(!isBigram) return; 
			else context.write(key, value); //just send to reducer
		}
	}
	//Step2 reducer
	public static class MuReduce2 extends Reducer<Text, AggregateWritable, Text, AggregateWritable>{
		public void reduce(Text key, Iterable<AggregateWritable> values, Context context) throws IOException, InterruptedException{
			AggregateWritable outw = new AggregateWritable(); //for output
			for(AggregateWritable val : values){  // merge aggregate result and map output from previous map step
				if(val.getBx() > 0) outw.setBx(val.getBx());
				if(val.getCx() > 0) outw.setCx(val.getCx());
				if(val.getBxy() > 0) outw.setBxy(val.getBxy());
				if(val.getCxy() > 0) outw.setCxy(val.getCxy());
				if(val.getBy() > 0) outw.setBy(val.getBy());
				if(val.getCy() > 0) outw.setCy(val.getCy());
			}
			context.write(key,outw); // all counts are merged to outw, will be provided to Compute.java
		}
	}
	
}

