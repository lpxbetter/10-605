import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class CountSize {
	public static class CountSizeMap extends Mapper<Text, AggregateWritable,Text, LongWritable>{

		private static final LongWritable one = new LongWritable(1);
		//map
		public void map(Text key, AggregateWritable value, Context context) throws IOException, InterruptedException{
			
			Boolean isUnigram = (key.toString().split(" ").length == 1 )? true:false;
			
			if(value.getBxy() > 0){
				context.write(new Text("bi_bg_wordCnt"), new LongWritable(value.getBxy()));
			}
			if(value.getCxy() > 0){
				context.write(new Text("bi_fg_wordCnt"), new LongWritable(value.getCxy()));
			}
			if(value.getBx() > 0){
				context.write(new Text("uni_bg_wordCnt"), new LongWritable(value.getBx()));
			}
			if(value.getCx() > 0){
				context.write(new Text("uni_fg_wordCnt"), new LongWritable(value.getCx()));
			}
			
			if(isUnigram){
				if(value.getBx() > 0) context.write(new Text("uni_bg_voc"), one);
				if(value.getCx() > 0) context.write(new Text("uni_fg_voc"), one);
			}
			else{
				if(value.getBxy() > 0) context.write(new Text("bi_bg_voc"), one);
				if(value.getCxy() > 0) context.write(new Text("bi_fg_voc"), one);
			}
		}
	}
	
	// reduce
	public static class CountSizeReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long sum = 0L;
			for(LongWritable val: values){
				sum += val.get();
			}
			context.write(key,new LongWritable(sum));
		}
	}

}