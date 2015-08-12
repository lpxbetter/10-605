import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Count_Size {
	
	//------------------------------------------------------------------------------------------
	//	MAPPER
	//------------------------------------------------------------------------------------------
	
	public static class CountSizeMap extends MapReduceBase implements Mapper<Text, CountDataWritable, Text, LongWritable>{

		private static final LongWritable one = new LongWritable(1);
		
		public void map(Text key, CountDataWritable value,
				OutputCollector<Text, LongWritable> outputList, Reporter reporter)
				throws IOException {
			
			
			Boolean isUnigram = key.toString().split(" ").length == 1;
			
			//unigram vocab 
			if(isUnigram){
				outputList.collect(new Text("unigramVocab"), one);
			}
			//bigram vocab 
			else{
				outputList.collect(new Text("bigramVocab"), one);
			}
			
			//word count for Bx
			if(value.getBx() > 0){
				outputList.collect(new Text("BxWordCount"), new LongWritable(value.getBx()));
				
			}
			//word count for Cx
			if(value.getCx() > 0){
				outputList.collect(new Text("CxWordCount"), new LongWritable(value.getCx()));
			}
			//word count for Bxy
			if(value.getBxy() > 0){
				outputList.collect(new Text("BxyWordCount"), new LongWritable(value.getBxy()));
			}
			//word count for Cxy
			if(value.getCxy() > 0){
				outputList.collect(new Text("CxyWordCount"), new LongWritable(value.getCxy()));
			}
				
		}
	}
	
	//-------------------------------------------------------------------------------------------------------------
	// REDUCER
	//-------------------------------------------------------------------------------------------------------------
		
	public static class CountSizeReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{

		public void reduce(Text key, Iterator<LongWritable> counts,
				OutputCollector<Text, LongWritable> outputList, Reporter reporter)
				throws IOException {
			
			long totalCounts = 0L;
			
			while(counts.hasNext()){
				totalCounts += counts.next().get();
			}
			
			//System.out.println(key+" - "+ new Long(totalCounts).toString());
			outputList.collect(key, new LongWritable(totalCounts));
		}
	}

}