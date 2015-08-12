import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Aggregate { 
	//	mapper
	public static class AggregateMap extends Mapper<LongWritable, Text, Text, AggregateWritable>{
		
		String[] stopArr = {"i","the","to","and","a","an","of","it","you","that","in","my","is","was","for"};
		private HashSet<String> swSet = new HashSet<String>(Arrays.asList(stopArr));

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] arr = value.toString().split("\t");
			String word = arr[0];
			// remove stop words
			String[] wordsArr = word.split(" ");
			for(String s:wordsArr){
				if(swSet.contains(s)) return;
			}
			
			AggregateWritable outw = new AggregateWritable();
			int year = Integer.parseInt(arr[1].toString());
			long wordCnt = Long.parseLong(arr[2].toString());
			Boolean isUnigram = (wordsArr.length == 1)? true:false;
			Boolean isBg = (year >= 1970) ? true:false;
			
			if(isUnigram){
				if(isBg) outw.setBx(wordCnt);
				else outw.setCx(wordCnt);
			}
			else{
				if(isBg) outw.setBxy(wordCnt);
				else outw.setCxy(wordCnt);
			}
			
			context.write(new Text(word), outw);
		}
	}
		
	// reduce
	public static class AggregateReduce extends Reducer<Text, AggregateWritable, Text, AggregateWritable>{
		
		public void reduce(Text key, Iterable<AggregateWritable> values, Context context) throws IOException, InterruptedException{
			AggregateWritable outw = new AggregateWritable();
			for( AggregateWritable val: values){
				outw.plus(val);
			}
			context.write(key,outw);
		}
	}
		
}
