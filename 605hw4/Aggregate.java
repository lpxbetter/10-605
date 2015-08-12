import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Aggregate { 
	
	//------------------------------------------------------------------------------------------
	//	MAPPER
	//------------------------------------------------------------------------------------------
	
	public static class AggregateMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, CountDataWritable>{
		
		String[] sw = {"i","the","to","and","a","an","of","it","you","that","in","my","is","was","for"};
		private HashSet<String> stopWords = new HashSet<String>(Arrays.asList(sw));

		@Override
		public void map(LongWritable rowNum, Text rowContent,
				OutputCollector<Text, CountDataWritable> outputList, Reporter reporter)
				throws IOException {
			
				String[] rowParts = rowContent.toString().split("\t");
				String nGram = rowParts[0];
				int year = Integer.parseInt(rowParts[1].toString());
				long nGramCount = Long.parseLong(rowParts[2].toString()); 
				String[] nGramParts = nGram.split(" ");
				
				//ignore all n-grams with at least one stop word
				for(String s : nGramParts){
					if(stopWords.contains(s))
						return;
				}
				
				Boolean isBigram = nGramParts.length > 1;
				Boolean isBackground = year >= 1970;
				
				CountDataWritable cdw = new CountDataWritable();
				
				if(isBigram){
					if(isBackground){ //Bxy
						cdw.setBxy(nGramCount);
					}
					else{ //Cxy
						cdw.setCxy(nGramCount);
					}
				}
				else{
					if(isBackground){ //Bx
						cdw.setBx(nGramCount);
					}
					else{ //Cx
						cdw.setCx(nGramCount);
					}
				}
				
				Text outputKey = new Text(nGram);
				
				outputList.collect(outputKey, cdw);	
		}
	}
		
		
	//-------------------------------------------------------------------------------------------------------------
	// REDUCER
	//-------------------------------------------------------------------------------------------------------------
		
	public static class AggregateReduce extends MapReduceBase implements Reducer<Text, CountDataWritable, Text, CountDataWritable>{

		public void reduce(Text key, Iterator<CountDataWritable> counts,
				OutputCollector<Text, CountDataWritable> outputList, Reporter reporter)
				throws IOException {
				
			CountDataWritable aggregatedCounts = new CountDataWritable();
			
			while(counts.hasNext()){
				aggregatedCounts.addTo(counts.next());
			}
			
			//System.out.println(key+" - "+aggregatedCounts.toString());
			outputList.collect(key, aggregatedCounts);
		}
	}
		
}