import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Message_Unigram {

	//------------------------------------------------------------------------------------------
	//	MAPPER 1 - generate unigram messages
	//------------------------------------------------------------------------------------------
		
	public static class MessageUnigramPartOneMap extends MapReduceBase implements Mapper<Text, CountDataWritable, Text, MessageAndCountsWritable>{

		@Override
		public void map(Text key, CountDataWritable counts,
				OutputCollector<Text, MessageAndCountsWritable> outputList,
				Reporter reporter) throws IOException {
			
			String[] keySplit = key.toString().split(" ");
			Boolean isUnigram = keySplit.length == 1;
			
			
			
			//just pass counts forward
			if(isUnigram){
				MessageAndCountsWritable macw = new MessageAndCountsWritable();
				macw.setMessage("");
				macw.setBx(counts.getBx());
				macw.setCx(counts.getCx());
				
				outputList.collect(key, macw);
			}
			//pass 2 messages
			else{
				String x = keySplit[0];
				String y = keySplit[1];
				MessageAndCountsWritable macw = new MessageAndCountsWritable();
				macw.setMessage(key.toString());
				outputList.collect(new Text(x), macw);
				macw = new MessageAndCountsWritable();
				macw.setMessage(key.toString());
				outputList.collect(new Text(y), macw);
			}
		}
	}//end of mapper 1
		
		
	//-------------------------------------------------------------------------------------------------------------
	// REDUCER 1 - Aggregate messages and unigram counts
	//-------------------------------------------------------------------------------------------------------------
		
	public static class MessageUnigramPartOneReduce extends MapReduceBase implements Reducer<Text, MessageAndCountsWritable, Text, CountDataWritable>{

		public void reduce(Text key, Iterator<MessageAndCountsWritable> counts,
				OutputCollector<Text, CountDataWritable> outputList, Reporter reporter)
				throws IOException {
				
			ArrayList<String> bigrams = new ArrayList<String>();  
			CountDataWritable cdwX = new CountDataWritable();
			CountDataWritable cdwY = new CountDataWritable();
			
			MessageAndCountsWritable currentObj;
			
			while(counts.hasNext()){
				currentObj = counts.next();
				if(currentObj.getMessage().equals("")){
					cdwX.setBx(currentObj.getBx());
					cdwX.setCx(currentObj.getCx());
					cdwY.setBy(currentObj.getBx());
					cdwY.setCy(currentObj.getCx());
				}
				else{
					bigrams.add(currentObj.getMessage());
				}
			}
			
			for(String s : bigrams){
				String[] s_split = s.split(" ");
				if(s_split[0].trim().equals(key.toString().trim())){
					//System.out.println(s+"-"+cdwX.toString());
					outputList.collect(new Text(s), cdwX);
				}
				if(s_split[1].trim().equals(key.toString().trim())){
					//System.out.println(s+"-"+cdwY.toString());
					outputList.collect(new Text(s), cdwY);
				}
			}
		}

	}//end of reducer 1
	
	
	
	//------------------------------------------------------------------------------------------
	//	MAPPER 2 
	//------------------------------------------------------------------------------------------
		
	public static class MessageUnigramPartTwoMap extends MapReduceBase implements Mapper<Text, CountDataWritable, Text, CountDataWritable>{

		public void map(Text key, CountDataWritable counts,
				OutputCollector<Text, CountDataWritable> outputList, Reporter reporter)
				throws IOException {
			
			String[] keySplit = key.toString().split(" ");
			Boolean isUnigram = keySplit.length == 1;
			
			//ignore unigrams
			if(isUnigram){
				return;
			}
			//pass bigrams forward
			else{
				outputList.collect(key, counts);
			}
		}
		
	}//end mapper 2

		
	//-------------------------------------------------------------------------------------------------------------
	// REDUCER 2 - Aggregate counts
	//-------------------------------------------------------------------------------------------------------------
		
	public static class MessageUnigramPartTwoReduce extends MapReduceBase implements Reducer<Text, CountDataWritable, Text, CountDataWritable>{

		public void reduce(Text key, Iterator<CountDataWritable> counts,
				OutputCollector<Text, CountDataWritable> outputList, Reporter reporter)
				throws IOException {
				
			
			 
			CountDataWritable cdw = new CountDataWritable();
			
			while(counts.hasNext()){
				CountDataWritable c = counts.next();
				if(c.getBx() > 0){
					cdw.setBx(c.getBx());
				}
				if(c.getCx()>0){
					cdw.setCx(c.getCx());
				}
				if(c.getBy()>0){
					cdw.setBy(c.getBy());
				}
				if(c.getCy()>0){
					cdw.setCy(c.getCy());
				}
				if(c.getBxy()>0){
					cdw.setBxy(c.getBxy());
				}
				if(c.getCxy()>0){
					cdw.setCxy(c.getCxy());
				}
				
			}
			
			System.out.println(key.toString()+"-"+cdw.toString());
			outputList.collect(key, cdw);
		}

	}//end of reducer 2
		
		
		
		
}//end of class