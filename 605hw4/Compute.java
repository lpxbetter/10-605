import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class Compute {
	
	//------------------------------------------------------------------------------------------
	//	MAPPER
	//------------------------------------------------------------------------------------------
		
	public static class ComputeMap extends MapReduceBase implements Mapper<Text, CountDataWritable, Text, CountDataWritable>{

		public void map(Text key, CountDataWritable counts,
				OutputCollector<Text, CountDataWritable> outputList,
				Reporter reporter) throws IOException {
			
			outputList.collect(key, counts);
			
		}
	}//end of mapper
	
	//-------------------------------------------------------------------------------------------------------------
	// REDUCER - compute scores
	//-------------------------------------------------------------------------------------------------------------
		
	public static class ComputeReduce extends MapReduceBase implements Reducer<Text, CountDataWritable, Text, Text>{
		
		long unigramVocab;
		long bigramVocab;
		long BxWordCount;
		long CxWordCount;
		long BxyWordCount;
		long CxyWordCount;
		
		public void configure(JobConf job){
			this.unigramVocab = job.getLong("unigramVocab", 0);
			this.bigramVocab = job.getLong("bigramVocab", 0);
			this.BxWordCount = job.getLong("BxWordCount", 0);
			this.CxWordCount = job.getLong("CxWordCount", 0);
			this.BxyWordCount = job.getLong("BxyWordCount", 0);
			this.CxyWordCount = job.getLong("CxyWordCount", 0);
		}
		
		
		public void reduce(Text key, Iterator<CountDataWritable> counts,
				OutputCollector<Text, Text> outputList, Reporter reporter)
				throws IOException {
			
			CountDataWritable cdw;
			
			double p=0D, q=0D;
			double phraseness=0D, informativeness=0D;
			double score = 0D;
			String result = "";
			
			while(counts.hasNext()){
				
				cdw = counts.next();
				
				//Phraseness
				p = ( (double)cdw.getCxy() + 1D ) / (double)CxyWordCount;
				q = ( ( (double)cdw.getCx() + 1D ) / (double)CxWordCount ) * ( ( (double)cdw.getCy() + 1D ) / (double)CxWordCount );
				phraseness = p * Math.log( p / q );
				
				//Informativeness
				p = ( (double)cdw.getCxy() + 1D ) / (double)CxyWordCount;
				q = ( (double)cdw.getBxy() + 1D ) / (double)BxyWordCount;
				informativeness = p * Math.log( p / q );
				
				//final score
				score = informativeness + phraseness;
				
				result = String.valueOf(score) + "\t" + String.valueOf(phraseness) + "\t" + String.valueOf(informativeness); 
				
				outputList.collect(key, new Text(result));
				
			}
			
		}

	}//end of reducer

}