import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Compute {
	//Mapper
	public static class computeMap extends Mapper<Text, AggregateWritable, Text, AggregateWritable>{
		@Override
		public void map(Text key, AggregateWritable value, Context context) throws IOException, InterruptedException{
				context.write(key, value);
		}
	}
	public static class computeReduce extends Reducer<Text,AggregateWritable, Text,Text>{
		long uni_bg_voc;
		long uni_fg_voc;
		long bi_bg_voc;
		long bi_fg_voc;
		long uni_bg_wordCnt;
		long uni_fg_wordCnt;
		long bi_bg_wordCnt;
		long bi_fg_wordCnt;
		
		@Override
		protected void setup(Context context){
			Configuration conf = context.getConfiguration();
			this.uni_bg_voc = Long.parseLong(conf.get("uni_bg_voc"));
			this.uni_fg_voc = Long.parseLong(conf.get("uni_fg_voc"));
			this.bi_bg_voc = Long.parseLong(conf.get("bi_bg_voc"));
			this.bi_fg_voc = Long.parseLong(conf.get("bi_fg_voc"));
			this.uni_bg_wordCnt = Long.parseLong(conf.get("uni_bg_wordCnt"));
			this.uni_fg_wordCnt = Long.parseLong(conf.get("uni_fg_wordCnt"));
			this.bi_fg_wordCnt = Long.parseLong(conf.get("bi_fg_wordCnt"));
		}
		
		public void reduce(Text key, Iterable<AggregateWritable> values, Context context) throws IOException, InterruptedException{
			double p=0.0,q=0.0;
			double phraseScore=0.0, inforScore=0.0;
			double finalScore=0.0;
			String output = "";
			for(AggregateWritable val:values){
				p = (double)(val.getCxy() + 1) / (double) (bi_fg_wordCnt + bi_fg_voc); //fg 
				q = (double)(val.getCx() + 1) / (double)(uni_fg_wordCnt + uni_fg_voc) * (double)(val.getCy()+1) /(double)(uni_fg_wordCnt + uni_fg_voc);
				phraseScore = p*Math.log(p/q);
				
				p=(double)(val.getCxy()+1) /(double)(bi_fg_wordCnt+bi_fg_voc);  //fg
				q=(double)(val.getBxy()+1) /(double)(bi_bg_wordCnt+bi_bg_voc); //bg
				inforScore = p*Math.log(p/q);
				
				finalScore = phraseScore + inforScore;
				output = String.valueOf(finalScore)+"\t"+String.valueOf(phraseScore)+"\t"+String.valueOf(inforScore);
				context.write(key, new Text(output));
			}
		}//end of reduce
	}//end Reducer
	
}