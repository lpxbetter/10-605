import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class run_hadoop_phrase {
	
	public static HashMap<String, Long>  getCountSize(String out) throws IOException, URISyntaxException{
		FileSystem fs = FileSystem.get(new URI(out), new Configuration());
		FileStatus[] statusArr = fs.listStatus(new Path(out));
		HashMap<String,Long> map = new HashMap<String, Long>();
		for(FileStatus status:statusArr){
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            String line;
            while ((line = br.readLine()) != null)
            {
                String[] stat = line.split("\t");
                map.put(stat[0], Long.parseLong(stat[1]));
            }
		}
		return map;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		String unigramPath = args[0];
		String bigramPath = args[1];
		String aggregateDir = args[2];
		String sizeCountsDir = args[3];
		String messageUnigDir = args[4];
		String computeDir = args[5];
		
		//aggregregate
		Configuration conf  = new Configuration();
		Job jobAgg = new Job(conf, "AggregateJob");
		jobAgg.setJarByClass(run_hadoop_phrase.class);
		
		jobAgg.setOutputKeyClass(Text.class);
		jobAgg.setOutputValueClass(AggregateWritable.class);
		
		jobAgg.setMapperClass(Aggregate.AggregateMap.class);
		jobAgg.setCombinerClass(Aggregate.AggregateReduce.class);
		jobAgg.setReducerClass(Aggregate.AggregateReduce.class);
		
		jobAgg.setInputFormatClass(TextInputFormat.class);
		jobAgg.setOutputFormatClass(SequenceFileOutputFormat.class);
		//jobAgg.setOutputFormat(TextOutputFormat.class);
		
		jobAgg.setNumReduceTasks(10);

		FileInputFormat.setInputPaths(jobAgg, new Path(unigramPath), new Path(bigramPath));
		FileOutputFormat.setOutputPath(jobAgg, new Path(aggregateDir));
		
		jobAgg.waitForCompletion(true);
		
		
		// countsize
		//Configuration conf  = new Configuration();
		Job jobCS = new Job(conf, "CountSize");
		jobCS.setJarByClass(run_hadoop_phrase.class);
		
		jobCS.setOutputKeyClass(Text.class);
		jobCS.setOutputValueClass(LongWritable.class);
		
		jobCS.setMapperClass(CountSize.CountSizeMap.class);
		jobCS.setCombinerClass(CountSize.CountSizeReduce.class);
		jobCS.setReducerClass(CountSize.CountSizeReduce.class);
		
		jobCS.setInputFormatClass(SequenceFileInputFormat.class);
		jobCS.setOutputFormatClass(TextOutputFormat.class);
		
		jobCS.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(jobCS, new Path(aggregateDir));
		FileOutputFormat.setOutputPath(jobCS, new Path(sizeCountsDir));
		
		jobCS.waitForCompletion(true);
		
		//MessageUnigram step1
		Job jobMU = new Job(conf, "MessageUnigram");
		jobMU.setJarByClass(run_hadoop_phrase.class);
		jobMU.setMapOutputKeyClass(Text.class);
		jobMU.setMapOutputValueClass(MessageWritable.class);
		
		jobMU.setOutputKeyClass(Text.class);
		jobMU.setOutputValueClass(AggregateWritable.class);
		
		jobMU.setMapperClass(Message_Unigram.MuMap.class);
		jobMU.setReducerClass(Message_Unigram.MuReduce.class);
		
		jobMU.setNumReduceTasks(10);
		jobMU.setInputFormatClass(SequenceFileInputFormat.class); //sequence in
		jobMU.setOutputFormatClass(SequenceFileOutputFormat.class); //sequence out
		
		FileInputFormat.setInputPaths(jobMU, new Path(aggregateDir));
		FileOutputFormat.setOutputPath(jobMU, new Path(messageUnigDir+"/step1/"));
		jobMU.waitForCompletion(true);
		
		//MessageUnigram step2
		Job jobMU2 = new Job(conf,"MessageUnigram2");
		jobMU2.setJarByClass(run_hadoop_phrase.class);
		jobMU2.setOutputKeyClass(Text.class);
		jobMU2.setOutputValueClass(AggregateWritable.class);
		
		jobMU2.setMapperClass(Message_Unigram.MuMap2.class);
		jobMU2.setReducerClass(Message_Unigram.MuReduce2.class);
		jobMU2.setNumReduceTasks(10);
		
		jobMU2.setInputFormatClass(SequenceFileInputFormat.class);
		jobMU2.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(jobMU2, new Path(messageUnigDir+"/step1/"), new Path(aggregateDir));
		FileOutputFormat.setOutputPath(jobMU2, new Path(messageUnigDir+"/step2/") );
		jobMU2.waitForCompletion(true);
		
		//compute
		conf = new Configuration();
		try {
			HashMap<String,Long> countMap = getCountSize(sizeCountsDir+"/part-r-00000");
			for(String s:countMap.keySet()){
				conf.setLong(s,countMap.get(s));
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		Job jobCom = new Job(conf,"Compute");
		jobCom.setJarByClass(run_hadoop_phrase.class);

		jobCom.setMapOutputKeyClass(Text.class);
		jobCom.setMapOutputValueClass(AggregateWritable.class);
		
		jobCom.setOutputKeyClass(Text.class);
		jobCom.setOutputValueClass(Text.class);
		 
		jobCom.setMapperClass(Compute.computeMap.class);
		jobCom.setReducerClass(Compute.computeReduce.class);
		jobCom.setNumReduceTasks(10);
		
		jobCom.setInputFormatClass(SequenceFileInputFormat.class);
		jobCom.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobCom, new Path(messageUnigDir+"/step2/"));
		FileOutputFormat.setOutputPath(jobCom, new Path(computeDir));
		
		jobCom.waitForCompletion(true);
		
	}
	
	
}