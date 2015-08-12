import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;



public class run_hadoop_phrase {
	
	
	public static HashMap<String, Long> getCounts(String count_sizeOutput) throws Exception{
		
        FileSystem fs = FileSystem.get(new URI(count_sizeOutput), new Configuration());
        FileStatus[] statuses = fs.listStatus(new Path(count_sizeOutput));
        
        HashMap<String, Long> totalCounts = new HashMap<String, Long>();

        for (FileStatus status : statuses)
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            String line;
            while ((line = br.readLine()) != null)
            {
                String[] stats = line.split("\t");
                totalCounts.put(stats[0], Long.parseLong(stats[1]));
            }
        }

        return totalCounts;
    }

	
	public static void main(String[] args) throws IOException{
		
		boolean wasSuccessfulRun = false;
		
		String unigramPath = args[0];
		String bigramPath = args[1];
		String aggregateFolder = args[2];
		String sizeCountsFolder = args[3];
		String unigramMessagesFolder = args[4];
		String finalFolder = args[5];
		
		
		//AGGREGATE JOB
		JobConf jobA = new JobConf(run_hadoop_phrase.class);
		jobA.setJobName("Aggregate");
		
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(CountDataWritable.class);
		 
		jobA.setMapperClass(Aggregate.AggregateMap.class);
		jobA.setCombinerClass(Aggregate.AggregateReduce.class);
		jobA.setReducerClass(Aggregate.AggregateReduce.class);
		jobA.setNumReduceTasks(10);
		
		jobA.setInputFormat(TextInputFormat.class);
		jobA.setOutputFormat(SequenceFileOutputFormat.class);
		//jobA.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobA, new Path(unigramPath), new Path(bigramPath));
		FileOutputFormat.setOutputPath(jobA, new Path(aggregateFolder));
		
		Job job_A = new Job(jobA);
		try {
			wasSuccessfulRun = job_A.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		//COUNT_SIZE JOB
		JobConf jobB = new JobConf(run_hadoop_phrase.class);
		jobB.setJobName("CountSize");
		
		jobB.setOutputKeyClass(Text.class);
		jobB.setOutputValueClass(LongWritable.class);
		 
		jobB.setMapperClass(Count_Size.CountSizeMap.class);
		jobB.setCombinerClass(Count_Size.CountSizeReduce.class);
		jobB.setReducerClass(Count_Size.CountSizeReduce.class);
		jobB.setNumReduceTasks(1);
		
		jobB.setInputFormat(SequenceFileInputFormat.class);
		jobB.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobB, new Path(aggregateFolder));
		FileOutputFormat.setOutputPath(jobB, new Path(sizeCountsFolder));
		
		Job job_B = new Job(jobB);
		try {
			wasSuccessfulRun = job_B.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		//MESSAGES PART 1 - JOB
		JobConf jobC1 = new JobConf(run_hadoop_phrase.class);
		jobC1.setJobName("MessagePart1");
		
		jobC1.setMapOutputKeyClass(Text.class);
		jobC1.setMapOutputValueClass(MessageAndCountsWritable.class);
		
		jobC1.setOutputKeyClass(Text.class);
		jobC1.setOutputValueClass(CountDataWritable.class);
		 
		jobC1.setMapperClass(Message_Unigram.MessageUnigramPartOneMap.class);
		jobC1.setReducerClass(Message_Unigram.MessageUnigramPartOneReduce.class);
		jobC1.setNumReduceTasks(10);
		
		jobC1.setInputFormat(SequenceFileInputFormat.class);
		jobC1.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobC1, new Path(aggregateFolder));
		FileOutputFormat.setOutputPath(jobC1, new Path(unigramMessagesFolder+"/Part1/"));
		
		Job job_C1 = new Job(jobC1);
		try {
			wasSuccessfulRun = job_C1.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		//MESSAGES PART 2 - JOB
		
		JobConf jobC2 = new JobConf(run_hadoop_phrase.class);
		jobC2.setJobName("Aggregate");
		
		jobC2.setOutputKeyClass(Text.class);
		jobC2.setOutputValueClass(CountDataWritable.class);
		 
		jobC2.setMapperClass(Message_Unigram.MessageUnigramPartTwoMap.class);
		jobC2.setCombinerClass(Message_Unigram.MessageUnigramPartTwoReduce.class);
		jobC2.setReducerClass(Message_Unigram.MessageUnigramPartTwoReduce.class);
		jobC2.setNumReduceTasks(10);
		
		jobC2.setInputFormat(SequenceFileInputFormat.class);
		jobC2.setOutputFormat(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobC2, new Path(unigramMessagesFolder+"/Part1/"), new Path(aggregateFolder));
		FileOutputFormat.setOutputPath(jobC2, new Path(unigramMessagesFolder+"/Part2/"));
		
		Job job_C2 = new Job(jobC2);
		try {
			wasSuccessfulRun = job_C2.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		
		// JOB FINAL
		JobConf jobD = new JobConf(run_hadoop_phrase.class);
		jobD.setJobName("Aggregate");
		
		try {
			HashMap<String, Long> counts = getCounts(sizeCountsFolder+"/part-00000");
			for(String k : counts.keySet()){
				jobD.setLong(k, counts.get(k));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		jobD.setOutputKeyClass(Text.class);
		jobD.setOutputValueClass(CountDataWritable.class);
		 
		jobD.setMapperClass(Compute.ComputeMap.class);
		jobD.setReducerClass(Compute.ComputeReduce.class);
		jobD.setNumReduceTasks(10);
		
		jobD.setInputFormat(SequenceFileInputFormat.class);
		jobD.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(jobD, new Path(unigramMessagesFolder+"/Part2/"));
		FileOutputFormat.setOutputPath(jobD, new Path(finalFolder));
		
		Job job_D = new Job(jobD);
		try {
			wasSuccessfulRun = job_D.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
}		
		
		
		
		

