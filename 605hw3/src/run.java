import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class run {
	
	public static void main(String[] args) throws Exception {
		Configuration conf  = new Configuration();
		//conf.set("mapred.reduce.slowstart.completed.maps","1.0");
		
		Job job = new Job(conf, "NB");
		job.setJarByClass(run.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text,IntWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		 ArrayList<String> tokenizeDoc(String cur_doc) {
	        String[] words = cur_doc.split("\\s+");
	        ArrayList<String> tokens = new ArrayList<String>();
	        for (int i = 0; i < words.length; i++) {
	        	words[i] = words[i].replaceAll("\\W", "");
	        	if (words[i].length() > 0) {
	        		tokens.add(words[i]);
	        	}
	        }
	        return tokens;
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			HashSet labelSet = new HashSet<String>();
			labelSet.add("CCAT");
			labelSet.add("ECAT");
			labelSet.add("GCAT");
			labelSet.add("MCAT");
			
				String[] labelsAndTokens = line.split("\\t",2);
				String[] labels = labelsAndTokens[0].split(",");
				ArrayList<String> tokens = tokenizeDoc(labelsAndTokens[1]);				
				
				for(String label : labels){
					if(!labelSet.contains(label)) continue;
					
					//(Y=y)
					word.set("Y="+label);
					context.write(word, one);
					//System.out.println("Y="+label+"\t1");
					
					word.set("Y=*");
					context.write(word, one);
					//System.out.println("Y=*"+"\t1");
					
					word.set("Y="+label+",W=*");
					IntWritable tokensize = new IntWritable(tokens.size());
					context.write( word,tokensize);   // Y=y,W=*
					
					for(String token : tokens){
						word.set("Y="+label+",W="+token);
						context.write(word,one);
					}
				}//end for labels
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable,Text,IntWritable>{
			
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
				int sum = 0;
				for(IntWritable val: values){
					sum += val.get();
				}
				context.write(key,new IntWritable(sum));
			}
	}
	
}