import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text,IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	static ArrayList<String> tokenizeDoc(String cur_doc) {
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
		String[] existingLabels = {"CCAT","ECAT","GCAT","MCAT"};
		
        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		BufferedReader br = new BufferedReader(new FileReader("/Users/lipingxiong/Documents/workspace/605hw3/src/tr") );
        
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