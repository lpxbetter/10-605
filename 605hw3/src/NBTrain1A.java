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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;


public class NBTrain1A {
	
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
	
	public static void main(String[] args) throws IOException {
		
		//String[] existingLabels = {"ca","de","el","es","fr","ga","hr","hu","nl","pl","pt","ru","sl","tr"};
		HashSet labelSet = new HashSet<String>();
		labelSet.add("CCAT");
		labelSet.add("ECAT");
		labelSet.add("GCAT");
		labelSet.add("MCAT");
		//System.out.println(labelSet.contains("CCAT") );
		String[] existingLabels = {"CCAT","ECAT","GCAT","MCAT"};
		
		//hashmap that links ClassName to array number of vectorOfDics
		HashMap<String,Integer> mapYy = new HashMap<String,Integer>();
		//HashMap<String,Integer> mapYStar = new HashMap<String,Integer>();
		int Ystar = 0;
		HashMap<String,Integer> mapYW = new HashMap<String,Integer>();
		HashMap<String,Integer> mapYWStar = new HashMap<String,Integer>();
		
        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		BufferedReader br = new BufferedReader(new FileReader("/Users/lipingxiong/Documents/workspace/605hw3/src/tr") );
        
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        
        String line = br.readLine();
        
		while (line != null) {
			
			//read labels and words
			String[] labelsAndTokens = line.split("\\t",2);
			String[] labels = labelsAndTokens[0].split(",");
			ArrayList<String> tokens = tokenizeDoc(labelsAndTokens[1]);				
			
			//for each label
			for(String label : labels){
				if(!labelSet.contains(label)) continue;
				String key = "Y="+label;
				if(!mapYy.containsKey(key)) mapYy.put(key, 1);
				else mapYy.put(key, mapYy.get(key) + 1);
				Ystar += 1;
				
				int N = 0;
				for(String token : tokens){
					key = "Y="+label+",W="+token;
					if(!mapYW.containsKey(key)) mapYW.put(key, 1);
					else mapYW.put(key, mapYW.get(key)+1);
					N++;
				}
				key = key = "Y="+label+",W=*";
				if(!mapYWStar.containsKey(key)) mapYWStar.put(key, 1);
				else mapYWStar.put(key, mapYWStar.get(key)+N);
				
			}//end for labels
			line = br.readLine();
			
		}//end while
		br.close();
		//for(String label:labelSet){
			outputMap(mapYy);
			outputMap(mapYW);
			outputMap(mapYWStar);
			System.out.println(Ystar);
		//}
		
	}
	
	public static void outputMap(HashMap<String,Integer> map){
		for (Entry<String, Integer> entry : map.entrySet()) {  
			  
		    System.out.println(entry.getKey() + "\t" + entry.getValue());  
		  
		} 
	}
	
}