import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;


public class LR {
	final static boolean DEBUG = false;
	static double overflow=20;
	
    protected static double getP(double score) {
       if (score > overflow) score =overflow;
       else if (score < -overflow) score = -overflow;
       double exp = Math.exp(score);
       return exp / (1 + exp);
    }
	
	static HashMap<Integer, Integer> parseDoc(int V ,String doc) {
		String[] words = doc.split("\\s+");
		HashMap<Integer,Integer> wordsMap = new HashMap<Integer, Integer>();
		int id;
		for(int i=0;i<words.length;i++){
			words[i] = words[i].replace("\\W", "");
			if(words.length > 0){
				id = words[i].hashCode() % V;
				if(id < 0) id += V;
				wordsMap.put(id, 1);
			}
		}
		return wordsMap;
	}
	
	public static void main(String[] args) throws IOException {
		
		int vocSize = Integer.parseInt(args[0]);
		double rate = Double.parseDouble(args[1]);
		double coeff = Double.parseDouble(args[2]);
		int maxIter = Integer.parseInt(args[3]);
		int trainSize = Integer.parseInt(args[4]);
		String testData ; 
		if(DEBUG) testData = "abstract.tiny.test";
		else testData = args[5];
		
		String[] allLabels = {"nl","el","ru","sl","pl","ca","fr","tr","hu","de","hr","es","ga","pt"};
		
		int labelNum = allLabels.length;
		
		HashMap<String,Integer> labelMap = new HashMap<String,Integer>();
		for(int i=0;i<labelNum;i++){
			labelMap.put(allLabels[i], i);
		}
		
		double[][] B = new double[labelNum][vocSize];
		int[][] A = new int[labelNum][vocSize];
		BufferedReader br;
		if(DEBUG){
			 br = new BufferedReader(new FileReader("abstract.tiny.train"));
		}
		else{
			br = new BufferedReader(new InputStreamReader(System.in));
		}
        String line = br.readLine();
        
        double  dotProd, p, y, newRate = rate;
        int k;
        for(int t=1; t<=maxIter; t++){
        	newRate = rate/(t*t);
        	
        	double objFunc = 0D;
        	
        	k = 0;
        	while(line != null && k < trainSize){
        		k++;
        		//parse line
        		String[] arr = line.split("\\t",2);
        		String[] labelArr = arr[0].split(",");
        		HashSet<Integer> labelSet = new HashSet<Integer>();
        		for(String label : labelArr){
        			labelSet.add(labelMap.get(label));
        		}
        		HashMap<Integer,Integer> wordsMap = parseDoc(vocSize,arr[1]);
        		
        		// Each label
        		for(int l=0;l<labelNum; l++){
        			//Caculate p
        			dotProd = 0D;
        			for(Entry<Integer,Integer> entry : wordsMap.entrySet()){
        				dotProd += entry.getValue() * B[l][entry.getKey()];
        			}
        			p = getP(dotProd);
					y = labelSet.contains(l) ? 1D : 0D;
					
					//update weights
					for(Entry<Integer,Integer> entry : wordsMap.entrySet()){
						B[l][entry.getKey()] *= Math.pow(1D-2D*newRate*coeff, k-A[l][entry.getKey()]);
						B[l][entry.getKey()] += newRate*(y-p)*entry.getValue();
						A[l][entry.getKey()] = k;
 					}
        			
					objFunc += (y==1D)? Math.log(p) : Math.log(1-p);
					
        		}//end of label for
        		
        		line = br.readLine();
        	}//end of k while
        	
        	//print LCL
			//System.out.println("iteration "+String.valueOf(t)+" objFunc="+String.valueOf(objFunc));
			
        	//
        	for(int l=0; l<labelNum;l++){
        		for(int i=0;i<vocSize;i++){
        			B[l][i] *= Math.pow(1-2D*newRate*coeff, k-A[l][i]);
        		}
        	}
        	
			//reinitialize A
			A = new int[labelNum][vocSize];
			
        	
        }//end of t for
		br.close();
		
		//******************* TEST **************
		BufferedReader brd = new BufferedReader(new FileReader(testData));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
		
		line = brd.readLine();
		
		while (line != null) {
    		//parse line
    		String[] arr = line.split("\\t",2);
    		HashMap<Integer,Integer> wordsMap = parseDoc(vocSize,arr[1]);
    		
    		// Each label
    		for(int l=0;l<labelNum; l++){
    			Double score, dotP = 0D;
    			for(Entry<Integer,Integer> entry : wordsMap.entrySet()){
    				dotP += entry.getValue() * B[l][entry.getKey()];
    			}
    			score = getP(dotP);
    			if(l == 0){
    				bw.append(allLabels[l]+"\t"+ String.valueOf(score));
    			}
    			else{
    				bw.append(","+allLabels[l]+"\t"+ String.valueOf(score));
    			}
    		}//end of for l
    		bw.append("\n");
    		line = brd.readLine();
		}
		brd.close();
		bw.flush();
		bw.close();
	}
	
	
}
