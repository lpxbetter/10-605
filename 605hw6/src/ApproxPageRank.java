import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;

public class ApproxPageRank {
	private static HashSet<String> cachedNodes ;
	private static HashMap<String, String[]> neighborsMap;
	private static HashMap<String,Double> pMap;
	private static HashMap<String,Double> rMap;
	private static double alpha;
	private static double epsilon;
	private static String inputPath;
	private static String seed;
	
	public static void main(String[] args) throws IOException{
		inputPath = args[0];
		seed = args[1];
		alpha = Double.parseDouble(args[2]);
		epsilon = Double.parseDouble(args[3]);
		
		/*
		HashSet<String> cachedNodes = new HashSet<String>();
		HashMap<String, String[]> neighborsMap = new HashMap<String, String[]>();
		HashMap<String,Double> pMap = new HashMap<String, Double>();
		HashMap<String,Double> rMap = new HashMap<String, Double>();
		*/
		cachedNodes = new HashSet<String>();
		neighborsMap = new HashMap<String, String[]>();
		pMap = new HashMap<String, Double>();
		rMap = new HashMap<String, Double>();
		
		pMap.put(seed,0D);
		rMap.put(seed, 1D);
		cachedNodes.add(seed);
		
		getNeighbors();
		cachedNodes.clear();
		
		createsubGraph();
		getLowConductanceGraph();
	
	}// end of main
	
	public static void createsubGraph() throws IOException{
		boolean needPush = true;
		while(needPush){
			boolean hasPushed = true;
			while(hasPushed){
				hasPushed = false;
				for(String u : neighborsMap.keySet()){
					Double rdRatio = rMap.get(u) / neighborsMap.get(u).length;
					if(rdRatio >= epsilon) hasPushed = true;
					else continue;
					double p = pMap.containsKey(u) ? pMap.get(u) : 0D;
					double r = rMap.get(u);
					pMap.put(u, p + alpha * r);
					rMap.put(u, (1D - alpha) * r / 2D);
					String[] vs = neighborsMap.get(u) ;
					double len = vs.length;
					for(String v:vs){
						double vr= rMap.containsKey(v) ? rMap.get(v) : 0D;
						rMap.put(v, vr + (1D - alpha) * r / (2D * len) );
					}
				}//end for
			}//end while
			
			for(Entry<String, Double> entry : rMap.entrySet()){
				if(entry.getValue() >= epsilon && !neighborsMap.containsKey(entry.getKey())){
					cachedNodes.add(entry.getKey());
				}
			}
			
			getNeighbors();
			cachedNodes.clear();
			
			needPush = false;
			for(String n : neighborsMap.keySet()){
				Double rdRatio = rMap.get(n) / neighborsMap.get(n).length; //r/d
				if(rdRatio >= epsilon){
					needPush = true;
					break;
				}
			}
			
		}//end while

	}
	
	public static void getLowConductanceGraph(){
		HashSet<String> temp = new HashSet<String>();
		HashSet<String> S = new HashSet<String>();
		HashSet<String> S_Star = new HashSet<String>();
		//HashSet<String> newElements = new HashSet<String>();
		
		S.add(seed);
		S_Star.add(seed);
		
		double volume = neighborsMap.get(seed).length;
		double boundary = getBoundary(S);
		double condS = boundary / volume;
		double condS_Star = condS;
		
		//sort nodes by pagerank decresely
		// Convert map.entrySet() to list
		List<Map.Entry<String,Double>> list = new ArrayList<Map.Entry<String,Double>>( pMap.entrySet() );

		      Collections.sort(list, new Comparator<Map.Entry<String,Double>>() {
		          public int compare(Map.Entry<String, Double> o1,
		                  Map.Entry<String, Double> o2) {
		              //return o1.getValue().compareTo(o2.getValue());
		          //Decreasing
		          return o2.getValue().compareTo(o1.getValue());
		          }
		      });
		      
		for(Map.Entry<String,Double> e : list){
			String u = e.getKey();
			if(u.equals(seed)) continue;
			S.add(u);
			temp.add(u);
			volume += neighborsMap.get(u).length ;
			boundary = getBoundary(S);
			condS = boundary / volume;
			if(condS < condS_Star){
				S_Star.addAll(temp);
				temp.clear();
				condS_Star = condS;
			}
		}

		for(String u: S_Star){
			System.out.println(u+"\t"+pMap.get(u).toString());
		}
		
	}
	
	public static double getBoundary(HashSet<String> S){
		double boundary = 0D;
		for(String u : S){
			String[] neighborsOfu = neighborsMap.get(u); 
			for(String v : neighborsOfu){
				if(!S.contains(v)){
					boundary++;
				}
			}
		}
		
		return boundary;
	}
	
	/*
	 * get neighbors of node from the file
	 */
//	public static void getNeighbors( String inputPath, HashSet<String> cachedNodes, HashMap<String,String[]> neighborsMap) throws IOException{
		public static void getNeighbors() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(inputPath));
		String line = null;
		while( !cachedNodes.isEmpty()  && (line=br.readLine()) != null){
			int idxSpace = line.indexOf("\t");
			String u = line.substring(0,idxSpace);
			//String[] arr = line.split("[\\t]");
			//String u = arr[0];
			if(cachedNodes.contains(u)){
				String[] arr = line.split("[\\t]");
				neighborsMap.put(u, Arrays.copyOfRange(arr, 1, arr.length));
				cachedNodes.remove(u);
			}
		}//end while
		br.close();
	}//end getNeighbors
	
}