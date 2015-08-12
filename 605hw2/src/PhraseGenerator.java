
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
/*
 * About calculating phraseness and informativeness in HW2
Let FG=foreground, BG=background.
 
It seems that Bx & By is not used.
 
Phraseness = KL(p_phraseness | q_phraseness)
p_phraseness = ProbFG(x^y) = Cxy / # FG bigrams
q_phraseness = ProbFG(x) * ProbFG(y) = (Cx / # FG unigrams) * (Cy / # FG unigrams)
So phrasesness uses only (Cx, Cy, Cxy).
 
Informativeness = KL(p_info | q_info)
p_info = p_phraseness = Cxy / # FG bigrams
q_info = Bxy / # BG bigrams
So informativeness uses only (Cxy, Bxy).
 */
/*
 * input:
a war	Cxy169328	Bxy420820
a war	Cx=219340038	Bx=801301707
a war	Cy=5682066	By=15897300
aa war	7	20
aa war	Cx=221363	Bx=876134
aa war	Cy=5682066	By=15897300
abandon war	62	118
abandon war	Cx=173229	Bx=544511
abandon war	Cy=5682066	By=15897300
abandoned war	37	89
 */
public class PhraseGenerator {
	public static Double p_phraseness,q_phraseness, p_info, q_info,phrasenessScore,inforScore,score;
	public static long FG_bigrams=0,BG_bigrams=0,FG_bigramsDistinct=0,BG_bigramsDistinct=0;
	public static long FG_unigrams=0,BG_unigrams=0,FG_unigramsDistinct=0,BG_unigramsDistinct=0;
	final static boolean debug = false;
	
	public static class phraseNode {
		private String phrase=new String();
		private Double totalScore=0.0;
		private Double phrasenessScore=0.0;
		private Double inforScore=0.0;
		public phraseNode(String strPhrase,Double Score, Double pScore, Double iScore){
			phrase=strPhrase;
			totalScore=Score;
			phrasenessScore=pScore;
			inforScore=iScore;
		}
	}
    //Comparator anonymous class implementation
    public static Comparator<phraseNode> scoreComparator = new Comparator<phraseNode>(){
        @Override
        public int compare(phraseNode n1, phraseNode n2) {
        	if(n1.totalScore==n2.totalScore) return 0;
        	else if(n1.totalScore>n2.totalScore) return 1;
        	else return -1;
            //return (int) (n1.totalScore - n2.totalScore);
        }
    };
    
	 public static void main(String[] args) throws IOException {
		 Queue<phraseNode> phrasePQ = new PriorityQueue<phraseNode>(20,scoreComparator);
		 
		 BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		 //BufferedReader br = new BufferedReader(new FileReader(new File("/Users/lipingxiong/Documents/workspace/605hw3/src/s5") ));
		 String line=null;
		 String bigram=null;
		 
		 int i=1;
		 long Cxy=0,Bxy=0,Cx=0,Cy=0;
		 
		 //!	totalfgCnt=9228771164	totalbgCnt=33150328842	uniquefgCnt=12337	uniquebgCnt=12337
		 //#	totalfgCnt=8735437	totalbgCnt=24441229	uniquefgCnt=15558	uniquebgCnt=15558
		 line = br.readLine();
		 String[] arr=line.split("\t");
		 
		 if(arr[0].equals("!")){
			 FG_unigrams=Long.parseLong(arr[1].split("=")[1]);
			 BG_unigrams=Long.parseLong(arr[2].split("=")[1]);
		     FG_unigramsDistinct=Long.parseLong(arr[3].split("=")[1]);
		     BG_unigramsDistinct=Long.parseLong(arr[4].split("=")[1]);
		 }
		 line = br.readLine();
		 arr=line.split("\t");
		 
		 if(arr[0].equals("#")){
			 FG_bigrams=Long.parseLong(arr[1].split("=")[1]);
			 BG_bigrams=Long.parseLong(arr[2].split("=")[1]);
		     FG_bigramsDistinct=Long.parseLong(arr[3].split("=")[1]);
		     BG_bigramsDistinct=Long.parseLong(arr[4].split("=")[1]);
		 }
		 printCounts();
		 while ((line = br.readLine()) != null) {
			 if(debug) System.out.println("line:"+line);
			 arr=line.split("\t");
			 
			 bigram =arr[0];
			 if(i==1){
				 Cxy=Long.parseLong(arr[1]);
				 Bxy=Long.parseLong(arr[2]);
				 i++;
			 }
			 else if(i==2){
				 Cx=Long.parseLong(arr[1].split("=")[1]);
				 //Cx=Long.parseLong(arr[1].substring(1));
				 i++;
			 }
			 else if(i==3){
				 Cy=Long.parseLong(arr[1].split("=")[1]);
				 //Cy=Long.parseLong(arr[1].substring(1));
				 
				 if(debug) System.out.println("Cxy,Bxy,Cx,Cy:"+Cxy+"\t"+Bxy+"\t"+Cx+"\t"+Cy);
				 //compute pharaseness, only need fg 
				 p_phraseness = (double)(Cxy+1) / (double)(FG_bigrams+FG_bigramsDistinct);
				 //q_phraseness = ProbFG(x) * ProbFG(y) = (Cx / # FG unigrams) * (Cy / # FG unigrams)
				 q_phraseness = ((double)(Cx+1) / (double)(FG_unigrams+FG_unigramsDistinct)) * ((double)(Cy+1)/(double)(FG_unigrams+FG_unigramsDistinct) );
				 phrasenessScore=p_phraseness*Math.log(p_phraseness/q_phraseness);
				 if(debug)  System.out.println(p_phraseness+"\t"+q_phraseness+"\t"+phrasenessScore);
				 
				 //computer informationness, need fg and bg
				 p_info = p_phraseness;
			     q_info = (double)(Bxy+1) / (double)(BG_bigrams+BG_bigramsDistinct);
				 inforScore=p_info * Math.log( p_info/q_info );
					 
				 score=phrasenessScore+inforScore;
				 if(debug)  System.out.println(p_info+"\t"+q_info+"\t"+inforScore + "\tscore="+score);
				 //System.out.println(phrasePQ.size());
				 if(phrasePQ.size()>20){
					 phrasePQ.poll();
				 }
				 phrasePQ.add(new phraseNode(bigram,score,phrasenessScore,inforScore));
				 //recover
				 i=1;
			 }
			 
		 }//end while
		 
		 ArrayList<phraseNode> nodeArr=new ArrayList<phraseNode>();
		 while(!phrasePQ.isEmpty()){
	            phraseNode node = phrasePQ.poll();
	            nodeArr.add(node);
	        }
		 // j from size-1 to 1, not to 0. 
		 for(int j=nodeArr.size()-1;j>=1;j--){
			 phraseNode node=nodeArr.get(j);
			 System.out.println(node.phrase+"\t"+node.totalScore+"\t"+node.phrasenessScore+"\t"+node.inforScore);
		 }
	 }
	 
	 //¡phrase¿¡total score¿¡phraseness score¿¡informativeness score¿
	 
	 public static void printCounts(){
		 if(!debug) return;
		 System.out.println("unigram vals："+FG_unigrams +"\t"+ BG_unigrams+"\t"+FG_unigramsDistinct+"\t"+BG_unigramsDistinct);
		 System.out.println("bigrams vals："+FG_bigrams +"\t"+ BG_bigrams+"\t"+FG_bigramsDistinct+"\t"+BG_bigramsDistinct);	 
	 }
	 
	 
}
