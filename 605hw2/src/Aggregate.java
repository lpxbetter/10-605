
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

public class Aggregate {
	 
	 
	 public static void main(String[] args) throws IOException {
		 BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		 long fgCount=0; //froeground
		 long bgCount=0; //background
		 long totalfgCnt=0,totalbgCnt=0;
		 long unifgCnt=0,unibgCnt=0;
		 boolean isBigram=args[0].equals("1")? true:false;
		 String line=null;
		 //String fgStr= (Integer.parseInt(args[0])==1) ?"Cxy":"Cx";
		 //String bgStr= (Integer.parseInt(args[0])==1) ?"Bxy":"Bx";
		 StopWords swObj=new StopWords();
		 HashSet<String> stopwords = swObj.stopWords;
		 /*
		 BufferedReader readerStopword = new BufferedReader(new FileReader(new File("stopword.list") ));
		 while ((line = br.readLine()) != null) {
			 stopwords.add(line.trim());
		 }
		 readerStopword.close();
		 */
		 String pre=null,cur=null;
		 while ((line = br.readLine()) != null) {
			// System.out.println(line);
			 String strArr[]=line.trim().split("\t");
			 cur=strArr[0];
			 //ignore stopwords
			 if(isBigram==true){
				 if(stopwords.contains(cur.split(" ")[0])  || stopwords.contains(cur.split(" ")[1]) ){
					 continue;
				 }
			 }
			 else if(isBigram==false && stopwords.contains(cur)){
				 continue;
			 }
			 
			 if(pre==null) pre=cur;
			 if(!cur.equals(pre)){ // next word
				 System.out.println(pre+"\t"+fgCount+"\t"+bgCount);
				 if(fgCount>0) unifgCnt++;
				 if(bgCount>0) unibgCnt++;
				 totalfgCnt+=fgCount;
				 totalbgCnt+=bgCount;
				 bgCount=fgCount=0;//recover
			 }
			 if(Integer.parseInt(strArr[1]) >= 1960 && Integer.parseInt(strArr[1]) <= 1969){
				 fgCount += Integer.parseInt(strArr[2]);
			 }
			 else{
				 bgCount += Integer.parseInt(strArr[2]);
			 }
			 pre=cur;
		 }//end of while 
		 System.out.println(cur+"\t"+fgCount+"\t"+bgCount);
		 
		 if(fgCount>0) unifgCnt++;
		 if(bgCount>0) unibgCnt++;
		 totalfgCnt+=fgCount;
		 totalbgCnt+=bgCount;

		 
		 String identifier= (isBigram==true)? "#":"!";  // bigram #, unigram !
		 System.out.println(identifier+"\t"+"totalfgCnt="+totalfgCnt+"\ttotalbgCnt="+totalbgCnt+"\tuniquefgCnt="+unifgCnt+"\tuniquebgCnt="+unibgCnt);
	 }
	 
}