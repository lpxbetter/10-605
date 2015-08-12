
import java.io.BufferedReader;
import java.io.BufferedWriter;
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


public class MessageUnigramCombiner {
	 public static void main(String[] args) throws IOException {
		 BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		 String preLine=null;
		 String line=null;
		 //String bigram=null;
		 
		 String x=null;

		 while ((line = br.readLine()) != null) {
			 if(line.split("\t")[0].equals("!")) {//skip the first line,"#\ttotal..."
				 System.out.println(line);
				 continue;
			 }
			 if(!line.contains(",")) {
				 x=line.split("\t")[0];
				 preLine=line;
				 continue;
			 }
			 String[] arr=preLine.split("\t");
			 if(line.contains(",")){
				 //xz
				 if(x.equals(line.split(",")[1].split(" ")[0])) {
					 //System.out.println(line.split(",")[1] + "\tx"+arr[1] + "\t"+arr[2] ); 
					 System.out.println(line.split(",")[1] + "\tCx="+arr[1] + "\tBx="+arr[2] );
				 } //zx
				 else if(x.equals(line.split(",")[1].split(" ")[1])) {
					 System.out.println(line.split(",")[1] + "\tCy="+arr[1] + "\tBy="+arr[2] );
				 }
			 }
		 }
	 }
}
