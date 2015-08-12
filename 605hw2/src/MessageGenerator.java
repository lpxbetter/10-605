
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

public class MessageGenerator {
	 public static void main(String[] args) throws IOException {
		 BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
		 String line=null;
		 String bigram=null;
		 while ((line = br.readLine()) != null) {
			 bigram = line.split("\t")[0];
			 if(bigram.equals("#") || bigram.equals("!")) continue;
			 String strArr[]=bigram.split(" ");
			 System.out.println(strArr[0]+","+ bigram);
			 System.out.println(strArr[1]+","+ bigram);
		 }
	 }
}
