package com.zhiyuan.demo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
public class FileSpilt {
 public static void main(String[] args) {
        try {
         //BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
         BufferedReader br = new BufferedReader(new FileReader("c:\\unigram_war.sorted.txt"));
           
        // Scanner reader=new Scanner(System.in);
         //reader.useDelimiter("\n");
         
            String s;
            StringBuffer sb = new StringBuffer();
            Map<String, Long> cmap=new HashMap<String, Long>();
            Map<String, Long> bmap=new HashMap<String, Long>();
            Map<String, String> tempmap=new HashMap<String, String>();
            while ((s = br.readLine()) != null) {
                sb.append(s);
                StringTokenizer st = new StringTokenizer(sb.toString(),"\t");
                while (st.hasMoreTokens()) {
                 String keyword = st.nextToken();//单词
                 Long year =  Long.valueOf(st.nextToken());//年份
                 String core =  st.nextToken().trim();//值
                 Long count;
                 if(year >=1960 && year<=1969){
                  if (cmap.get(keyword) == null) {
                            count = Long.valueOf(core);
                        } else {
                            count = cmap.get(keyword).intValue() + Long.valueOf(core);
                        }
                        cmap.put(keyword,count);
                 }else if(year >=1970 && year<=1999){
                  if (bmap.get(keyword) == null) {
                            count = Long.valueOf(core);
                        } else {
                            count = bmap.get(keyword).intValue() + Long.valueOf(core);
                        }
                        bmap.put(keyword,count);
                 }
    }
                sb.delete(0, sb.length());
            }
            String str = "";
            Set<Entry<String, Long>> bSet=bmap.entrySet();  
            Set<Entry<String, Long>> cSet=cmap.entrySet();
           
            Iterator<Map.Entry<String, Long>> biter=bSet.iterator();
            Iterator<Map.Entry<String, Long>> citer=cSet.iterator();
            
            BufferedWriter bw = new BufferedWriter(new FileWriter("c:\\temp.txt"));
				while (citer.hasNext()) {
					Map.Entry<String, Long> c = citer.next();
					Map.Entry<String, Long> b = biter.next();
					//if (c.getKey().equals(b.getKey())) {
						
						str = b.getKey() + "\t" + "Cx="+"Bx="+b.getValue()+"\n";
						tempmap.put(c.getKey(), str);
						
					//}
					//System.out.println(str);
					
				}
				Set<Entry<String, String>> tempSet=tempmap.entrySet();
				Iterator<Map.Entry<String, String>> tempiter=tempSet.iterator();
				while(tempiter.hasNext()){
					Map.Entry<String, String> temp = tempiter.next();
					bw.write(temp.getValue());
					bw.flush();
				} 
           
        }catch(Exception e){
         e.printStackTrace();
        }
 }
}
