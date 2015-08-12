import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class CompAccuracy {
	final static boolean DEBUG = false;
	public static void main(String[] args) throws IOException{
		String predFile,testFile;
		if(DEBUG){
			predFile = "rst.tiny";
			testFile = "abstract.tiny.test"; // /Users/lipingxiong/Documents/workspace/605hw5/src/			
		}
		else{
			predFile = args[0];
			testFile = args[1];
		}
		
		String[] Labels = {"nl","el","ru","sl","pl","ca","fr","tr","hu","de","hr","es","ga","pt"};
		int labelNum = Labels.length;
		long truePosSum = 0L;
		long trueNegSum = 0L;
		BufferedReader predbr = new BufferedReader(new FileReader(predFile));
		BufferedReader testbr = new BufferedReader(new FileReader(testFile));
		String predLine = predbr.readLine();
		String testLine = testbr.readLine();
		int docNum=1;
		while(testLine != null && predLine != null){
			long truePosNum = 0L;
			long trueNegNum = 0L;
			
			String[] testLabels = testLine.split("\\t")[0].split(",");
			HashSet<String> testLabelsSet = new HashSet<String>(Arrays.asList(testLabels));// true label set
			
			String[] preds = predLine.split("[\\t,]");
			HashSet<String> predPos = new HashSet<String>(); //pred label set
			HashSet<String> predNeg = new HashSet<String>(); //pred label set
			for(int i=0;i<preds.length ; i+=2){
				//System.out.println(preds[i]+ " "+preds[i+1]);
				if(Double.parseDouble(preds[i+1])>= 0.5D){
					predPos.add(preds[i]);
				}
				else{
					predNeg.add(preds[i]);
				}
			}
			/*
			 * For a (doc, label), if the doc has the label, and you predicted it correct (p>=0.5), it is true positive.
If the doc does not have the label, and you predicted it correct (p<0.5), it is true negative.
accuracy = (true positive + true negative) / (14 * #docs)
			 */
			for(String pred : predPos){
				if(testLabelsSet.contains(pred)){
					truePosNum += 1;
				}
			}
			for(String pred: predNeg){
				if(!testLabelsSet.contains(pred))
				trueNegNum += 1;
			}
			truePosSum +=  truePosNum;
			trueNegSum +=  trueNegNum;
			predLine = predbr.readLine();
			testLine = testbr.readLine();
			docNum++;
		}//end of while
	
		predbr.close();
		testbr.close();
		Double accuracy = (double)(truePosSum + trueNegSum) / (double)(14*docNum);
		System.out.println(String.valueOf(accuracy));
	}//main
}
