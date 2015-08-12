package org.petuum.app.matrixfact;

import org.petuum.app.matrixfact.Rating;
import org.petuum.app.matrixfact.LossRecorder;

import org.petuum.ps.PsTableGroup;
import org.petuum.ps.row.double_.DenseDoubleRow;
import org.petuum.ps.row.double_.DenseDoubleRowUpdate;
import org.petuum.ps.row.double_.DoubleRow;
import org.petuum.ps.row.double_.DoubleRowUpdate;
import org.petuum.ps.table.DoubleTable;
import org.petuum.ps.common.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class MatrixFactCore {
    private static final Logger logger =
        LoggerFactory.getLogger(MatrixFactCore.class);

    // Perform a single SGD on a rating and update LTable and RTable
    // accordingly.
    public static void sgdOneRating(Rating r, double learningRate,
            DoubleTable LTable, DoubleTable RTable, int K, double lambda) {
        // TODO
    	int uid=r.userId;
    	int mid=r.prodId;
    	DoubelRow LCache=new DenseDoubleRow(K+1);
    	DoubleRow oneRow=LTable.get(uid);
    	LCache.reset(oneRow);
    	
    	DoubleRow RCache=new DenseDoubleRow(K+1);
    	DoubelRow oneCol=RTable.get(mid);
    	RCache.reset(oneCol);
    	
    	double eij=0D;
    	for(int i=0;i<K;i++){
    		eij+=LCache.getUnlocked(i) * RCache.getUnlocked(i);
    	}
    	eij=r.rating-eij;
    	
     	DoubleRowUpdate updateL = new DenseDoubleRowUpdate(K + 1);
    	DoubleRowUpdate updateR = new DenseDoubleRowUpdate(K + 1);    	
    			
    	for(int i=0;i<K;i++){
    		double Li=(double) LCache.getUnlocked(i);
    		double Ri=(double) RCache.getUnlocked(i);
    		
    		updateL.setUpdate(i,2*learningRate*(eij*Ri-((double) (lambda/LCache.getUnlocked(K)))*Li)  );
    		updateR.setUpdate(i,2*learningRate*(eij*Li-((double) (lambda/RCache.getUnlocked(K)))*Ri ) );
    	} 
    	LTable.batchInc(uid,updateL);
    	RTable.batchInc(mid,updateR);
    	//Li= Li + 2*learningRate(eij*Ri  - (lambda/ni) * Li);
    	//Rj= Rj + 2*learningRate(eij*Li - (lambda/mj) * Rj );
    	
    }

    // Evaluate square loss on entries [elemBegin, elemEnd), and L2-loss on of
    // row [LRowBegin, LRowEnd) of LTable,  [RRowBegin, RRowEnd) of Rtable.
    // Note the interval does not include LRowEnd and RRowEnd. Record the loss to
    // lossRecorder.
    public static void evaluateLoss(ArrayList<Rating> ratings, int ithEval,
            int elemBegin, int elemEnd, DoubleTable LTable,
            DoubleTable RTable, int LRowBegin, int LRowEnd, int RRowBegin,
            int RRowEnd, LossRecorder lossRecorder, int K, double lambda) {
        // TODO
        double sqLoss = 0;
        double totalLoss = 0;
        
        for(int i=elemBegin;i<elemEnd;i++){
        	Rating r=ratings.get(i);
        	int uid=r.userId;
        	int mid=r.prodId;
        	DoubelRow LCache=new DenseDoubleRow(K+1);
        	DoubleRow oneRow=LTable.get(uid);
        	LCache.reset(oneRow);
        	
        	DoubleRow RCache=new DenseDoubleRow(K+1);
        	DoubelRow oneCol=RTable.get(mid);
        	RCache.reset(oneCol);
        	double eij=0D;
        	for(int j=0;j<K;j++){
        		eij+=LCache.getUnlocked(j) * RCache.getUnlocked(j);
        	}
        	sqLoss+=Math.pow(r.rating-eij,2) ;
        }
        double lLoss=0D;
        for(int i=LRowBegin;i<LRowEnd;i++){
        	DoubelRow LCache=new DenseDoubleRow(K+1);
        	DoubleRow oneRow=LTable.get(i);
        	LCache.reset(oneRow);
        	for(int j=0;j<K;j++){
        		lLoss+=LCache.getUnlocked(j) * LCache.getUnlocked(j);
        	}        	
        }
        double rLoss=0D;
        for(int i=RRowBegin;i<RRowEnd;i++){
        	DoubleRow RCache = new DenseDoubleRow(K+1);
        	DoubleRow oneRow = RTable.get(i);
        	RCache.reset(oneRow);
       
        	for(int j=0;j<K;j++){
        		rLoss+=RCache.getUnlocked(j) * RCache.getUnlocked(j);
        	}
        }
        totalLoss = sqLoss + lambda * ( lLoss + rLoss );
        lossRecorder.incLoss(ithEval, "SquareLoss", sqLoss);
        lossRecorder.incLoss(ithEval, "FullLoss", totalLoss);
        lossRecorder.incLoss(ithEval, "NumSamples", elemEnd - elemBegin);
    }
}
