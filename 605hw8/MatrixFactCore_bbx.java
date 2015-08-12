package org.petuum.app.matrixfact;

import java.util.ArrayList;
import java.util.HashMap;

import org.petuum.ps.row.double_.DenseDoubleRow;
import org.petuum.ps.row.double_.DenseDoubleRowUpdate;
import org.petuum.ps.row.double_.DoubleRow;
import org.petuum.ps.row.double_.DoubleRowUpdate;
import org.petuum.ps.table.DoubleTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixFactCore {
    private static final Logger logger =
        LoggerFactory.getLogger(MatrixFactCore.class);

    // Perform a single SGD on a rating and update LTable and RTable
    // accordingly.
    public static void sgdOneRating(Rating r, double learningRate,
            DoubleTable LTable, DoubleTable RTable, int K, double lambda) {
        // TODO
    	// find the l and r value from r
    	int row = r.userId;
    	int col = r.prodId;
    	
    	//first lets cache the value in L and R tables
    	DoubleRow LCache = new DenseDoubleRow(K + 1);
    	DoubleRow aRow = LTable.get(row);
    	LCache.reset(aRow);
    	
    	// cache the col values
    	DoubleRow RCache = new DenseDoubleRow(K + 1);
    	DoubleRow aCol = RTable.get(col);
    	RCache.reset(aCol);
    	
    	float e = 0f;
    	for(int i = 0; i < K; i++) {
    		// calculate e_ij
    		e += LCache.getUnlocked(i) * RCache.getUnlocked(i);
    	}
    	e = r.rating - e;

    	// now update each rank in the ltable and the rtable
    	float rate = (float) (learningRate * 2), rVal, lVal;
    	float lamddaL = (float) (lambda/LCache.getUnlocked(K));
    	float lambdaR = (float) (lambda/RCache.getUnlocked(K));
    	
    	DoubleRowUpdate updateL = new DenseDoubleRowUpdate(K + 1);
    	DoubleRowUpdate updateR = new DenseDoubleRowUpdate(K + 1);
    	
    	for(int i = 0; i < K; i++) {
    		// update 
    		lVal = (float) LCache.getUnlocked(i);
    		rVal = (float) RCache.getUnlocked(i);

    		float deltaL = (rate * (e * rVal - lamddaL * lVal));
    		float deltaR = (rate * (e * lVal - lambdaR * rVal));

    		updateL.setUpdate(i, deltaL); 
    		updateR.setUpdate(i, deltaR);
    	}
    	
    	LTable.batchInc(row, updateL);
    	RTable.batchInc(col, updateR);
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
        
        // first let's calculate the square loss on entries.
        DoubleRow LCache = new DenseDoubleRow(K + 1);
        DoubleRow RCache = new DenseDoubleRow(K + 1);
        
        HashMap<Integer, Float> lMap = new HashMap<Integer, Float>();
        HashMap<Integer, Float> rMap = new HashMap<Integer, Float>();

        for(int i = elemBegin; i < elemEnd; i++) {
        	// calculate e_ij
        	Rating curRat = ratings.get(i);
        	float e = 0f;
        	LCache.reset((DoubleRow)LTable.get(curRat.userId));
        	RCache.reset((DoubleRow)RTable.get(curRat.prodId));
        	
        	float Lnorm = -1, Rnorm = -1;
        	for(int j = 0; j < K; j++) {
        		//compute the  dot product 
        		double lv = LCache.getUnlocked(j);
        		double rv = RCache.getUnlocked(j);
        		e += lv * rv;
        		
        		//as well as the L2 norm
        		if(curRat.userId < LRowEnd && curRat.userId >= LRowBegin) {
        			Lnorm += lv * lv;
        		}
        		
        		if(curRat.prodId < RRowEnd && curRat.prodId >= RRowBegin ) {
        			Rnorm += rv * rv;
        		}
        	}
        	
        	if(Lnorm != -1) {
        		lMap.put(curRat.userId, Lnorm + 1);
        	}
        	
        	if(Rnorm != -1) {
        		rMap.put(curRat.prodId, Rnorm + 1);
        	}
        	
        	sqLoss += (ratings.get(i).rating - e) * (ratings.get(i).rating - e);
        }
        
        // now calculate the norm loss
        for(int i = LRowBegin; i < LRowEnd; i++) {
        	if(lMap.containsKey(i)) {
//        		logger.info("LMap Hit");
        		totalLoss += lMap.get(i);
        	} else {
//        		logger.info("LMap Miss");
        		// we need to for a again.... damn!
        		for(int j = 0; j < K; j++) {
        			double lv = LCache.getUnlocked(j);
        			totalLoss += lv * lv;
        		}
        	}
        }
        
        for(int i = RRowBegin; i < RRowEnd; i++) {
        	if(rMap.containsKey(i)) {
        		totalLoss += rMap.get(i);
//        		logger.info("RMap Hit");
        	} else {
//        		logger.info("RMap Hit");
        		// we need to for a again.... damn!
        		for(int j = 0; j < K; j++) {
            		double rv = RCache.getUnlocked(j);
            		totalLoss += rv * rv;
        		}
        	}
        }
        
        totalLoss = totalLoss * lambda + sqLoss;	
        lossRecorder.incLoss(ithEval, "SquareLoss", sqLoss);
        lossRecorder.incLoss(ithEval, "FullLoss", totalLoss);
        lossRecorder.incLoss(ithEval, "NumSamples", elemEnd - elemBegin);
    }
}
