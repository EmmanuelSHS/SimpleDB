package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	// TODO: improve with finer granularity statistics
	private int _max;
	private int _min;
	private int _nbuckets;
	private int _width;
	private int[] _bucket;
	private int[] _bucketmin;
	private int[] _bucketmax;
	private int _totn;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	_max = max;
    	_min = min;
    	_nbuckets = buckets;
    	_width = (_max - _min) / _nbuckets + 1;
    	_bucket = new int[_nbuckets];
    	_totn = 0;
    	
    	_bucketmin = new int[_nbuckets];
    	_bucketmax = new int[_nbuckets];
    	for (int i = 0; i < _nbuckets; ++i) {
        	_bucketmin[i] = _min + _width * (i);
        	_bucketmax[i] = _min + _width * (i + 1);
    	}
    }

    private int getIdx(int v) {
    	if (v < _min || v > _max) {
    		return -1;
    	} else {
    		return (v - _min) / _width;
    	}
    }
    
    private int getWidth(int idx) {
    	return _bucketmax[idx] - _bucketmin[idx] + 1;
    }
    
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int idx = getIdx(v);
    	_bucket[idx]++;
    	_totn++;
    	
    	_bucketmin[idx] = Math.min(v, _bucketmin[idx]);
    	_bucketmax[idx] = Math.max(v, _bucketmax[idx]);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// boundary
    	if (v < _min) {
    		switch (op) {
    		case GREATER_THAN:
    		case GREATER_THAN_OR_EQ:
    		case NOT_EQUALS:
    			return 1.0;
    		default:
    			return 0.;
    		}
    	}
    	if (v > _max) {
    		switch (op) {
    		case LESS_THAN:
    		case LESS_THAN_OR_EQ:
    		case NOT_EQUALS:
    			return 1.0;
    		default:
    			return 0.;
    		}
    	}
    	
    	// normal case, naive solution
    	int idx = getIdx(v);
    	double res = 0.;
        switch (op) {
        case EQUALS:
        	res = (double) _bucket[idx];
        	break;
        case LESS_THAN_OR_EQ:
        case GREATER_THAN_OR_EQ:
        	res = (double) _bucket[idx] / getWidth(idx);
        	break;
        case NOT_EQUALS:
        	return 1.0 - (double) _bucket[idx] / getWidth(idx);
        case LIKE:
        	return 1.0;
        default:
        	break;
        }
        switch (op) {
        case LESS_THAN:
        case LESS_THAN_OR_EQ:
        	for (int i = 0; i < idx; ++i) {
        		res += _bucket[i];
        	}
        	res += (double) (v - _bucketmin[idx]) / getWidth(idx) * _bucket[idx];
        	break;
        case GREATER_THAN:
        case GREATER_THAN_OR_EQ:
        	res += (double) (_bucketmax[idx] - v) / getWidth(idx) * _bucket[idx];
        	for (int i = idx + 1; i < _nbuckets; ++i) {
        		res += _bucket[i];
        	}
        	break;
        default:
        	break;
      }
      return res / _totn;
    	
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
