package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private int _count;
	private Map<Field, Integer> _counts;
	private int _gb;
	private Type _gbtype;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
        	throw new IllegalArgumentException();
        } else {
        	_gb = gbfield;
        	_gbtype = gbfieldtype;
        	_count = 0;
        	_counts = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        if (_gb == NO_GROUPING) {
        	_count++;
        } else {
        	Field f = tup.getField(_gb);
        	Integer c = _counts.get(f);
        	if (c == null) {
        		c = 1;
        	} else {
        		c++;
        	}
        	_counts.put(f, c);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new AbstractDbIterator() {
        	
        	private Iterator<Field> _it;
        	private boolean _done;
        	private TupleDesc _td;
        	
        	{
        		if (_gb == NO_GROUPING) {
        			_td = new TupleDesc(new Type[] {Type.INT_TYPE});
        			_done = false;
        		} else {
        			_td = new TupleDesc(new Type[] {_gbtype, Type.INT_TYPE});
        			_it = _counts.keySet().iterator();
        		}
        	}
        	
        	protected Tuple readNext() throws DbException, TransactionAbortedException {
        		Tuple t = new Tuple(_td);
        		
        		if (_gb == NO_GROUPING) {
        			if (!_done) {
        				t.setField(0, new IntField(_count));
        				_done = true;
        				return t;
        			}
        			return null;
        		} else {
        			if (_it.hasNext()) {
        				Field f = _it.next();
        				t.setField(0, f);
        				t.setField(1, new IntField(_counts.get(f)));
        				return t;
        			}
        			return null;
        		}
        	}
        
        	public TupleDesc getTupleDesc() {
        		return _td;
        	}
        	
        	public void rewind() throws DbException, TransactionAbortedException {
        		open();
        	}
        	
        	public void open() throws DbException, TransactionAbortedException {
        		if (_gb == NO_GROUPING) {
        			_done = false;
        		} else {
        			_it = _counts.keySet().iterator();
        		}
        	}
        
        };
    }

}
