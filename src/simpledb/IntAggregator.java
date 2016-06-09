package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

	private privateAggregator _aggregator;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (gbfield != NO_GROUPING) {
        	_aggregator = new GroupingAggregator(gbfield, gbfieldtype, afield, what);
        } else {
        	_aggregator = new NoGroupingAggregator(gbfield, gbfieldtype, afield, what);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
    	_aggregator.merge(tup);
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
        return _aggregator.iterator();
    }
    
    private abstract class privateAggregator {
    	
    	protected int _gbfield;
    	protected Type _gbtype;
    	protected int _afield;
    	protected Op _op;
    	protected int _defaultvalue;
    	
    	public privateAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    		_gbfield = gbfield;
    		_gbtype = gbfieldtype;
    		_afield = afield;
    		_op = what;
    		
    		switch (what) {
    		case MIN:
    			_defaultvalue = Integer.MAX_VALUE;
    			break;
    		case MAX:
    			_defaultvalue = Integer.MIN_VALUE;
    			break;
    		default:
    			_defaultvalue = 0;
    		}
    	}
    	
    	public abstract void merge(Tuple tup);
    	public abstract DbIterator iterator();
    	
    	protected Integer[] mergePrivate(Integer v, Integer v1, Integer v2) {
    		switch (_op) {
    		case MIN:
    			v1 = Math.min(v1, v);
    			break;
    		case MAX:
    			v1 = Math.max(v1, v);
    			break;
    		case SUM:
    			v1 += v;
    			break;
    		case COUNT:
    			v1++;
    			break;
    		case AVG:
    			v1 += v;
    			v2++;
    			break;
    		}
    		
    		return new Integer[] {v1, v2};
    	}
    	
    }

    private class GroupingAggregator extends privateAggregator {
    	
    	// for AVG, need to maintain a counter
    	private Map<Field, Integer> _values1, _values2;
    	
    	public GroupingAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    		super(gbfield, gbfieldtype, afield, what);
        	_values1 = new HashMap<Field, Integer>();
        	_values2 = new HashMap<Field, Integer>();
    	}
    	
    	public void merge(Tuple tup) {
    		int value = ((IntField) tup.getField(_afield)).getValue();
    		Field gb = tup.getField(_gbfield);
    		
    		Integer v1 = _values1.containsKey(gb) ? _values1.get(gb) : _defaultvalue;
    		Integer v2 = _values2.containsKey(gb) ? _values2.get(gb) : _defaultvalue;
    		
    		Integer[] newvalues = mergePrivate(value, v1, v2);
    		_values1.put(gb, newvalues[0]);
    		_values2.put(gb, newvalues[1]);
    	}
    	
    	public DbIterator iterator() {
    		return new GroupingAIterator(this);
    	}
    	
    }
    
    private class NoGroupingAggregator extends privateAggregator {
    	
    	// no need for map in no groupping
    	int _values1, _values2;
    	
    	public NoGroupingAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    		super(gbfield, gbfieldtype, afield, what);
        	_values1 = _defaultvalue;
        	_values2 = _defaultvalue;
    	}
    	
    	public void merge(Tuple tup) {
    		int value = ((IntField) tup.getField(_afield)).getValue();
    		Integer[] newvalues = mergePrivate(value, _values1, _values2);
    		_values1 = newvalues[0];
    		_values2 = newvalues[1];
    	}
    	
    	public DbIterator iterator() {
    		return new NoGroupingAIterator(this);
    	}
    }
    
    private abstract class AbstractADbIterator extends AbstractDbIterator {
    	
    	protected int computeNext(int v1, int v2, Op what) {
    		int value = 0;
    		
    		switch (what) {
    		case MAX:
    		case MIN:
    		case SUM:
    		case COUNT:
    			value = v1;
    			break;
    		case AVG:
    			value = v1 / v2;
    			break;
    		}
    		
    		return value;
    	}
    }

    private class GroupingAIterator extends AbstractADbIterator {
    	
    	private GroupingAggregator _ga;
    	private TupleDesc _td;
    	// iterator over groupby field
    	private Iterator<? extends Field> _gbfields;
    	
    	public GroupingAIterator(GroupingAggregator a) {
    		_ga = a;
    		_td = new TupleDesc(new Type[] {_ga._gbtype, Type.INT_TYPE});
    	}
    	
    	protected Tuple readNext() throws DbException, TransactionAbortedException {
    		Tuple t = new Tuple(_td);
    		
    		if (_gbfields.hasNext()) {
    			Field gbfield = _gbfields.next();
    			int value = computeNext(_ga._values1.get(gbfield),
    									_ga._values2.get(gbfield),
    									_ga._op);
    			t.setField(0, gbfield);
    			t.setField(1, new IntField(value));
    			return t;
    		}
    		
    		return null;
    	}
    	
    	public TupleDesc getTupleDesc() {
    		return _td;
    	}
    	
    	public void rewind() throws DbException, TransactionAbortedException {
    		open();
    	}
    	
    	public void open() throws DbException, TransactionAbortedException {
    		_gbfields = _ga._values1.keySet().iterator();
    	}
    }
    
    private class NoGroupingAIterator extends AbstractADbIterator {
    	
    	private NoGroupingAggregator _nga;
    	private TupleDesc _td;
    	private boolean _iterated;
    	
    	public NoGroupingAIterator(NoGroupingAggregator a) {
    		_nga = a;
    		_td = new TupleDesc(new Type[]{Type.INT_TYPE});
    		_iterated = false;
    	}
    	
    	protected Tuple readNext() throws DbException, TransactionAbortedException {
    		Tuple t = new Tuple(_td);
    		
    		if (!_iterated) {
    			int value = computeNext(_nga._values1, _nga._values2, _nga._op);
    			
    			t.setField(0, new IntField(value));
    			_iterated = true;
    			return t;
    		}
    		
    		return null;
    	}
    	
    	public TupleDesc getTupleDesc() {
    		return _td;
    	}
    	
    	public void rewind() throws DbException, TransactionAbortedException {
    		_iterated = false;
    	}
    	
    	public void open() throws DbException, TransactionAbortedException {
    		_iterated = false;
    	}
    }
}
