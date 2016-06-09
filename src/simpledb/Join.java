package simpledb;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends AbstractDbIterator {

	private JoinPredicate _p;
	private DbIterator _c1;
	private DbIterator _c2;
	private Tuple _dummyT1;
	
    /**
     * Constructor.  Accepts to children to join and the predicate
     * to join them on
     *
     * @param p The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        _p = p;
        _c1 = child1;
        _c2 = child2;
        _dummyT1 = null;
    }

    /**
     * @see simpledb.TupleDesc#combine(TupleDesc, TupleDesc) for possible implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.combine(_c1.getTupleDesc(), _c2.getTupleDesc());
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        _c1.open();
        _c2.open();
    }

    public void close() {
        _c1.close();
        _c2.close();
        _dummyT1 = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _c1.rewind();
        _c2.rewind();
        _dummyT1 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no more tuples.
     * Logically, this is the next tuple in r1 cross r2 that satisfies the join
     * predicate.  There are many possible implementations; the simplest is a
     * nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of
     * Join are simply the concatenation of joining tuples from the left and
     * right relation. Therefore, if an equality predicate is used 
     * there will be two copies of the join attribute
     * in the results.  (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
    	Tuple t2;
    	
        while (_dummyT1 != null || _c1.hasNext()) {
        	if (_dummyT1 == null)
        		_dummyT1 = _c1.next();
        	while (_c2.hasNext()) {
        		t2 = _c2.next();
        		if (_p.filter(_dummyT1, t2)) {
        			return TupleCombine(_dummyT1, t2);
        		}
        	}
        	_c2.rewind();
        	_dummyT1 = null;
        }
        return null;
    }
    
    private Tuple TupleCombine(Tuple t1, Tuple t2) {
		Tuple newTuple = new Tuple(getTupleDesc());
		
		int n1 = t1.getTupleDesc().numFields();
		int n2 = t2.getTupleDesc().numFields();
		
		for (int i = 0; i < n1 ; ++i) {
			newTuple.setField(i, t1.getField(i));
		}
		for (int j = 0; j < n2; ++j) {
			newTuple.setField(j + n1, t2.getField(j));
		}
		
		return newTuple;
    }
}