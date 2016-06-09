package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

	private Predicate _p;
	private DbIterator _child;
	private TupleDesc _td;
	
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this._p = p;
        this._child = child;
        this._td = this._child.getTupleDesc();
    }

    public TupleDesc getTupleDesc() {
        return _td;
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        _child.open();
    }

    public void close() {
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        while (_child.hasNext()) {
        	Tuple t = _child.next();
        	if (_p.filter(t)) {
        		return t;
        	}
        }
        return null;
    }
}
