package simpledb;
import java.util.*;
import java.io.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

	private TransactionId _t;
	private DbIterator _child;
	private int _tableid;
	private Tuple _acounter;
	private boolean _done;
	
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        _t = t;
        _child = child;
        _tableid = tableid;
        _acounter = new Tuple(new TupleDesc(new Type[] {Type.INT_TYPE}));
        _done = false;
    }

    public TupleDesc getTupleDesc() {
        return _child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        _child.open();
        _done = false;
    }

    public void close() {
        _child.close();
        _done = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
        _done = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
    	if (_done)
    		return null;
    	
        int counter = 0;
        while (_child.hasNext()) {
        	Tuple t = _child.next();
        	try {
        		Database.getBufferPool().insertTuple(_t, _tableid, t);
            	counter++;
        	} catch (IOException e) {
        		DbException dbe = new DbException("IOErr in Insertion operation");
        		dbe.initCause(e);
        		throw dbe;
        	}
        	
        }
        
        _done = true;
        _acounter.setField(0, new IntField(counter));
        return _acounter;
    }
}
