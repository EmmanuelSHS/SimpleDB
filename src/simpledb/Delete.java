package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

	private TransactionId _t;
	private DbIterator _child;
	private Tuple _dcounter;
	private boolean _done;

	/**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        _t = t;
        _child = child;
        _dcounter = new Tuple(new TupleDesc(new Type[] {Type.INT_TYPE}));
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
    	if (_done)
    		return null;
    	
    	int counter = 0;
        while (_child.hasNext()) {
        	Tuple t = _child.next();
        	Database.getBufferPool().deleteTuple(_t, t);
        	counter++;
        }
        
        _done = true;
        _dcounter.setField(0, new IntField(counter));
        
        return _dcounter;
    }
}
