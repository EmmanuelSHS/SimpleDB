package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    private Aggregator _ator;
    private DbIterator _child;
    private DbIterator _ait;
    private TupleDesc _td;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        TupleDesc childtd = child.getTupleDesc();
        Type atype, gtype;

        _child = child;
        gtype = gfield == Aggregator.NO_GROUPING ? null : childtd.getType(gfield);
        _td = aggTD(gfield, gtype, childtd, afield, aop);

        atype = childtd.getType(afield);
        _ator = atype == Type.INT_TYPE ? new IntAggregator(gfield, gtype, afield, aop)
                                       : new StringAggregator(gfield, gtype, afield, aop);
    }

    private TupleDesc aggTD(int gfield, Type gtype, TupleDesc childtd, int afield, Aggregator.Op aop) {
        if (gfield == Aggregator.NO_GROUPING)
            return new TupleDesc(new Type[]{Type.INT_TYPE},
                                 new String[]{aggColumnName(childtd, afield, aop)});
        else
            return new TupleDesc(new Type[]{gtype, Type.INT_TYPE},
                                 new String[]{childtd.getFieldName(gfield),
                                              aggColumnName(childtd, afield, aop)});
    }

    private String aggColumnName(TupleDesc childtd, int afield, Aggregator.Op aop) {
        return aggName(aop) + "(" + childtd.getFieldName(afield) + ")";
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    private void aggregate()
        throws NoSuchElementException, DbException, TransactionAbortedException {

        _child.open();
        while (_child.hasNext())
            _ator.merge(_child.next());
        _child.close();
    }

    private void openInternal()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        
        _ait = _ator.iterator();
        _ait.open();
    }        

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        
        aggregate();
        openInternal();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (_ait.hasNext())
            return _ait.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        openInternal();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        return _td;
    }

    public void close() {
        _ait.close();
    }
}