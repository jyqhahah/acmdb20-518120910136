package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private DbIterator child;
    private final int tableId;
    private boolean isFetched;
    private final TupleDesc tupleDesc;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("different child tupleDesc throw by insert constructor");
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.isFetched = false;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"insert tuples"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        isFetched = false;
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        isFetched = false;
        child.rewind();
        super.close();
        super.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isFetched) return null;
        Tuple tuple; int cnt=0;
        while(child.hasNext()){
            tuple = child.next(); ++cnt;
            try {
                Database.getBufferPool().insertTuple(t, tableId, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple res = new Tuple(tupleDesc);
        res.setField(0, new IntField(cnt));
        isFetched = true;
        return res;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
