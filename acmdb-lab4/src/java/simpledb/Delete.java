package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private DbIterator child;
    private boolean isFetched;
    private final TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        this.isFetched = false;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"delete tuples"});
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isFetched) return null;
        Tuple tuple; int cnt=0;
        while(child.hasNext()){
            tuple = child.next(); ++cnt;
            if(cnt == 550) {
                int a = 1;
            }
            try {
                Database.getBufferPool().deleteTuple(t, tuple);
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
