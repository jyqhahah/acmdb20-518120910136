package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1, child2;
    private TupleDesc tupleDesc;
    private Tuple tuple;
    private final HashMap<Field, ArrayList<Tuple>> fieldMap = new HashMap<>();

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        assert(p.getOperator() == Predicate.Op.EQUALS);
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
	    return child1.getTupleDesc().getFieldName(p.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    private void buildMap() throws DbException, TransactionAbortedException {
        fieldMap.clear();
        Tuple tmpTuple;
        while(child1.hasNext()){
            if(fieldMap.size() >= 8192)
                break;
            tmpTuple = child1.next();
            Field field = tmpTuple.getField(p.getField1());
            if(!fieldMap.containsKey(field))
                fieldMap.put(field, new ArrayList<>());
            fieldMap.get(field).add(tmpTuple);
        }
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        buildMap();
        if(child2.hasNext())
            tuple = child2.next();
        else
            tuple = null;
        listIt = null;
        super.open();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        tuple = null;
        fieldMap.clear();
        listIt = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        buildMap();
        if(child2.hasNext())
            tuple = child2.next();
        else
            tuple = null;
        listIt = null;
        super.close();
        super.open();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(!fieldMap.isEmpty()){
            while(tuple != null){
                if(listIt == null){
                    Field field = tuple.getField(p.getField2());
                    if(fieldMap.containsKey(field))
                        listIt = fieldMap.get(field).listIterator();
                    else{
                        if(child2.hasNext())
                            tuple = child2.next();
                        else
                            tuple = null;
                        //listIt = null;
                        continue;
                    }
                }
                if(listIt.hasNext()){
                    Tuple tmpTuple = listIt.next();
                    int num1 = child1.getTupleDesc().numFields();
                    int num2 = child2.getTupleDesc().numFields();
                    Tuple nwTuple = new Tuple(tupleDesc);
                    for(int i = 0; i < num1; ++i)
                        nwTuple.setField(i, tmpTuple.getField(i));
                    for(int i = 0; i < num2; ++i)
                        nwTuple.setField(i+num1, tuple.getField(i));
                    return nwTuple;
                }
                else{
                    if(child2.hasNext())
                        tuple = child2.next();
                    else
                        tuple = null;
                    listIt = null;
                }
            }
            child2.rewind();
            buildMap();
            if(child2.hasNext())
                tuple = child2.next();
            else
                tuple = null;
            listIt = null;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
        tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    
}
