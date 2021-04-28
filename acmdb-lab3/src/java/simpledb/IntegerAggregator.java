package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private class Pair{
        public int first;
        public int second;

        public Pair(int first, int second){
            this.first = first;
            this.second = second;
        }
    }

    private final int gbfield;
    private final int afield;
    private final TupleDesc tupleDesc;
    private final Op what;
    private final HashMap<Field, Pair> fieldMap;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        if(gbfield != NO_GROUPING)
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"group", "no_group"});
        else
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"no_group"});
        this.afield = afield;
        this.what = what;
        this.fieldMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(afield), key;
        int value = ((IntField)field).getValue();
        if(gbfield != NO_GROUPING)
            key = tup.getField(gbfield);
        else
            key = new IntField(0);
        if(fieldMap.containsKey(key)){
            Pair pair = fieldMap.get(key);
            if(what == Op.MIN){
                if(value < pair.first) pair.first = value;
            }
            else if (what == Op.MAX){
                if(value > pair.first) pair.first = value;
            }
            else if (what == Op.SUM){
                pair.first += value;
            }
            else if (what == Op.AVG){
                pair.first += value; pair.second += 1;
            }
            else{
                pair.first += 1;
            }
        }
        else{
            Pair pair;
            if(what == Op.MIN || what == Op.MAX || what == Op.SUM){
                pair = new Pair(value, 0);
            }
            else if(what == Op.AVG){
                pair = new Pair(value, 1);
            }
            else{
                pair = new Pair(1, 0);
            }
            fieldMap.put(key, pair);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tpList = new ArrayList<>();
        if(gbfield != NO_GROUPING){
            for(Map.Entry<Field, Pair> entry : fieldMap.entrySet()){
                Tuple tmpTuple = new Tuple(tupleDesc);
                tmpTuple.setField(0, entry.getKey());
                if(what == Op.AVG)
                    tmpTuple.setField(1, new IntField(entry.getValue().first / entry.getValue().second));
                else
                    tmpTuple.setField(1, new IntField(entry.getValue().first));
                tpList.add(tmpTuple);
            }
        }
        else{
            Tuple tmpTuple = new Tuple(tupleDesc);
            if(what == Op.AVG)
                tmpTuple.setField(0, new IntField(fieldMap.get(new IntField(0)).first / fieldMap.get(new IntField(0)).second));
            else
                tmpTuple.setField(0, new IntField(fieldMap.get(new IntField(0)).first));
            tpList.add(tmpTuple);
        }
        return new TupleIterator(tupleDesc, tpList);
    }

}
