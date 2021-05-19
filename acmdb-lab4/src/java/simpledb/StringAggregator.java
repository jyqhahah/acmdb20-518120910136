package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final int afield;
    private final TupleDesc tupleDesc;
    private final Op what;
    private final HashMap<Field, Integer> fieldMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT)
            throw new IllegalArgumentException("IllegalArgumentException throw by StringAggregator constructor");
        this.gbfield = gbfield;
        if(gbfield !=  NO_GROUPING)
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"group", "no_group"});
        else
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"no_group"});
        this.afield = afield;
        this.what = what;
        this.fieldMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        if(gbfield != NO_GROUPING)
            key = tup.getField(gbfield);
        else
            key = new IntField(0);
        if(fieldMap.containsKey(key)) {
            int value = fieldMap.get(key) + 1;
            fieldMap.put(key, value);
        }
        else
            fieldMap.put(key, 1);
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
        // some code goes here
        ArrayList<Tuple> tpList = new ArrayList<>();
        if(gbfield != NO_GROUPING){
            for(Map.Entry<Field, Integer> entry : fieldMap.entrySet()){
                Tuple tmpTuple = new Tuple(tupleDesc);
                tmpTuple.setField(0, entry.getKey());
                tmpTuple.setField(1, new IntField(entry.getValue()));
                tpList.add(tmpTuple);
            }
        }
        else{
            Tuple tmpTuple = new Tuple(tupleDesc);
            tmpTuple.setField(0, new IntField(fieldMap.get(new IntField(0))));
            tpList.add(tmpTuple);
        }
        return new TupleIterator(tupleDesc, tpList);
    }

}
