package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     * */

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(TDItem tdItem)
        {
            return (this.fieldName.equals(tdItem.fieldName) && this.fieldType == tdItem.fieldType);
        }
    }

    private ArrayList<TDItem> tdItemList = null;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert typeAr.length == fieldAr.length;
        this.tdItemList = new ArrayList<TDItem>(typeAr.length);
        for(int i=0; i<typeAr.length; ++i)
            this.tdItemList.add(new TDItem(typeAr[i], fieldAr[i]));
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.tdItemList = new ArrayList<TDItem>(typeAr.length);
        for(Type type : typeAr)
            this.tdItemList.add(new TDItem(type, "anonymous"));
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tdItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= numFields())
            throw new NoSuchElementException("NoSuchElementException throw by getFieldName()");
        return this.tdItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= numFields())
            throw new NoSuchElementException("NoSuchElementException throw by getFieldType()");
        return this.tdItemList.get(i).fieldType;
    }

    /**
     * Gets the ith TDItem of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public TDItem getTDItem(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= numFields())
            throw new NoSuchElementException("NoSuchElementException throw by getTDItem()");
        return this.tdItemList.get(i);
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i = 0; i < tdItemList.size(); ++i){
            if(tdItemList.get(i).fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException("NoSuchElementException throw by fieldNameToIndex()");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int ret = 0;
        for(TDItem tdItem : tdItemList)
            ret += tdItem.fieldType.getLen();
        return ret;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int mergeSize = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[mergeSize];
        String[] fieldAr = new String[mergeSize];
        for(int i = 0; i < td1.numFields(); ++i){
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }
        for(int i = 0; i < td2.numFields(); ++i){
            typeAr[i+td1.numFields()] = td2.getFieldType(i);
            fieldAr[i+td1.numFields()] = td2.getFieldName(i);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc))
            return false;
        TupleDesc td_o = (TupleDesc)o;
        if(td_o.numFields() != this.numFields())
            return false;
        for(int i = 0; i < td_o.numFields(); ++i){
            if(!td_o.getTDItem(i).equals(this.getTDItem(i)))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        if(this.numFields() > 0)
            builder.append(this.getFieldType(0).toString()).append("(").append(this.getFieldName(0)).append(")");
        for(int i = 1; i < this.numFields(); ++i)
        {
            builder.append(", ").append(this.getFieldType(i).toString()).append("(").append(this.getFieldName(i)).append(")");
        }
        return builder.toString();
    }
}
