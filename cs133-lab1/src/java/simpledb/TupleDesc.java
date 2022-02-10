package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.NoSuchElementException;

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
    }

	private ArrayList<TDItem> TDs;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	Iterator<TDItem> iter = this.TDs.iterator();
        return iter;
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
        this.TDs = new ArrayList<TDItem>(typeAr.length); 
    	for (int i = 0; i < typeAr.length; i++) {
    		this.TDs.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
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
        this.TDs = new ArrayList<TDItem>(typeAr.length); 
    	for (int i = 0; i < typeAr.length; i++) {
    		this.TDs.add(new TDItem(typeAr[i], null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.TDs.size();
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
        if (i >= this.TDs.size()) {
        	throw new NoSuchElementException();
        }
        return this.TDs.get(i).fieldName;
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
    	if (i >= this.TDs.size()) {
        	throw new NoSuchElementException();
        }
        return this.TDs.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * No match if name is null.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        Iterator<TDItem> iter = this.iterator();
        for (int i = 0; iter.hasNext(); i++) {
        	String cur = iter.next().fieldName;
        	if (cur != null) {
            	if (cur.equals(name)) {
            		return i;
            	}
        	}
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     * @see Type#getSizeInBytes
     */
    public int getSizeInBytes() {
        int sizeByte = 0;
        Iterator<TDItem> iter = this.iterator();
        while (iter.hasNext()) {
        	sizeByte += iter.next().fieldType.getSizeInBytes();
        }
        
        return sizeByte;
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
        ArrayList<TDItem> mergeNames = new ArrayList<>();
        Type typeArr[] = new Type[td1.numFields() + td2.numFields()];
        String nameArr[] = new String[td1.numFields() + td2.numFields()];
        Iterator<TDItem> iter1 = td1.iterator();
        Iterator<TDItem> iter2 = td2.iterator();
        for (int i = 0; iter1.hasNext(); i++) {
        	TDItem cur = iter1.next();
        	typeArr[i] = cur.fieldType;
        	nameArr[i] = cur.fieldName;
        }
        for (int i = td1.numFields(); iter2.hasNext(); i++) {
        	TDItem cur = iter2.next();
        	typeArr[i] = cur.fieldType;
        	nameArr[i] = cur.fieldName;
        }
        return new TupleDesc(typeArr, nameArr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i. It does not matter if the field names are equal.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o == null || !(o instanceof TupleDesc)) {
    		return false;
    	}
        if (this.numFields() != ((TupleDesc) o).numFields()) {
        	return false;
        } else {
        	Iterator<TDItem> iter1 = this.iterator();
            Iterator<TDItem> iter2 = ((TupleDesc) o).iterator();
            while (iter1.hasNext()) {
            	if (!(iter1.next().fieldType.equals(iter2.next().fieldType))) {
            		return false;
            	}
            }
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
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String outString = "";
        Iterator<TDItem> iter = this.iterator();
        while (iter.hasNext()) {
        	TDItem cur = iter.next();
        	outString += cur.fieldName;
        	outString += "(" + cur.fieldType + ")";
        	outString += ",";
        }
    	return outString;
    }
}
