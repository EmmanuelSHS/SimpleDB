package simpledb;

/**
 * Tuple maintains information about the contents of A-SINGLE tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {
	
	private RecordId rid;
	private TupleDesc descriptor;
	private Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // TODO: here by question, valid desc check is done, but could not be the case sometimes
    	descriptor = td;
    	fields = new Field[td.numFields()];
    	
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return descriptor;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = new RecordId(rid.getPageId(), rid.tupleno());
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	// no need to return null for ScanTest.java
    	
        //if (i >= descriptor.numFields()) {
    	//	return null;
    	//}
    	
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
    	String output = new String();
    
    	for (int i = 0; i < fields.length; ++i) {
    		output += fields[i].toString() + "\t";
    	}
    	output += "\n";
    	
    	return output;
    }
}
