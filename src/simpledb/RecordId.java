package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

	private PageId _pid;
	private int _tupleno;
    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this._pid = pid;
        this._tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return this._tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this._pid;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if (!(o instanceof RecordId)) {
    		return false;
    	}
    	
    	RecordId that = (RecordId) o;
    	
    	// PageId is an object, requiring equals operation
    	if (that.getPageId().equals(this.getPageId()) && this.tupleno() == that.tupleno()) {
    		return true;
    	}
    	
    	return false;
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	return _tupleno;
    	
    }
    
}
