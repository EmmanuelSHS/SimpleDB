package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    /**
     * Page capacity in # pages
     */
    private int _maxPages;
    private Map<PageId, Page> _bufferPages;
    private Map<PageId, Integer> _lfu; // least frequently used algo
    // TODO: dirty page mgr may need to be depreciated
    // private Map<TransactionId, Set<PageId>> _dirtyPageMgr;
    private AtomicInteger _currPages;
    private LockManager _lockMgr; // locking mgr

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        _maxPages = numPages;
        _bufferPages = new HashMap<PageId, Page>();
        _lfu = new HashMap<PageId, Integer>();
        //_dirtyPageMgr = new HashMap<TransactionId, Set<PageId>>();
        _currPages = new AtomicInteger(0);
        _lockMgr = LockManager.create();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        _lockMgr.acquireLock(tid, pid, perm);
    	
        //addToDPMgr(tid, pid);
        Page page = _bufferPages.get(pid);
    	if (page != null) {
    		// lfu
        	increaseLFU(pid);
    		return page;
    	} else {
    		if (_currPages.get() >= _maxPages) {
    			evictPage();	
    		}
    		Page newPage = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
    		_bufferPages.put(pid, newPage);
    		_currPages.incrementAndGet();
    		// lfu
        	increaseLFU(pid);
    		return newPage;
    	}
    }
    /*
    private void addToDPMgr(TransactionId tid, PageId pid) {
        if (_dirtyPageMgr.containsKey(tid)) {
        	_dirtyPageMgr.get(tid).add(pid);
          } else {
            Set<PageId> dirtypages = new HashSet<PageId>();
            dirtypages.add(pid);
            _dirtyPageMgr.put(tid, dirtypages);
          }
    }
    
    private void dropFromDPMgr(TransactionId tid) {
    	_dirtyPageMgr.remove(tid);
    }
	*/
    private void increaseLFU(PageId pid) {
    	int count = _lfu.containsKey(pid) ? _lfu.get(pid) : 0;
    	_lfu.put(pid, ++count);
    }
    
    private void deleteLFU(PageId pid) {
    	if (_lfu.containsKey(pid)) {
    		_lfu.remove(pid);
    	}
    }
    
    private Iterator<PageId> sortLFU() {
        List<Map.Entry<PageId,Integer>> list=new ArrayList<>();  
        list.addAll(_lfu.entrySet()); 
        ValueComparator vc = new ValueComparator();
        Collections.sort(list, vc);
        
        return _lfu.keySet().iterator();
    }
    
    /**
     * Comparator for sorting lest frequently used hash map by value
     * @author Emmanuel-PC
     * @param m, n two map entries
     */
    private class ValueComparator implements Comparator<Map.Entry<PageId,Integer>> {
        public int compare(Map.Entry<PageId,Integer> m, Map.Entry<PageId,Integer> n)  
        {  
            return m.getValue() - n.getValue();  
        }  
    }
    
    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        _lockMgr.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
        return _lockMgr.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	// NO STEAL, FORCE
    	try {
    		if (commit) {
    			commitTransaction(tid);
    		} else {
    			abortTransaction(tid);
    		}
    	} finally {
    		_lockMgr.releasePages(tid);
    		//dropFromDPMgr(tid);
    	}
    }
    
    private synchronized void commitTransaction(TransactionId tid) throws IOException {
    	for (PageId pid : _bufferPages.keySet()) {
    		Page p = _bufferPages.get(pid);
    		TransactionId ptid = p.isDirty();
    		if (ptid != null && ptid.equals(tid)) {
    			flushPage(pid);
    			p.markDirty(false, tid);
    			// use current page contents as the before-image
    			// for the next transaction that modifies this page.
    			p.setBeforeImage();
    		}
    	}
    }
    
    private synchronized void abortTransaction(TransactionId tid) throws IOException {
    	for (PageId pid : _bufferPages.keySet()) {
    		Page p = _bufferPages.get(pid);
    		TransactionId ptid = p.isDirty();
    		if (ptid != null && ptid.equals(tid)) {
    			p.markDirty(false, null);
    			_bufferPages.put(pid, p.getBeforeImage());
    			//p.setBeforeImage();
    		}
    	}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
        DbFile dbfile = getDbFile(tableId);
        // locking not required 
        Page p = dbfile.addTuple(tid, t).get(0);
        p.markDirty(true, tid);
        
        // LFU update
        increaseLFU(p.getId());
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        
    	DbFile dbfile = getDbFile(t.getRecordId().getPageId().getTableId());
    	Page p = dbfile.deleteTuple(tid, t);
    	p.markDirty(true, tid);
    	// lfu
    	increaseLFU(p.getId());
    }

    private DbFile getDbFile(int tableid) {
    	return Database.getCatalog().getDbFile(tableid);
    }
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	//
        Iterator<PageId> iter = _bufferPages.keySet().iterator();
        while (iter.hasNext()) {
        	flushPage(iter.next());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        _bufferPages.remove(pid);
        _currPages.decrementAndGet();
    }

    /**
     * Flushes a certain page to disk, flush != delete from buffer
     * Do not remove dirty flag when flush
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page p = _bufferPages.get(pid);
        if (p != null && p.isDirty() != null) {
        	Database.getLogFile().logWrite(p.isDirty(), p.getBeforeImage(), p);
        	Database.getLogFile().force();
        	
        	Database.getCatalog().getDbFile(pid.getTableId()).writePage(p);
        	p.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : _bufferPages.keySet()) {
        	Page p = _bufferPages.get(pid);
            TransactionId pagetid = p.isDirty();
            if (pagetid != null && pagetid.equals(tid)) {
                flushPage(pid);
                // use current page contents as the before-image
                // for the next transaction that modifies this page.
                p.setBeforeImage();
                p.markDirty(false, null);
            }
        }
    }
    
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * NO STEAL
     */
    private synchronized  void evictPage() throws DbException {
    	// TODO: what happened to this LFU ?
    	// TODO: observed that TransactionTest systemtest total time is indefinitive
    	/*
    	Iterator<PageId> slfu = sortLFU();
    	while (slfu.hasNext()) {
    		PageId pid = slfu.next();
    		if (_bufferPages.get(pid).isDirty() == null) {
    	        _bufferPages.remove(pid);
    	        // delete lfu rec
    	        deleteLFU(pid);
    	        _currPages.decrementAndGet();
    	        return;
    		}
    	}
    	*/
    	
    	for (PageId pid : _bufferPages.keySet()) {
    		Page p = _bufferPages.get(pid);
    		if (p.isDirty() == null) {
    			_bufferPages.remove(pid);
    			deleteLFU(pid);
    			_currPages.decrementAndGet();
    			return;
    		}
    	}
    	
    	
    	throw new DbException("All pages are dirty");
    }

}
