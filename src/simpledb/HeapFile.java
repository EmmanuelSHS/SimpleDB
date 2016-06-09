package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File _f;
	private TupleDesc _td;
	private int _countOldPages; // n pages after initialization
	private int _countNewPages; // n pages added after init.
	
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        _f = f;
        _td = td;
        _countOldPages = (int) (f.length() / BufferPool.PAGE_SIZE);
        _countNewPages = 0;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return _f;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return this._f.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        byte[] buf = new byte[BufferPool.PAGE_SIZE];
        FileInputStream fs = null;
        HeapPage p = null;
        
        try {
        	fs = new FileInputStream(_f);
        	fs.skip(pid.pageno() * BufferPool.PAGE_SIZE);
        	fs.read(buf);
        	fs.close();
        	
        	p = new HeapPage((HeapPageId) pid, buf);
        } catch(IOException e) {
        	e.printStackTrace();
        }
        
        return p;
    }

    // see DbFile.java for javadocs
    /**
     * rws flushes the contents of the file and the modification date of the file.
     * rwd flushs the contents of the file, but the modification date might not change until the file is closed.
     * rw only flushes when you tell it to and doesn't change the modifcation date until you close the file.
     * rwd is much slower for writes than rw, and rws is slower again.
     */
    public void writePage(Page page) throws IOException {
        RandomAccessFile diskf = new RandomAccessFile(_f, "rw");
        diskf.seek(page.getId().pageno() * BufferPool.PAGE_SIZE);
        diskf.write(page.getPageData());
        diskf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	// dynamic
    	return _countOldPages + _countNewPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        
    	for (int i = 0; i < numPages(); ++i) {
    		HeapPageId hpid = new HeapPageId(getId(), i); // table id = heap file id; page num
    		HeapPage p = getHeapPage(tid, hpid, Permissions.READ_WRITE);
    		if (p.getNumEmptySlots() > 0) {
    			p.addTuple(t);
    			ArrayList<Page> arr = new ArrayList<Page>();
    			arr.add(p);
    			
    			return arr;
    		}
    	}
    	
    	// else situation, tuple belongs to a new page
        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage p = getHeapPage(tid, pid, Permissions.READ_WRITE);
        _countNewPages++;
        p.addTuple(t);
        ArrayList<Page> arr = new ArrayList<Page>();
        arr.add(p);
        
        return arr;
    	
    }
    
    private HeapPage getHeapPage(TransactionId tid, PageId pid, Permissions perm)
    	throws TransactionAbortedException, DbException {
    	return (HeapPage) Database.getBufferPool().getPage(tid, pid, perm);
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // general case
    	if (!isInTable(t)) {
    		throw new DbException("Tuple does not belong to this table");
    	}
    	
    	HeapPage p = getHeapPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	p.deleteTuple(t);
    	return p;
    	
    	// delete might cause eviction of one whole page
    }
    
    private boolean isInTable(Tuple t) {
    	return getId() == t.getRecordId().getPageId().getTableId();
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
    
}

class HeapFileIterator implements DbFileIterator {
    
	private HeapFile _hf;
	private TransactionId _tid;
	private int _tableid;
	private int _pageno;
	private Iterator<Tuple> _pageTuples;
	
    HeapFileIterator(HeapFile hf, TransactionId tid) {
        _hf         = hf;
        _tid        = tid;
        _tableid    = hf.getId();
        _pageno     = 0;
    }
    
    public void open()
        throws DbException, TransactionAbortedException {
    	_pageTuples = openIterator();
    };
    
    private Iterator<Tuple> openIterator() throws DbException,
    	TransactionAbortedException {
        HeapPageId pid = new HeapPageId(_tableid, _pageno);
        HeapPage p = (HeapPage)Database.getBufferPool()
                                       .getPage(_tid, pid, Permissions.READ_ONLY);
        return p.iterator();
    }

    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        if (_pageTuples == null) {
            return false;
        } else if (_pageTuples.hasNext()) {
            return true;
        } else {

            _pageno++;
            if (_pageno < _hf.numPages()) {
                _pageTuples = openIterator();
                return _pageTuples.hasNext();
            } else {
                return false;
            }
        }
    };

    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext())
            return _pageTuples.next();
        else
            throw new NoSuchElementException();
    };

    public void rewind() throws DbException, TransactionAbortedException {
        _pageno = 0;
        _pageTuples = openIterator();
    };

    public void close() {
        _pageTuples = null;
    };
}
