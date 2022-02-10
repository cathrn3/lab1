package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
	private TupleDesc td;
	private RandomAccessFile raf;

	/**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
	 * @throws FileNotFoundException 
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        try {
			this.raf = new RandomAccessFile(f, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = pid.getPageNumber();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
			this.raf.read(data, offset, BufferPool.getPageSize());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			return (Page) new HeapPage((HeapPageId) pid, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long totalBytes = this.file.length();
        long buffBytes = (long) BufferPool.getPageSize();
        return (int) Math.floor(totalBytes/buffBytes);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	int tableId = this.getId();
    	int numPages = this.numPages();
    	System.out.println("numpages");
    	System.out.println(numPages);
        return new HeapFileIterator(tid, tableId, numPages);
    }
    
    public class HeapFileIterator implements DbFileIterator {
    	
    	private TransactionId tid;
		private int tableId;
		private int pgNo;
		private Iterator<Tuple> tupleIter;
		private int numPages;
		private boolean isOpen = false;

		public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
    		this.tid = tid;
    		this.tableId = tableId;
    		this.numPages = numPages;
    	}
    	
    	public void open() {
    		this.pgNo = 0;
    		this.isOpen = true;
    		PageId pageId = new HeapPageId(this.tableId, this.pgNo);
    		Page page = null;
    		try {
				page = Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_WRITE);
			} catch (TransactionAbortedException | DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		this.tupleIter = ((HeapPage) page).iterator();
    		
    		
    	}
    	
    	public boolean hasNext() {
    		int tmpPgNo = this.pgNo;
    		boolean newIter = false;
    		Iterator<Tuple> tempIter = null;
    		System.out.println(this.pgNo);
    		if (!this.isOpen) {
    			throw new IllegalStateException();
    		}
    		while (tmpPgNo < this.numPages) {
    			if (!newIter && this.tupleIter.hasNext()) {
    				return true;
    			} 
    			else if (newIter && tempIter.hasNext()) {
    				return true;
    			}
    			else {
    				tmpPgNo++;
    				System.out.println(tmpPgNo);
    				if (tmpPgNo == this.numPages) {
    					return false;
    				}
    	    		PageId pageId = new HeapPageId(this.tableId, tmpPgNo);
    	    		Page page = null;
    	    		try {
    					page = Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_WRITE);
    				} catch (TransactionAbortedException | DbException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    	    		tempIter = ((HeapPage) page).iterator();
    				
    			}
    		}
    		return false;
    	}
    	
    	public Tuple next() {
    		// System.out.println(this.pgNo);
    		// System.out.println(this.numPages);
    		if (!this.isOpen) {
    			throw new IllegalStateException();
    		}
    		while (this.pgNo < this.numPages) {
    			if (this.tupleIter.hasNext()) {
    				return tupleIter.next();
    			}
    			else {
    				this.pgNo++;
    				if (this.pgNo == this.numPages) {
    					throw new NoSuchElementException();
    				}
    	    		PageId pageId = new HeapPageId(this.tableId, this.pgNo);
    	    		Page page = null;
    	    		try {
    					page = Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_WRITE);
    				} catch (TransactionAbortedException | DbException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    	    		this.tupleIter = ((HeapPage) page).iterator();
    			}
    		}
    		throw new NoSuchElementException();
    	}
    	
    	public void rewind() {
    		this.open();
    	}
    	
    	public void close() {
    		this.isOpen = false;
    	}
    }

}

