package hw4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.Tuple;

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

    private Map<HeapPage, ReentrantReadWriteLock> bufferPool;
    private Map<Integer, Set<ReentrantReadWriteLock>> lockOwners;
    private Map<Integer, List<HeapPage>> trace;
    private int numPages;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    		this.numPages = numPages;
    		bufferPool = new HashMap<>(numPages);
    		lockOwners = new HashMap<>();
    		trace = new HashMap<>();
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
     * @param tableId the ID of the table with the requested page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
        throws Exception {
    		if(perm == null) {
    			throw new Exception("Invalid permission");
    		}
    		
    		HeapPage heapPage = lookUpHeapPage(tableId, pid);
    		if(heapPage == null) {
    			Catalog catalog = Database.getCatalog();
    			HeapFile heapFile = catalog.getDbFile(tableId);
        		heapPage = heapFile.readPage(pid);
        		addPageToBufferPool(heapPage);
    		}
    		
    		ReentrantReadWriteLock readWriteLock = bufferPool.get(heapPage);
    		Set<ReentrantReadWriteLock> locks = lockOwners.getOrDefault(tid, new HashSet<>());
    		
    		
    		if(Permissions.READ_ONLY == perm) {
    			
    			if(!readWriteLock.readLock().tryLock(20, TimeUnit.MILLISECONDS)) {
    				throw new Exception("Abort");
    			}
    			locks.add(readWriteLock);
    			lockOwners.put(tid, locks);
    			
    		}else if(Permissions.READ_WRITE == perm) {
    			if(!readWriteLock.isWriteLockedByCurrentThread()) {
    				releaseLock(readWriteLock);
    				if(!readWriteLock.writeLock().tryLock(20, TimeUnit.MILLISECONDS)) {
        				throw new Exception("Abort");
        			}
        			locks.add(readWriteLock);
        			lockOwners.put(tid, locks);
    			}
    		}else {
    			throw new Exception("Unknow permission");
    		}
    		return heapPage;
    }
    
    private HeapPage lookUpHeapPage(int tableId, int pid) {
    		for(HeapPage cur : bufferPool.keySet()) {
    			if(cur != null && checkSamePage(cur, tableId, pid)) {
    				return cur;
    			}
    		}
    		return null;
    }
    
    private boolean checkSamePage(HeapPage hp, int tableId, int pid) {
    		return hp != null && hp.getId() == pid && hp.getTableId() == tableId;
    }
    
    private void addPageToBufferPool(HeapPage heapPage) throws Exception {
    		if(bufferPool.size() >= numPages) {
    			evictPage();
    		}
    		bufferPool.put(heapPage, new ReentrantReadWriteLock());
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(int tid, int tableId, int pid) {
    		HeapPage heapPage = lookUpHeapPage(tableId, pid);
		Set<ReentrantReadWriteLock> locks = lockOwners.get(tid);
		ReentrantReadWriteLock lock = bufferPool.get(heapPage);
		if(locks.contains(lock)) {
			releaseLock(lock);
		}
    }
    
    private void releaseLock(ReentrantReadWriteLock lock) {
    		int readLockNumber = lock.getReadHoldCount();
    		int writeLockNumber = lock.getWriteHoldCount();
    		for(int i=0; i<readLockNumber; i++) {
    			lock.readLock().unlock();
    		}
    		for(int i=0; i<writeLockNumber; i++) {
    			lock.writeLock().unlock();
    		}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(int tid, int tableId, int pid) {
    		HeapPage heapPage = lookUpHeapPage(tableId, pid);
    		Set<ReentrantReadWriteLock> locks = lockOwners.get(tid);
    		
    		return locks != null && heapPage != null && locks.contains(bufferPool.get(heapPage));
    		
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. If the transaction wishes to commit, write
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(int tid, boolean commit)
        throws IOException {

		List<HeapPage> pages = trace.getOrDefault(tid, new ArrayList<>());
		Catalog catalog = Database.getCatalog();
    		
		if(commit) {
	    		for(HeapPage p : pages) {
	    			int tableId = p.getTableId();
	    			int pid = p.getId();
	    			flushPage(tableId, pid);
	    		}
		}
		
		for(HeapPage p : pages) {
			int tableId = p.getTableId();
			int pid = p.getId();
			HeapFile heapFile = catalog.getDbFile(tableId);
			HeapPage newPage = heapFile.readPage(pid);
			HeapPage oldPage = lookUpHeapPage(tableId, pid);
			ReentrantReadWriteLock readWriteLock = bufferPool.get(oldPage);
			bufferPool.remove(oldPage);
			bufferPool.put(newPage, readWriteLock);
		}
		
    		trace.remove(tid);
    		Set<ReentrantReadWriteLock> locks = lockOwners.get(tid);
    		for(ReentrantReadWriteLock lock : locks) {
    			releaseLock(lock);
    		}
    }
    

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(int tid, int tableId, Tuple t)
        throws Exception {
    	
    		Catalog catalog = Database.getCatalog();
		HeapFile heapFile = catalog.getDbFile(tableId);
		HeapPage heapPage = heapFile.addTuple(t);
		
		if(heapPage == null) {
			throw new Exception("No enough space to insert");
		}
		
		int pid = heapPage.getId();
		ReentrantReadWriteLock readWriteLock = bufferPool.get(heapPage);
		bufferPool.remove(heapPage);
		bufferPool.put(heapPage, readWriteLock);
		List<HeapPage> pages = trace.getOrDefault(tid, new ArrayList<>());
		pages.add(heapPage);
		trace.put(tid, pages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t the tuple to add
     */
    public void deleteTuple(int tid, int tableId, Tuple t)
        throws Exception {
    		Catalog catalog = Database.getCatalog();
		HeapFile heapFile = catalog.getDbFile(tableId);
		HeapPage heapPage = heapFile.deleteTupleWithReturn(t);
		if(heapPage == null) {
			throw new Exception("No such tuple");
		}
		ReentrantReadWriteLock readWriteLock = bufferPool.get(heapPage);
		bufferPool.remove(heapPage);
		bufferPool.put(heapPage, readWriteLock);
		List<HeapPage> pages = trace.getOrDefault(tid, new ArrayList<>());
		pages.add(heapPage);
		trace.put(tid, pages);
    }

    private synchronized void flushPage(int tableId, int pid) throws IOException {
    		Catalog catalog = Database.getCatalog();
        HeapPage heapPage = lookUpHeapPage(tableId, pid);
        HeapFile heapFile = catalog.getDbFile(tableId);
        heapFile.writePage(heapPage);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws Exception {
    		for(HeapPage p : bufferPool.keySet()) {
    			ReentrantReadWriteLock readWriteLock = bufferPool.get(p);
    			if(!readWriteLock.isWriteLocked() && readWriteLock.getReadLockCount() == 0) {
    				bufferPool.remove(p);
    				return ;
    			}
    		}
    }

}
