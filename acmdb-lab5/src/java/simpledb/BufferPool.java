package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private ConcurrentHashMap<PageId, Page> pageMap;
    private final Random generator;

    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageMap = new ConcurrentHashMap<>();
        this.generator = new Random();
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
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
        // some code goes here
        if(perm == Permissions.READ_ONLY)
            lockManager.acquireLock(pid, tid, LockType.SHARED);
        else{
            assert perm == Permissions.READ_WRITE;
            lockManager.acquireLock(pid, tid, LockType.EXCLUSIVE);
        }
        if(pageMap.containsKey(pid))
            return pageMap.get(pid);
        if(pageMap.size() == numPages)
            evictPage();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);
        pageMap.put(pid, page);
        return pageMap.get(pid);
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
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> holds = lockManager.getHoldsLock(tid);
        if(holds == null) return;
        if(commit)
            flushPages(tid);
        else{
            for(PageId pid : holds){
                Page page = pageMap.get(pid);
                if(page != null){
                    Page p = page.getBeforeImage();
                    assert p != null;
                    pageMap.put(pid, p);
                }
            }
        }
        lockManager.releaseAllLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPageAr = f.insertTuple(tid, t);
        for (Page dirtyPage : dirtyPageAr){
            dirtyPage.markDirty(true, tid);
            PageId pid = dirtyPage.getId();
            if(!pageMap.containsKey(pid) && pageMap.size() == numPages){
                evictPage();
            }
            pageMap.put(pid, dirtyPage);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        assert t.getRecordId() != null;
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPageAr = f.deleteTuple(tid, t);
        for (Page dirtyPage : dirtyPageAr){
            dirtyPage.markDirty(true, tid);
            PageId pid = dirtyPage.getId();
            if(!pageMap.containsKey(pid) && pageMap.size() == numPages){
                evictPage();
            }
            pageMap.put(pid, dirtyPage);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pageMap.keySet())
            flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(!pageMap.containsKey(pid)) return;
        Page page = pageMap.get(pid);
        if(page.isDirty() != null){
            int tableId = pid.getTableId();
            DbFile f = Database.getCatalog().getDatabaseFile(tableId);
            f.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> holds = lockManager.getHoldsLock(tid);
        if(holds == null) return;
        for(PageId pid : holds){
            Page page = pageMap.get(pid);
            if(page != null && page.isDirty() != null){
                assert page.isDirty() == tid;
                flushPage(pid);
                page.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        ArrayList<PageId> KeyAr = new ArrayList<>(pageMap.keySet());
        int size = KeyAr.size();
        int first = generator.nextInt(size);
        for(int i=0; i<size; ++i) {
            int index = (first + i) % size;
            PageId pid = KeyAr.get(index);
            if(pageMap.get(pid).isDirty() != null) continue;
            try{
                flushPage(pid);
            }catch (IOException e){
                throw new DbException("IOException throw by BufferPool evictPage()");
            }
            pageMap.remove(pid);
            return;
        }
        throw new DbException("No clean page throw by BufferPool evictPage");
    }
}
