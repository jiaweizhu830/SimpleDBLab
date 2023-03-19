package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private Map<PageId, Page> pageMap;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here (OK)
        this.numPages = numPages;
        pageMap = new HashMap<>(this.numPages);
        lockManager = new LockManager();
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire a
     * lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is present,
     * it should be returned. If it is not present, it should be added to the buffer
     * pool and returned. If there is insufficient space in the buffer pool, a page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: some code goes here (OK)
        Page page = pageMap.get(pid);
        if (!lockManager.acquireLock(tid, pid, perm)) {
            throw new TransactionAbortedException();
        }
        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);

            // evict page from buffer pool and flush to disk if buffer pool is occupied
            if (pageMap.size() > numPages) {
                try {
                    evictPage();
                } catch (IOException ex) {
                    return null;
                }
            }
            pageMap.put(pid, page);
        }
        return page;
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result in
     * wrong behavior. Think hard about who needs to call this and why, and why they
     * can run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here (OK)
        // not necessary for lab1|lab2

        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here (OK)
        // not necessary for lab1|lab2

        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here (OK)
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here (OK)
        // not necessary for lab1|lab2

        if (commit) {
            // commit
            try {
                // flush committed pages to disk
                flushPages(tid);
            } catch (IOException ex) {
            }
        } else {
            // abort
            // restore the page in buffer pool to its on-disk state
            Set<PageId> pids = lockManager.getPageIdsFromTransactionId(tid);

            for (PageId pid : pids) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                pageMap.put(pid, dbFile.readPage(pid));
            }
        }

        // release lock
        lockManager.releaseLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will acquire
     * a write lock on the page the tuple is added to and any other pages that are
     * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
     * cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here (OK)
        // not necessary for lab1

        // TODO: add lock on modified page
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        if (file == null) {
            return;
        }

        List<Page> dirtyPages = file.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write lock on
     * the page the tuple is removed from and any other pages that are updated. May
     * block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling their
     * markDirty bit, and adds versions of any pages that have been dirtied to the
     * cache (replacing any existing versions of those pages) so that future
     * requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here (OK)
        // not necessary for lab1

        // TODO: add lock on modified page
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        if (file == null) {
            return;
        }

        List<Page> dirtyPages = file.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
     * dirty data to disk so will break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here (OK)
        // not necessary for lab1
        for (PageId pid : pageMap.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages are removed from the
     * cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here (OK)
        // not necessary for lab1
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here (OK)
        // not necessary for lab1

        // Write dirty page to disk, mark it as not dirty
        Page page = pageMap.get(pid);
        if (page == null)
            return;

        TransactionId tid = page.isDirty();
        if (tid != null) {
            HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            if (file == null) {
                return;
            }

            file.writePage(page);
            page.markDirty(false, tid);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here (OK)
        // not necessary for lab1|lab2

        Set<PageId> pids = lockManager.getPageIdsFromTransactionId(tid);
        for (PageId pid : pids) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     * 
     * No Steal -> not evict dirty page. If the transaction aborts, the dirty page
     * should be cleaned
     */
    private synchronized void evictPage() throws DbException, IOException {
        // TODO: some code goes here (OK)
        // not necessary for lab1

        for (Map.Entry<PageId, Page> entry : pageMap.entrySet()) {
            PageId pid = entry.getKey();
            Page p = entry.getValue();
            if (p.isDirty() == null) {
                flushPage(pid);
                removePage(pid);
                return;
            }
        }

        throw new DbException("All pages in Buffer Pool are dirty. Cannot evict any pages.");
    }

}
