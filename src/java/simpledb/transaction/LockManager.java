package simpledb.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {
    private Map<PageId, List<LockManager.Lock>> lockMap;
    private Map<TransactionId, Set<PageId>> txMap;

    public LockManager() {
        lockMap = new HashMap<>();
        txMap = new HashMap<>();
    }

    public synchronized boolean acquireLock(TransactionId tid, PageId p, Permissions permission) {
        // No lock on the page
        if (lockMap.get(p) == null) {
            List<LockManager.Lock> locks = new ArrayList<>();
            locks.add(new LockManager.Lock(tid, permission));
            lockMap.put(p, locks);

            Set<PageId> pages = txMap.get(tid);
            if (pages == null) {
                pages = new HashSet<>();
            }
            pages.add(p);
            txMap.put(tid, pages);
            return true;
        }

        List<LockManager.Lock> locks = lockMap.get(p);

        // return true if the same transaction acquires the same type of lock
        for (LockManager.Lock lock : locks) {
            if (lock.tid.equals(tid) && lock.permission.equals(permission)) {
                return true;
            }
        }

        // Page has exclusive lock already
        if (locks.size() == 1 && locks.get(0).permission.equals(Permissions.READ_WRITE)) {
            // The same ransaction can acquire a shared lock after exclusive lock
            if (locks.get(0).tid.equals(tid) && permission.equals(Permissions.READ_ONLY)) {
                locks.add(new LockManager.Lock(tid, permission));
                lockMap.put(p, locks);
                return true;
            }
            return false;
        }

        // Page has shared lock already
        if (permission.equals(Permissions.READ_WRITE)) {
            // upgrade shared lock to exclusive lock for the given transaction
            if (locks.size() == 1 && locks.get(0).permission.equals(Permissions.READ_ONLY)
                    && locks.get(0).tid.equals(tid)) {
                locks.set(0, new LockManager.Lock(tid, permission));
                lockMap.put(p, locks);
                return true;
            }
            // can NOT acquire exclusive lock on page that already has shared lock
            return false;
        } else {
            // can acquire shared lock on page that already has shared lock
            locks.add(new LockManager.Lock(tid, permission));
            lockMap.put(p, locks);

            Set<PageId> pages = txMap.get(tid);
            if (pages != null) {
                pages.add(p);
                txMap.put(tid, pages);
            }
            return true;
        }
    }

    public synchronized boolean releaseLock(TransactionId tid) {
        Set<PageId> pages = txMap.get(tid);

        if (pages == null)
            return true;

        for (PageId pid : pages) {
            if (!releaseLock(tid, pid)) {
                return false;
            }
        }

        return true;
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        List<LockManager.Lock> locks = lockMap.get(pid);
        if (locks == null)
            return true;

        List<LockManager.Lock> updatedLocks = new ArrayList<>();
        for (LockManager.Lock lock : locks) {
            if (!lock.tid.equals(tid)) {
                updatedLocks.add(lock);
            }
        }
        if (updatedLocks.isEmpty()) {
            lockMap.remove(pid);
        } else {
            lockMap.put(pid, updatedLocks);
        }

        Set<PageId> pages = txMap.get(tid);
        Set<PageId> updatedPages = new HashSet<>();
        for (PageId page : pages) {
            if (!page.equals(pid)) {
                updatedPages.add(page);
            }
        }
        if (updatedPages.isEmpty()) {
            txMap.remove(tid);
        } else {
            txMap.put(tid, updatedPages);
        }

        return true;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        List<LockManager.Lock> locks = lockMap.get(p);
        if (locks == null) {
            return true;
        }

        for (LockManager.Lock lock : locks) {
            if (lock.tid.equals(tid))
                return true;
        }

        return false;
    }

    public synchronized Set<PageId> getPageIdsFromTransactionId(TransactionId tid) {
        return txMap.get(tid);
    }

    class Lock {
        TransactionId tid;
        Permissions permission;

        Lock(TransactionId tid, Permissions permission) {
            this.tid = tid;
            this.permission = permission;
        }
    }
}
