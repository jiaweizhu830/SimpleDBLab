package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.storage.BufferPool;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private Map<PageId, Page> pageMap;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here (OK)
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here (OK)
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you
     * will need to generate this tableid somewhere to ensure that each HeapFile has
     * a "unique id," and that you always return the same value for a particular
     * HeapFile. We suggest hashing the absolute file name of the file underlying
     * the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here (OK)
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here (OK)
        return td;
    }

    // see DbFile.java for javadocs
    // Random access file:
    // https://www.digitalocean.com/community/tutorials/java-randomaccessfile-example
    public Page readPage(PageId pid) {
        // TODO: some code goes here (OK)
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "r");
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(offset);

            byte[] bytes = new byte[BufferPool.getPageSize()];
            raf.read(bytes);
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
        } catch (IOException e) {
            return null;
        } finally {
            try {
                raf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    // iterate through through the tuples of each page in the HeapFile
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFile.HeapFileIterator(this, null);
    }

    private static class HeapFileIterator implements DbFileIterator {
        private HeapFile heapFile;
        private TransactionId tid;
        private Iterator<Tuple> tpIter;
        private int pgCursor;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pgCursor = 0;
            Page pg = Database.getBufferPool().getPage(this.tid, new HeapPageId(heapFile.getId(), this.pgCursor),
                    Permissions.READ_ONLY);
            tpIter = ((HeapPage) pg).iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tpIter == null) {
                return false;
            }

            if (!tpIter.hasNext() && this.pgCursor < this.heapFile.numPages() - 1) {
                // There are still more pages
                return true;
            }
            return tpIter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if (tpIter == null) {
                throw new NoSuchElementException();
            }
            if (hasNext()) {
                if (!tpIter.hasNext()) {
                    Page pg = Database.getBufferPool().getPage(this.tid,
                            new HeapPageId(heapFile.getId(), ++this.pgCursor), Permissions.READ_ONLY);
                    tpIter = ((HeapPage) pg).iterator();
                }

                return tpIter.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tpIter = null;
        }
    }
}
