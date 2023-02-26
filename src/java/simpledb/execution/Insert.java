package simpledb.execution;

import java.io.IOException;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private int count;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // TODO: some code goes here (OK)
        this.t = t;
        this.child = child;
        this.tableId = tableId;

        Type[] typeAr = new Type[] { Type.INT_TYPE };
        this.td = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here (OK)
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here (OK)
        count = 0;
        child.open();
        super.open();
    }

    public void close() {
        // TODO: some code goes here (OK)
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here (OK)
        child.rewind();
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor.
     * It returns a one field tuple containing the number of inserted records.
     * Inserts should be passed through BufferPool. An instances of BufferPool is
     * available via Database.getBufferPool(). Note that insert DOES NOT need check
     * to see if a particular tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if
     *         called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here (OK)
        if (!this.child.hasNext())
            return null;

        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, this.child.next());
            } catch (NoSuchElementException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
            count++;
        }

        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(count));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here (OK)
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here (OK)
        this.child = children[0];
    }
}
