package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private int count;

    /**
     * Constructor specifying the transaction that this delete belongs to as well as
     * the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here (OK)
        this.t = t;
        this.child = child;

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here (OK)
        if (!child.hasNext())
            return null;

        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(this.t, child.next());
                count++;
            } catch (NoSuchElementException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }
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
