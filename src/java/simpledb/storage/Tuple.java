package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import simpledb.storage.TupleDesc;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId rid;
    private List<Field> fieldList;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance
     *           with at least one field.
     */
    public Tuple(TupleDesc td) {
        // TODO: some code goes here (OK)
        if (!validTupleDesc(td)) {
            throw new IllegalArgumentException("TupleDesc instance needs to have at least one field.");
        }

        this.td = td;
        this.fieldList = new ArrayList<>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here (OK)
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be
     *         null.
     */
    public RecordId getRecordId() {
        // TODO: some code goes here (OK)
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // TODO: some code goes here (OK)
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // TODO: some code goes here (OK)
        if (i < 0 || i > this.fieldList.size()) {
            throw new IllegalArgumentException("Field index is invalid");
        }

        // append new element
        if (i == this.fieldList.size()) {
            this.fieldList.add(f);
        } else {
            this.fieldList.set(i, f);
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // TODO: some code goes here (OK)
        if (i < 0 || i >= this.fieldList.size()) {
            throw new IllegalArgumentException("Field index is invalid");
        }

        return this.fieldList.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system
     * tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // TODO: some code goes here (OK)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.fieldList.size(); i++) {
            sb.append(this.fieldList.get(i).toString() + " ");
        }
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // TODO: some code goes here (OK)
        return this.fieldList.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // TODO: some code goes here (OK)
        if (!validTupleDesc(td)) {
            throw new IllegalArgumentException("TupleDesc instance needs to have at least one field.");
        }
        this.td = td;
    }

    private boolean validTupleDesc(TupleDesc td) {
        if (td == null || td.numFields() == 0) {
            return false;
        }
        return true;
    }
}
