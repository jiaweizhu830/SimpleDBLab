package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIdx;
    private Type gbFieldType;
    private int aFieldIdx;
    private Op aOp;
    private TupleDesc td;

    // key: group by field | value: tuple for that group (group by field value,
    // aggregate value)
    private Map<Field, Tuple> aggValues;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here (OK)
        this.gbFieldIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aFieldIdx = afield;
        this.aOp = what;
        if (!Op.COUNT.equals(what)) {
            throw new IllegalArgumentException("String Aggregator only supports COUNT operator");
        }
        this.aggValues = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here (OK)
        Field aggField = tup.getField(this.aFieldIdx);
        TupleDesc td = tup.getTupleDesc();
        String aggFieldName = this.aOp.toString() + " " + td.getFieldName(this.aFieldIdx);

        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            if (this.td == null) {
                Type[] typeAr = new Type[] { Type.INT_TYPE };
                String[] fieldAr = new String[] { aggFieldName };
                this.td = new TupleDesc(typeAr, fieldAr);
            }
            calculateAggregateValue(null, aggField);
        } else {
            if (this.td == null) {
                Type[] typeAr = new Type[] { td.getFieldType(this.gbFieldIdx), Type.INT_TYPE };
                String[] fieldAr = new String[] { td.getFieldName(this.gbFieldIdx), aggFieldName };
                this.td = new TupleDesc(typeAr, fieldAr);
            }

            Field gbField = tup.getField(this.gbFieldIdx);
            calculateAggregateValue(gbField, aggField);
        }
    }

    private void calculateAggregateValue(Field gbField, Field aggField) {
        Tuple tup = this.aggValues.getOrDefault(gbField, null);
        if (tup == null) {
            tup = new Tuple(this.td);
            if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
                tup.setField(0, new IntField(0));
            } else {
                tup.setField(0, gbField);
                tup.setField(1, new IntField(0));
            }
        }

        Integer aggValue;
        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            aggValue = ((IntField) tup.getField(0)).getValue();
        } else {
            aggValue = ((IntField) tup.getField(1)).getValue();
        }

        switch (this.aOp) {
            case COUNT:
                aggValue++;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator for String aggreagte: " + this.aOp.toString());
        }

        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            tup.setField(0, new IntField(aggValue));
        } else {
            tup.setField(1, new IntField(aggValue));
        }
        aggValues.put(gbField, tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
     *         using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here (OK)
        return new StringAggregatorIterator(aggValues.entrySet(), td);
    }

    private static class StringAggregatorIterator extends Operator {
        private Set<Map.Entry<Field, Tuple>> entries;
        private TupleDesc td;
        private Iterator<Map.Entry<Field, Tuple>> iterator;

        StringAggregatorIterator(Set<Map.Entry<Field, Tuple>> entries, TupleDesc td) {
            this.entries = entries;
            this.td = td;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator = entries.iterator();
            super.open();
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (!iterator.hasNext())
                return null;
            Map.Entry<Field, Tuple> next = iterator.next();
            return next.getValue();
        }

        @Override
        public OpIterator[] getChildren() {
            return null;
        }

        @Override
        public void setChildren(OpIterator[] children) {
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }
    }

}
