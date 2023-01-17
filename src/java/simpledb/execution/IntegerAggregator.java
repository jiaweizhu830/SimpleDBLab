package simpledb.execution;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIdx;
    private Type gbFieldType;
    private int aFieldIdx;
    private Op aggOp;
    private TupleDesc td;

    // key: group by field | value: tuple for that group (aggregate value + optional
    // group by value)
    private Map<Field, Tuple> aggValues;
    // key: group by field | value: count of original tuples in that group
    private Map<Field, Integer> groupByCount;

    // keep track of sum for each group (bc. int avg loses accuracy during
    // calculation)
    private Map<Field, Integer> groupBySum;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here (OK)
        this.gbFieldIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aFieldIdx = afield;
        this.aggOp = what;
        this.aggValues = new HashMap<>();
        this.groupByCount = new HashMap<>();
        this.groupBySum = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here (OK)
        IntField aggField = (IntField) tup.getField(this.aFieldIdx);
        TupleDesc td = tup.getTupleDesc();
        String aggFieldName = this.aggOp.toString() + " " + td.getFieldName(this.aFieldIdx);

        // only return aggregate values
        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            if (this.aggValues.size() == 0) {
                Type[] typeAr = new Type[] { td.getFieldType(this.aFieldIdx) };
                String[] fieldAr = new String[] { aggFieldName };
                this.td = new TupleDesc(typeAr, fieldAr);
            }

            calculateAggregateValue(null, aggField, td);

        } else {
            // return aggregate values + group by values

            if (this.aggValues.size() == 0) {
                Type[] typeAr = new Type[] { td.getFieldType(this.gbFieldIdx), td.getFieldType(this.aFieldIdx) };
                String[] fieldAr = new String[] { td.getFieldName(this.gbFieldIdx), aggFieldName };
                this.td = new TupleDesc(typeAr, fieldAr);
            }

            Field gbField = tup.getField(this.gbFieldIdx);
            calculateAggregateValue(gbField, aggField, td);
        }
    }

    private void calculateAggregateValue(Field gbField, IntField aggField, TupleDesc td) {
        Tuple tuple = aggValues.getOrDefault(gbField, null);
        if (tuple == null) {
            tuple = new Tuple(this.td);
            if (this.gbFieldIdx != Aggregator.NO_GROUPING) {
                tuple.setField(0, gbField);
                tuple.setField(1, aggField);
            } else {
                tuple.setField(0, aggField);
            }
            groupByCount.put(gbField, 0);
            groupBySum.put(gbField, 0);
        }
        Integer aggValue;
        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            aggValue = ((IntField) tuple.getField(0)).getValue();
        } else {
            aggValue = ((IntField) tuple.getField(1)).getValue();
        }
        // number of fields for that group
        int curCount = groupByCount.get(gbField);

        switch (this.aggOp) {
            case AVG:
                int sum = groupBySum.get(gbField) + aggField.getValue();
                groupBySum.put(gbField, sum);
                aggValue = sum / (curCount + 1);
                break;
            case COUNT:
                aggValue = curCount + 1;
                break;
            case MAX:
                aggValue = Math.max(aggValue, aggField.getValue());
                break;
            case MIN:
                aggValue = Math.min(aggValue, aggField.getValue());
                break;
            case SUM:
                if (curCount != 0) {
                    aggValue += aggField.getValue();
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "THe aggregation operator " + this.aggOp.toString() + " is not supported");
        }

        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            tuple.setField(0, new IntField(aggValue));
        } else {
            tuple.setField(1, new IntField(aggValue));
        }
        aggValues.put(gbField, tuple);
        groupByCount.put(gbField, curCount + 1);
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
        return new IntegerAggregrateIterator(aggValues.entrySet(), this.td);
    }

    private static class IntegerAggregrateIterator extends Operator {
        private Set<Map.Entry<Field, Tuple>> entries;
        private TupleDesc td;
        private Iterator<Map.Entry<Field, Tuple>> iterator;

        IntegerAggregrateIterator(Set<Map.Entry<Field, Tuple>> entries, TupleDesc td) {
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
            this.iterator = this.entries.iterator();
            super.open();
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (!this.iterator.hasNext())
                return null;

            Map.Entry<Field, Tuple> nextEntry = iterator.next();
            return nextEntry.getValue();
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
            return this.td;
        }
    }

}
