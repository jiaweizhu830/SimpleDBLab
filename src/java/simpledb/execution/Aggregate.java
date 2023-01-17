package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aFieldIndex;
    private int gFieldIndex;
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator iterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // TODO: some code goes here (OK)
        this.child = child;
        this.aFieldIndex = afield;
        this.gFieldIndex = gfield;
        this.aop = aop;

        initAggregator(child);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // TODO: some code goes here (OK)
        return this.gFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of
     *         the groupby field in the <b>OUTPUT</b> tuples. If not, return null;
     */
    public String groupFieldName() {
        // TODO: some code goes here (OK)
        if (Aggregator.NO_GROUPING == this.gFieldIndex) {
            return null;
        }
        return child.getTupleDesc().getFieldName(this.gFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // TODO: some code goes here (OK)
        return this.aFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        // TODO: some code goes here (OK)
        return this.child.getTupleDesc().getFieldName(this.aFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // TODO: some code goes here (OK)
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        // TODO: some code goes here (OK)
        child.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            this.aggregator.mergeTupleIntoGroup(next);
        }
        this.iterator = this.aggregator.iterator();
        this.iterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is
     * the field by which we are grouping, and the second field is the result of
     * computing the aggregate. If there is no group by field, then the result tuple
     * should contain one field representing the result of the aggregate. Should
     * return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here (OK)
        if (!this.iterator.hasNext())
            return null;
        return this.iterator.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here (OK)
        this.iterator.rewind();
        this.child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this
     * will have one field - the aggregate column. If there is a group by field, the
     * first field will be the group by field, and the second will be the aggregate
     * value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
     * in the constructor, and child_td is the TupleDesc of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here (OK)
        TupleDesc td = child.getTupleDesc();
        String aggFieldName = this.aop.toString() + " " + td.getFieldName(this.aFieldIndex);
        Type[] typeAr;
        String[] fieldAr;
        if (this.gFieldIndex == Aggregator.NO_GROUPING) {
            typeAr = new Type[] { td.getFieldType(this.aFieldIndex) };
            fieldAr = new String[] { aggFieldName };
        } else {
            typeAr = new Type[] { td.getFieldType(this.gFieldIndex), td.getFieldType(this.aFieldIndex) };
            fieldAr = new String[] { td.getFieldName(this.gFieldIndex), aggFieldName };
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    public void close() {
        // TODO: some code goes here (OK)
        this.iterator = null;
        child.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here (OK)
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here (OK)
        this.child = children[0];
        initAggregator(child);
    }

    private void initAggregator(OpIterator child) {
        Type type = this.child.getTupleDesc().getFieldType(aFieldIndex);
        switch (type) {
            case INT_TYPE:
                this.aggregator = new IntegerAggregator(gFieldIndex, type, aFieldIndex, aop);
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(gFieldIndex, type, aFieldIndex, aop);
                break;
            default:
                throw new IllegalArgumentException("input type is not supported: " + type.toString());
        }
    }
}
