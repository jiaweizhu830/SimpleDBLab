package simpledb;

import java.io.*;
import simpledb.common.*;
import simpledb.storage.*;
import simpledb.execution.*;
import simpledb.transaction.*;

public class test {
    public static void main(String[] args) {
        Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String[] names = new String[] { "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        HeapFile table1 = new HeapFile(new File("data.txt"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, table1.getId());

        try {
            scan.open();
            while (scan.hasNext()) {
                // load page from disk/buffer pool
                Tuple tup = scan.next();
                System.out.println(tup);
            }
            scan.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        }
    }
}
