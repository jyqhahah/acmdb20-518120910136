package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try{
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            int offset = pid.pageNumber() * BufferPool.getPageSize();
            raf.seek(offset);
            byte[] data = new byte[BufferPool.getPageSize()];
            int size = raf.read(data);
            assert size == BufferPool.getPageSize();
            raf.close();
            return new HeapPage((HeapPageId)pid, data);
        }catch (Exception e) {
            throw new IllegalArgumentException("IllegalArgumentException throw by HeapFile readPage()");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator{
        private TransactionId tid;
        private int offset;
        private Iterator<Tuple> tupleIt;
        private HeapPage curPage;

        private HeapPage getHeapPage(int offset) throws TransactionAbortedException, DbException {
            return (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), offset), Permissions.READ_ONLY);
        }

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException{
            if(curPage == null || tupleIt == null)
                return false;
            if(tupleIt.hasNext())
                return true;
            int tmp = offset + 1;
            while (tmp < numPages())
            {
                HeapPage nextPage = getHeapPage(tmp);
                Iterator<Tuple> nextIt = nextPage.iterator();
                if (nextIt.hasNext())
                    return true;
                tmp++;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            if(curPage == null || tupleIt == null)
                throw new NoSuchElementException("NoSuchElementException throw by HeapFileIterator");
            if(tupleIt.hasNext())
                return tupleIt.next();
            offset++;
            while (offset < numPages())
            {
                curPage = getHeapPage(offset);
                tupleIt = curPage.iterator();
                if (tupleIt.hasNext())
                    return tupleIt.next();
                offset++;
            }
            throw new NoSuchElementException("NoSuchElementException throw by HeapFileIterator");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException{
            offset = 0;
            curPage = getHeapPage(offset);
            tupleIt = curPage.iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException{
            this.open();
        }

        @Override
        public void close(){
            offset = 0;
            curPage = null;
            tupleIt = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

