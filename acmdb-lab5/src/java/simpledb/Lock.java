package simpledb;


import java.util.HashSet;
import java.util.Set;

public class Lock {
    private PageId pid;
    private Set<TransactionId> sharedTidSet;
    private TransactionId exclusiveTid;

    public Lock(PageId pid){
        this.pid = pid;
        sharedTidSet = new HashSet<>();
    }

    public boolean acquireLock(TransactionId tid, LockType type){
        if(type == LockType.SHARED){
            if(exclusiveTid != null)
                return exclusiveTid == tid;
            sharedTidSet.add(tid);
            return true;
        }
        else{
            if(exclusiveTid != null)
                return exclusiveTid == tid;
            if(sharedTidSet.isEmpty()){
                exclusiveTid = tid;
                return true;
            }
            if(sharedTidSet.contains(tid) && sharedTidSet.size() == 1){
                sharedTidSet.remove(tid);
                exclusiveTid = tid;
                return true;
            }
            return false;
        }
    }

    public synchronized void releaseLock(TransactionId tid){
        if(exclusiveTid != null){
            assert exclusiveTid == tid;
            exclusiveTid = null;
        }
        else{
            assert sharedTidSet.contains(tid);
            sharedTidSet.remove(tid);
        }
        notify();
    }

    public synchronized boolean holdsLock(TransactionId tid){
        if(exclusiveTid != null)
            return exclusiveTid == tid;
        else
            return sharedTidSet.contains(tid);
    }

    public synchronized Set<TransactionId> getHoldsTids(){
        Set<TransactionId> tids = new HashSet<>();
        if(exclusiveTid != null)
            tids.add(exclusiveTid);
        else
            tids.addAll(sharedTidSet);
        return tids;
    }
}
