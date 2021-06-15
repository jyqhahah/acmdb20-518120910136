package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId, Lock> lockMap;
    private Map<TransactionId, Set<PageId>> holdsMap;
    private Map<TransactionId, Set<TransactionId>> waitsMap;

    public LockManager(){
        lockMap = new ConcurrentHashMap<>();
        holdsMap = new ConcurrentHashMap<>();
        waitsMap = new ConcurrentHashMap<>();
    }

    private synchronized void updateWaits(PageId pid, TransactionId tid){
        Set<TransactionId> waits = new HashSet<>();
        if(!waitsMap.containsKey(tid))
            waitsMap.put(tid, waits);
        else
            waits = waitsMap.get(tid);
        waits.clear();
        if(pid != null){
            waits.addAll(lockMap.get(pid).getHoldsTids());
        }
    }

    public synchronized boolean isDeadLock(TransactionId tid){
        return isDeadLock(tid, tid, new HashSet<>());
    }

    private boolean isDeadLock(TransactionId nw, TransactionId dest, Set<TransactionId> isVisited){
        isVisited.add(nw);
        Set<TransactionId> waits = waitsMap.get(nw);
        if(waits == null) return false;
        for(TransactionId nxt : waits){
            if(nxt == dest)
                return true;
            if(isVisited.contains(nxt))
                continue;
            if(isDeadLock(nxt, dest, isVisited))
                return true;
        }
        return false;
    }

    public void acquireLock(PageId pid, TransactionId tid, LockType type) throws TransactionAbortedException {
        Lock lock = new Lock(pid);
        if(!lockMap.containsKey(pid))
            lockMap.put(pid, lock);
        else
            lock = lockMap.get(pid);
        synchronized (lock){
            while(!lock.acquireLock(tid, type)){
                updateWaits(pid, tid);
                if(isDeadLock(tid)){
                    updateWaits(null, tid);
                    throw new TransactionAbortedException();
                }
                try{
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        updateWaits(null, tid);
        Set<PageId> holds = new HashSet<>();
        if(!holdsMap.containsKey(tid))
            holdsMap.put(tid, holds);
        else
            holds = holdsMap.get(tid);
        holds.add(pid);
    }

    public void releaseLock(PageId pid, TransactionId tid){
        assert lockMap.containsKey(pid);
        lockMap.get(pid).releaseLock(tid);
        holdsMap.get(tid).remove(pid);
    }

    public void releaseAllLock(TransactionId tid){
        Set<PageId> holds = holdsMap.get(tid);
        holdsMap.remove(tid);
        if(holds != null){
            for(PageId pid : holds)
                lockMap.get(pid).releaseLock(tid);
        }
    }

    public Set<PageId> getHoldsLock(TransactionId tid){
        return holdsMap.get(tid);
    }

    public boolean holdsLock(PageId pid, TransactionId tid){
        if(lockMap.containsKey(pid))
            return lockMap.get(pid).holdsLock(tid);
        Lock lock = new Lock(pid);
        lockMap.put(pid, lock);
        return lock.holdsLock(tid);
    }
}
