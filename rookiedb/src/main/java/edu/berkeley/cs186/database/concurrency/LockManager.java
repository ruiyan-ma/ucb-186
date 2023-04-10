package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 * <p>
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multi-granularity is handled by LockContext instead.
 * <p>
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 * <p>
 * This does mean that in the case of:
 * queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allow
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for (Lock prevLock : locks) {
                if (prevLock.transactionNum == except) continue;
                if (!LockType.compatible(prevLock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            long transNum = lock.transactionNum;

            // update lock if the transaction already has a lock
            for (Lock prevLock : locks) {
                if (prevLock.transactionNum.equals(transNum)) {
                    prevLock.lockType = lock.lockType;
                    return;
                }
            }

            // give the transaction a new lock
            locks.add(lock);
            transactionLocks.putIfAbsent(transNum, new ArrayList<>());
            transactionLocks.get(transNum).add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            long transNum = lock.transactionNum;
            locks.remove(lock);
            transactionLocks.get(transNum).remove(lock);
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            while (requests.hasNext()) {
                LockRequest request = requests.next();
                Lock lock = request.lock;

                // stop when the next lock cannot be granted
                if (!checkCompatible(lock.lockType, lock.transactionNum)) break;

                // remove this request from the queue
                requests.remove();

                // the transaction that made the request should be given the lock
                grantOrUpdateLock(lock);

                // any locks that the request stated should be released are released
                for (Lock releaseLock : request.releasedLocks) {
                    releaseLock(releaseLock);
                }

                // the transaction that made the request should be unblocked
                request.transaction.unblock();
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Throw exception if a lock on `name` is already held by `transaction`.
     */
    public void checkDuplicateLockRequest(String methodName, TransactionContext transaction,
                                           ResourceName name) {
        checkDuplicateLockRequest(methodName, transaction, name, Collections.emptyList());
    }

    /**
     * Throw exception if a lock on `name` is already held by `transaction`.
     */
    private void checkDuplicateLockRequest(String methodName, TransactionContext transaction,
                                           ResourceName name, List<ResourceName> releaseNames) {
        LockType currLock = getLockType(transaction, name);
        if (currLock != LockType.NL && !releaseNames.contains(name)) {
            throw new DuplicateLockRequestException(String.format(
                    "%s: transaction %d wants to get a lock on %s, but it already has a %s lock.",
                    methodName, transaction.getTransNum(), name, currLock));
        }
    }

    /**
     * Throw exception if a `type` lock on `name` is already held by `transaction`.
     */
    public void checkDuplicateLockRequest(String methodName, TransactionContext transaction,
                                          ResourceName name, LockType newLockType) {
        LockType currLock = getLockType(transaction, name);
        if (currLock == newLockType) {
            throw new DuplicateLockRequestException(String.format(
                    "%s: transaction %d wants to get a %s lock on %s, but it already has a %s lock.",
                    methodName, transaction.getTransNum(), newLockType, name, currLock));
        }
    }

    /**
     * Throw exception if `transaction` doesn't hold a lock on `name`.
     */
    public void checkNoLockHeld(String methodName, TransactionContext transaction, ResourceName name) {
        if (getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException(String.format(
                    "%s: transaction %d does not have a lock on %s.",
                    methodName, transaction.getTransNum(), name));
        }
    }

    /**
     * Throw exception if this is not a valid promotion.
     * <p>
     * A promotion from lock type A to lock type B is valid if and only if B is
     * substitutable for A, and B is not equal to A.
     */
    public void checkValidPromotion(String methodName, TransactionContext transaction,
                                    ResourceName name, LockType newLockType) {
        LockType currLock = getLockType(transaction, name);
        if (newLockType == currLock) {
            throw new InvalidLockException(String.format(
                    "%s: transaction %d wants to get a %s lock on %s, but it already has a %s lock.",
                    methodName, transaction.getTransNum(), newLockType, name, currLock));
        }

        if (!LockType.substitutable(newLockType, currLock)) {
            throw new InvalidLockException(String.format(
                    "%s: transaction %d wants to get a %s lock on %s, but it already has " +
                            "a %s lock and %s lock is not substitutable for %s lock.",
                    methodName, transaction.getTransNum(), newLockType, name,
                    currLock, newLockType, currLock));
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     * <p>
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     * <p>
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     * <p>
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     *                                       by `transaction` and isn't being released
     * @throws NoLockHeldException           if `transaction` doesn't hold a lock on one
     *                                       or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        boolean shouldBlock = false;
        String methodName = "LockManager#acquireAndRelease";

        synchronized (this) {
            checkDuplicateLockRequest(methodName, transaction, name, releaseNames);
            for (ResourceName releaseName : releaseNames) {
                checkNoLockHeld(methodName, transaction, releaseName);
            }

            long transNum = transaction.getTransNum();
            ResourceEntry resource = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transNum);

            List<Lock> releaseLocks = new ArrayList<>();
            Set<ResourceName> resourceSet = new HashSet<>(releaseNames);
            for (Lock lock : getLocks(transaction)) {
                // skip the lock on the target resource
                if (lock.name.equals(name)) continue;
                if (resourceSet.contains(lock.name)) {
                    releaseLocks.add(lock);
                }
            }

            if (resource.checkCompatible(lockType, transNum)) {
                // grant new lock
                resource.grantOrUpdateLock(newLock);
                // release all locks on releaseNames
                for (Lock lock : releaseLocks) {
                    getResourceEntry(lock.name).releaseLock(lock);
                }
            } else {
                // block this transaction
                transaction.prepareBlock();
                shouldBlock = true;
                // put this request at the front of the resource's queue
                LockRequest request = new LockRequest(transaction, newLock, releaseLocks);
                resource.addToQueue(request, true);
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     * <p>
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     *                                       `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        boolean shouldBlock = false;
        String methodName = "LockManager#acquire";

        synchronized (this) {
            checkDuplicateLockRequest(methodName, transaction, name);

            long transNum = transaction.getTransNum();
            ResourceEntry resource = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transNum);

            if (resource.checkCompatible(lockType, transNum)
                    && resource.waitingQueue.isEmpty()) {
                // grant new lock
                resource.grantOrUpdateLock(newLock);
            } else {
                // block this transaction
                transaction.prepareBlock();
                shouldBlock = true;
                // put the request at the back of waiting queue
                LockRequest request = new LockRequest(transaction, newLock);
                resource.addToQueue(request, false);
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     * <p>
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        String methodName = "LockManager#release";

        synchronized (this) {
            checkNoLockHeld(methodName, transaction, name);
            for (Lock lock : getLocks(transaction)) {
                if (lock.name.equals(name)) {
                    getResourceEntry(name).releaseLock(lock);
                    break;
                }
            }
        }

        transaction.unblock();
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if it's a valid substitution).
     * <p>
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     * <p>
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock on `name`
     * @throws NoLockHeldException           if `transaction` has no lock on `name`
     * @throws InvalidLockException          if the requested lock type is not a promotion.
     *                                       A promotion from lock type A to lock type B is
     *                                       valid if and only if B is substitutable for A,
     *                                       and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        boolean shouldBlock = false;
        String methodName = "LockManager#promote";

        synchronized (this) {
            checkDuplicateLockRequest(methodName, transaction, name, newLockType);
            checkNoLockHeld(methodName, transaction, name);
            checkValidPromotion(methodName, transaction, name, newLockType);

            long transNum = transaction.getTransNum();
            ResourceEntry resource = getResourceEntry(name);
            Lock newLock = new Lock(name, newLockType, transNum);

            if (resource.checkCompatible(newLockType, transNum)) {
                // grant new lock
                resource.grantOrUpdateLock(newLock);
            } else {
                // block this transaction
                transaction.prepareBlock();
                shouldBlock = true;
                // put this request at the front of the resource's queue
                LockRequest request = new LockRequest(transaction, newLock);
                resource.addToQueue(request, true);
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
