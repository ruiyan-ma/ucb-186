package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multi-granularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockMgr;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockMgr, LockContext parent, String name) {
        this(lockMgr, parent, name, false);
    }

    protected LockContext(LockManager lockMgr, LockContext parent, String name,
                          boolean readonly) {
        this.lockMgr = lockMgr;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockMgr, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockMgr.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Recursively update numChildLocks for this LockContext and all parent contexts.
     */
    private void updateNumChildLocks(TransactionContext transaction,
                                     LockContext context, int val) {
        if (context == null) return;
        int lockNum = context.getNumChildren(transaction) + val;
        context.numChildLocks.put(transaction.getTransNum(), Math.max(lockNum, 0));
        updateNumChildLocks(transaction, context.parentContext(), val);
    }

    /**
     * Throw exception if the parent lock of this context cannot grant `childType` lock.
     */
    private void checkParentLock(TransactionContext transaction, LockType childType) {
        if (name.parent() != null) {
            LockType parentType = lockMgr.getLockType(transaction, name.parent());
            if (!LockType.canBeParentLock(parentType, childType)) {
                throw new InvalidLockException(String.format(
                        "Transaction %d wants %s lock on %s but its parent has %s lock.",
                        transaction.getTransNum(), childType, name, parentType));
            }
        }
    }

    /**
     * Throw exception if this context is read only.
     */
    private void checkReadOnly() {
        if (readonly) {
            throw new UnsupportedOperationException(
                    String.format("%s is read only.", this));
        }
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        checkReadOnly();
        checkParentLock(transaction, lockType);
        if (hasSIXAncestor(transaction) && (lockType == LockType.S || lockType == LockType.IS)) {
            throw new InvalidLockException(String.format(
                    "Transaction %d acquires an %s lock on %s but an ancestor has SIX.",
                    transaction.getTransNum(), name, lockType));
        }

        lockMgr.acquire(transaction, name, lockType);
        updateNumChildLocks(transaction, parentContext(), 1);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multi-granularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        checkReadOnly();
        if (getNumChildren(transaction) != 0) {
            throw new InvalidLockException(String.format(
                    "Transaction %d wants to release lock on %s but has child lock.",
                    transaction.getTransNum(), name));
        }

        lockMgr.release(transaction, name);
        updateNumChildLocks(transaction, parentContext(), -1);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        checkReadOnly();
        checkParentLock(transaction, newLockType);
        lockMgr.checkDuplicateLockRequest(transaction, name, newLockType);
        lockMgr.checkNoLockHeld(transaction, name);
        lockMgr.checkValidPromotion(transaction, name, newLockType);

        if (newLockType != LockType.SIX) {
            lockMgr.promote(transaction, name, newLockType);
        } else {
            if (hasSIXAncestor(transaction)) {
                // throw exception if an ancestor has SIX
                throw new InvalidLockException(String.format(
                        "Transaction %d wants to promote %s lock to SIX but an ancestor has SIX.",
                        transaction.getTransNum(), name));
            } else {
                // release all descendant locks of type S/IS
                List<ResourceName> releaseNames = sisDescendants(transaction);
                releaseNames.add(name);  // release its own lock to promote
                lockMgr.acquireAndRelease(transaction, name, newLockType, releaseNames);

                // update numChildLocks for all descendant locks
                releaseNames.remove(name);  // we promote (not release) lock on this context
                for (ResourceName resource : releaseNames) {
                    LockContext context = fromResourceName(lockMgr, resource);
                    updateNumChildLocks(transaction, context.parentContext(), -1);
                }
            }
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        checkReadOnly();
        lockMgr.checkNoLockHeld(transaction, name);

        LockType currLock = lockMgr.getLockType(transaction, name);
        if (currLock == LockType.S || currLock == LockType.X) return;

        List<ResourceName> releaseNames = new ArrayList<>();
        releaseNames.add(name);  // release its own lock to get a new one
        for (Lock lock : lockMgr.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name)) {
                releaseNames.add(lock.name);
            }
        }

        if (currLock == LockType.IS) {
            lockMgr.acquireAndRelease(transaction, name, LockType.S, releaseNames);
        } else {
            lockMgr.acquireAndRelease(transaction, name, LockType.X, releaseNames);
        }

        releaseNames.remove(name);  // we escalate (not release) lock on this context
        for (ResourceName resource : releaseNames) {
            LockContext context = fromResourceName(lockMgr, resource);
            updateNumChildLocks(transaction, context.parentContext(), -1);
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        return lockMgr.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;

        // check if there is an explicit lock
        LockType explicit = getExplicitLockType(transaction);
        if (explicit != LockType.NL) return explicit;

        // find the first non-NL lock on ancestors of this context
        LockContext context = parentContext();
        while (context != null) {
            LockType lockType = context.getExplicitLockType(transaction);
            if (lockType != LockType.NL) break;
            context = context.parentContext();
        }

        // cannot find non-NL lock, return NL
        if (context == null) return LockType.NL;

        // check if the lock type is S, X or SIX
        LockType type = context.getExplicitLockType(transaction);
        if (type == LockType.S || type == LockType.SIX) {
            return LockType.S;
        } else if (type == LockType.X) {
            return LockType.X;
        } else {
            return LockType.NL;
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     *
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext context = parentContext();
        while (context != null) {
            LockType lockType = context.getExplicitLockType(transaction);
            if (lockType == LockType.SIX) return true;
            context = context.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<ResourceName> result = new ArrayList<>();

        // find all descendant locks that are S or IS type for this transaction
        for (Lock lock : lockMgr.getLocks(transaction)) {
            if (lock.name.isDescendantOf(name) && (lock.lockType == LockType.S
                    || lock.lockType == LockType.IS)) {
                result.add(lock.name);
            }
        }
        return result;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockMgr, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

