package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multi-granularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor locks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {

        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // do nothing if the current lock type can substitute the requested type
        LockType currLock = lockContext.getEffectiveLockType(transaction);
        if (LockType.substitutable(currLock, requestType)) return;

        if (currLock == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
        } else if (currLock.isIntent()) {
            // first escalate lock, then promote if not substitutable
            lockContext.escalate(transaction);
            currLock = lockContext.getExplicitLockType(transaction);
            if (!LockType.substitutable(currLock, requestType)) {
                lockContext.promote(transaction, requestType);
            }
        } else {
            // ensure that we have appropriate locks on ancestors
            ensureParentLock(transaction, lockContext.parentContext(),
                    LockType.parentLock(requestType));

            // acquire or promote lock for this context
            if (currLock == LockType.NL) {
                lockContext.acquire(transaction, requestType);
            } else {
                lockContext.promote(transaction, requestType);
            }
        }
    }

    /**
     * Ensure that the lock on parent has more permission than the required intent lock.
     * That means the lock on parent can substitute the required intent lock.
     *
     * If the lock on parent cannot substitute the required lock, first recursively ensure
     * that ancestors have appropriate locks, then acquire or promote lock for parent.
     */
    private static void ensureParentLock(TransactionContext transaction,
                                         LockContext parent, LockType required) {
        // required lock must be an intent lock
        assert (required.isIntent());

        // do nothing if parent context is null
        if (parent == null) return;

        LockType currLock = parent.getExplicitLockType(transaction);
        if (!LockType.substitutable(currLock, required)) {
            // recursively ensure that ancestors have appropriate locks
            ensureParentLock(transaction, parent.parentContext(), required);
            // acquire or promote lock for parent
            if (currLock == LockType.NL) {
                parent.acquire(transaction, required);
            } else {
                parent.promote(transaction, required);
            }
        }
    }
}
