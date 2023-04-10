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
     * `requestType` on `context`.
     * <p>
     * `requestType` is guaranteed to be one of: S, X, NL.
     * <p>
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor locks will need to be
     * acquired or changed.
     * <p>
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext context, LockType requestType) {

        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // do nothing if the transaction or context is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || context == null) return;

        // do nothing if the current lock type can substitute the requested type
        LockType effectiveLock = context.getEffectiveLockType(transaction);
        if (LockType.substitutable(effectiveLock, requestType)) return;

        LockType explicitLock = context.getExplicitLockType(transaction);

        // if current lock is IX and request lock is S, promote to SIX
        if (explicitLock == LockType.IX && requestType == LockType.S) {
            ensureParentLock(transaction, context.parentContext(), LockType.IX);
            context.promote(transaction, LockType.SIX);
            return;
        }

        // if current lock is an intent lock, first escalate, then promote if necessary
        if (explicitLock.isIntent()) {
            context.escalate(transaction);
            explicitLock = context.getExplicitLockType(transaction);
            if (LockType.substitutable(explicitLock, requestType)) return;
        }

        // now explicit lock is a non-intent type lock (S or X)

        // ensure that we have appropriate locks on ancestors
        ensureParentLock(transaction, context.parentContext(), LockType.parentLock(requestType));
        // acquire or promote lock for this context
        if (explicitLock == LockType.NL) {
            context.acquire(transaction, requestType);
        } else {
            context.promote(transaction, requestType);
        }
    }

    /**
     * Ensure that the lock on context has more permission than the required intent lock.
     * That means the lock on context can substitute the required intent lock.
     * <p>
     * If the lock on context cannot substitute the required lock, first recursively ensure
     * that ancestors have appropriate locks, then acquire or promote lock for context.
     */
    private static void ensureParentLock(TransactionContext transaction,
                                         LockContext context, LockType required) {
        // required lock must be an intent lock
        assert (required.isIntent());

        // do nothing if context context is null
        if (context == null) return;

        LockType currLock = context.getExplicitLockType(transaction);
        if (!LockType.substitutable(currLock, required)) {
            // recursively ensure that ancestors have appropriate locks
            ensureParentLock(transaction, context.parentContext(), required);
            // acquire or promote lock for context
            if (currLock == LockType.NL) {
                context.acquire(transaction, required);
            } else if (currLock.isIntent()) {
                context.promote(transaction, required);
            } else {
                // currLock == S and required == IS --> redundant
                // currLock == S and required == IX --> promote to SIX
                // currLock == X and required == IS --> redundant
                // currLock == X and required == IX --> redundant
                context.promote(transaction, LockType.SIX);
            }
        }
    }
}
