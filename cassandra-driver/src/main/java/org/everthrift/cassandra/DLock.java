package org.everthrift.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class DLock implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DLock.class);

    public static class LockException extends RuntimeException {
        final String lockName;

        final String ownerName;

        LockException(String lockName, String ownerName, Exception e) {
            super(e);
            this.lockName = lockName;
            this.ownerName = ownerName;
        }

        @Override
        public String toString() {
            return "LockException [lockName=" + lockName + ", ownerName=" + ownerName + "]";
        }

    }

    private static String TABLE_NAME = "locks";

    private final List<String> unlocked;

    private final List<String> locked;

    private final String ownerName;

    private final Session session;

    private final int ttl;

    public static final long minSleep = 10;

    public static final long maxSleep = 100;

    public static final int DEFAULT_TTL = 3 * 60;

    public static final long DEFAULT_TIMEOUT = DEFAULT_TTL * 600 - 1;

    private static final Random rnd = new Random();

    DLock(List<String> lockNames, String ownerName, Session session, int ttl) {
        super();
        this.unlocked = Lists.newArrayList(lockNames);
        this.locked = Lists.newArrayListWithCapacity(unlocked.size());
        this.ownerName = ownerName;
        this.session = session;
        this.ttl = ttl;
    }

    private boolean tryLock(final String _lockName) {
        final Statement insert = QueryBuilder.insertInto(TABLE_NAME)
                                             .value("name", _lockName)
                                             .value("owner", ownerName)
                                             .ifNotExists()
                                             .using(QueryBuilder.ttl(ttl));
        final ResultSet rs = session.execute(insert);
        final boolean wasApplied = rs.wasApplied();
        log.debug("lock '{}' by '{}': {}", _lockName, ownerName, wasApplied);
        return wasApplied;
    }

    private boolean unlock(String _lockName) {
        final ResultSet rs = session.execute(QueryBuilder.delete()
                                                         .from(TABLE_NAME)
                                                         .where(QueryBuilder.eq("name", _lockName))
                                                         .onlyIf(QueryBuilder.eq("owner", ownerName)));
        final boolean wasApplied = rs.wasApplied();
        log.debug("unlock '{}' by '{}': {}", _lockName, ownerName, wasApplied);
        return wasApplied;
    }

    void lock() {
        lock(DEFAULT_TIMEOUT);
    }

    public void unlock() {
        final Iterator<String> it = locked.iterator();
        while (it.hasNext()) {
            final String lockName = it.next();
            unlock(lockName);
            it.remove();
            Lists.reverse(unlocked).add(lockName);
        }
    }

    void lock(long timeoutMillis) {

        if (timeoutMillis > ttl * 600) {
            throw new IllegalArgumentException("timeoutMillis must be <=" + (ttl * 600));
        }

        final long expiredTs = System.currentTimeMillis() + timeoutMillis;
        final Iterator<String> it = unlocked.iterator();
        while (it.hasNext()) {
            final String lockName = it.next();
            try {
                lock(lockName, expiredTs);
            } catch (LockException e) {
                unlock();
                throw e;
            }
            it.remove();
            Lists.reverse(locked).add(lockName);
        }
    }

    private void lock(final String _lockName, final long expiredTs) {
        boolean locked = false;
        while (locked == false && System.currentTimeMillis() < expiredTs) {
            locked = tryLock(_lockName);
            final long now = System.currentTimeMillis();

            if (locked == false && expiredTs - now > minSleep) {
                final long t = minSleep + rnd.nextInt((int) Math.min(maxSleep, expiredTs - now));
                try {
                    Thread.sleep(t);
                } catch (InterruptedException e) {
                    throw new LockException(_lockName, ownerName, e);
                }
            }
        }

        if (locked == false) {
            throw new LockException(_lockName, ownerName, null);
        }
    }

    @Override
    public void close() {
        unlock();
    }

}
