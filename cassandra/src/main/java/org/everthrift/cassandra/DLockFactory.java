package org.everthrift.cassandra;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

public class DLockFactory {

	private final Session session;
	private final int ttl;
	
	public DLockFactory(Session session) {
		this(session, DLock.DEFAULT_TTL);
	}
	
	public DLockFactory(Session session, int ttl) {
		super();
		this.session = session;
		this.ttl = ttl;
	}
	
	public DLock lock(String ... lockNames){
		final List<String> _lockNames = Lists.newArrayList(lockNames);
		Collections.sort(_lockNames);
		final DLock dlock =  new DLock(_lockNames, UUID.randomUUID().toString(), session, ttl);
		dlock.lock();
		return dlock;		
	}

	public DLock lock(long timeoutMillis, String ... lockNames){
		final List<String> _lockNames = Lists.newArrayList(lockNames);
		Collections.sort(_lockNames);
		final DLock dlock =  new DLock(_lockNames, UUID.randomUUID().toString(), session, ttl);
		dlock.lock(timeoutMillis);
		return dlock;		
	}

	public DLock lock(String lockName){		
		final DLock dlock =  new DLock(Collections.singletonList(lockName), UUID.randomUUID().toString(), session, ttl);
		dlock.lock();
		return dlock;
	}
	
	public DLock lock(long timeoutMillis, String lockName){		
		final DLock dlock =  new DLock(Collections.singletonList(lockName), UUID.randomUUID().toString(), session, ttl);
		dlock.lock(timeoutMillis);
		return dlock;
	}
	
}
