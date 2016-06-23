package org.everthrift.appserver.model.lazy;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

public interface AsyncLazyLoader<K> extends LazyLoader<K> {
	
	@Override
	default int process(List<K> entities){
		try {
			return processAsync(entities).get();
		} catch (ExecutionException e) {
			throw Throwables.propagate(e.getCause());
		} catch (InterruptedException e) {
			throw Throwables.propagate(e);
		}
	}
	
	ListenableFuture<Integer> processAsync(List<K> entities);
}
