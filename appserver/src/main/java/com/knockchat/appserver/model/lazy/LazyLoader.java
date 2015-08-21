package com.knockchat.appserver.model.lazy;

import java.util.List;

public interface LazyLoader<K> {
	int process(List<K> entities);
}
