package com.knockchat.appserver.model;

import java.util.List;

public interface LazyLoader<K> {
	int process(List<K> entities);
}
