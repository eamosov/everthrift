package com.knockchat.node.model;

import java.util.Collection;

public interface InvalidatedIF<K> {
	void invalidate(K id);
    void invalidate(Collection<K> ids);
}
