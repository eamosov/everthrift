package org.everthrift.jms;

import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Created by fluder on 18/09/17.
 */
@ManagedResource
public class JmsContainerManager {
    final DefaultMessageListenerContainer container;
    final String jmxName;

    public JmsContainerManager(DefaultMessageListenerContainer container, String jmxName) {
        this.container = container;
        this.jmxName = jmxName;
    }

    @ManagedAttribute
    public void setMaxConcurrentConsumers(int maxConcurrentConsumers) {
        container.setMaxConcurrentConsumers(maxConcurrentConsumers);
    }

    @ManagedAttribute
    public final int getMaxConcurrentConsumers() {
        return container.getMaxConcurrentConsumers();
    }

}
