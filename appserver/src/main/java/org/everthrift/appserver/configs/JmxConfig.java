package org.everthrift.appserver.configs;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource("classpath:jmx-beans.xml")
public class JmxConfig {

    public JmxConfig() {

    }
}
