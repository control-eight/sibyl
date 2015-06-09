package com.my.sibyl.itemsets.guice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author abykovsky
 * @since 6/9/15
 */
//@Singleton
public class MonitorService {

    private static final Log LOG = LogFactory.getLog(MonitorService.class);

    private Map<String, Long> counterMap = new ConcurrentHashMap<>();

    public MonitorService() {
        Thread daemon = new Thread(() -> {
            LOG.info("Monitor Service daemon is started.");
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                LOG.info("Monitor Service statistics:");
                for (Map.Entry<String, Long> entry : counterMap.entrySet()) {
                    LOG.info(entry);
                }
            }
        });
        daemon.setDaemon(true);
        daemon.start();
    }

    public void addMethodInvocation(String name) {
        counterMap.compute(name, (s, counter) -> {
            if(counter == null) counter = 0l;
            return counter + 1;
        });
    }
}
