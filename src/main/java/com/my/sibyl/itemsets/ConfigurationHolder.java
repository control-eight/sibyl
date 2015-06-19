package com.my.sibyl.itemsets;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class ConfigurationHolder {

    private static final Log LOG = LogFactory.getLog(ConfigurationHolder.class);

    private static final String PROPERTIES_NAME = "application.properties";

    private static final String ENV_PROPERTIES = "SIBYL_ENV_PROPERTIES";

    public static Configuration getConfiguration() {
        return ConfigurationHolderBuilder.configuration;
    }

    private static class ConfigurationHolderBuilder {

        public static Configuration configuration;

        static {
            CompositeConfiguration config = new CompositeConfiguration();
            config.addConfiguration(new SystemConfiguration());

            if(System.getenv(ENV_PROPERTIES) != null || System.getProperty(ENV_PROPERTIES) != null) {
                String envProperties = System.getenv(ENV_PROPERTIES);
                if(envProperties == null) {
                    envProperties = System.getProperty(ENV_PROPERTIES);
                }
                LOG.info("Load env properties: " + envProperties);

                try {
                    config.addConfiguration(new PropertiesConfiguration(envProperties));
                } catch (ConfigurationException e) {
                    throw new RuntimeException("Exception during build properties", e);
                }
            }

            LOG.info("Load app properties: " + PROPERTIES_NAME);
            try {
                config.addConfiguration(new PropertiesConfiguration(PROPERTIES_NAME));
            } catch (ConfigurationException e) {
                throw new RuntimeException("Exception during build properties", e);
            }

            for(Iterator<String> iter = config.getKeys(); iter.hasNext();) {
                String key = iter.next();
                if(System.getProperty(key) == null) {
                    Object property = config.getProperty(key);
                    System.setProperty(key, (property != null? property.toString(): null));
                }
            }

            configuration = config;
        }
    }
}
