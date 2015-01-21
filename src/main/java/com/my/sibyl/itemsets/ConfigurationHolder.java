package com.my.sibyl.itemsets;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

/**
 * @author abykovsky
 * @since 1/21/15
 */
public class ConfigurationHolder {

    private static final String PROPERTIES_NAME = "application.properties";

    public static Configuration getConfiguration() {
        return ConfigurationHolderBuilder.configuration;
    }

    private static class ConfigurationHolderBuilder {

        public static Configuration configuration;

        static {
            CompositeConfiguration config = new CompositeConfiguration();
            config.addConfiguration(new SystemConfiguration());
            try {
                config.addConfiguration(new PropertiesConfiguration(PROPERTIES_NAME));
            } catch (ConfigurationException e) {
                throw new RuntimeException("Exception during build properties", e);
            }
            configuration = config;
        }
    }
}
