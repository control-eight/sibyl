package com.my.sibyl.itemsets.guice;

import com.google.inject.AbstractModule;

/**
 * @author abykovsky
 * @since 6/9/15
 */
public class AppInjector extends AbstractModule {

    @Override
    protected void configure() {
        install(new AppModule());
    }
}
