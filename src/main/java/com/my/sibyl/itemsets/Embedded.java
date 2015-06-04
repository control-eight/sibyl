package com.my.sibyl.itemsets;

import com.my.sibyl.itemsets.rest.SibylVerticle;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxFactory;

/**
 * @author abykovsky
 * @since 5/27/15
 */
public class Embedded {

    public static void main(String[] args) throws Exception {

        ConfigurationHolder.getConfiguration();

        Vertx vertx = VertxFactory.newVertx();

        String host = args.length == 0? null: args[0];
        SibylVerticle sibylVerticle = new SibylVerticle(host);
        sibylVerticle.setVertx(vertx);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                sibylVerticle.stop();
            }
        });

        sibylVerticle.start();

        // Prevent the JVM from exiting
        while (true) {
            System.in.read();
        }
    }
}
