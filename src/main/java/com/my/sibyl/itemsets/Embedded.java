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

        Vertx vertx = VertxFactory.newVertx();

        SibylVerticle sibylVerticle = new SibylVerticle();
        sibylVerticle.setVertx(vertx);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                sibylVerticle.stop();
            }
        });

        sibylVerticle.start();

        // Prevent the JVM from exiting
        System.in.read();
    }
}
