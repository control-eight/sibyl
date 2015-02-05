package com.my.sibyl.itemsets.rest;

import com.my.sibyl.itemsets.util.Avro;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;

import java.util.ArrayList;

/**
 * @author abykovsky
 * @since 2/5/15
 */
public class SibylVerticle extends Verticle {

    private HttpServer server;

    private HBaseClient hBaseClient;

    @Override
    public void start() {
        server = vertx.createHttpServer();
        hBaseClient = new HBaseClient("sibylhbase");

        server.requestHandler(request -> {
            makeAsyncHbaseGetRequest(request, hBaseClient);
        }).listen(8080, "localhost");
    }

    @Override
    public void stop() {
        System.out.println("Stop HBaseClient");
        hBaseClient.shutdown();
    }

    private void makeAsyncHbaseGetRequest(final HttpServerRequest request, final HBaseClient hBaseClient) {
        GetRequest getRequest = new GetRequest(Bytes.UTF8("instances"), Avro.charSequenceTyBytes("default"),
                Bytes.UTF8("I"), Bytes.UTF8("C"));

        Deferred<ArrayList<KeyValue>> deferred = hBaseClient.get(getRequest);

        deferred.addCallbacks(arg -> {
            StringBuilder sb = new StringBuilder();
            for (KeyValue keyValue : arg) {
                sb.append(Avro.bytesToInstance(keyValue.value()));
            }
            request.response().end(sb.toString());
            return null;
        }, new Callback<Void, Exception>() {
            @Override
            public Void call(Exception arg) throws Exception {
                getContainer().logger().error(arg.getMessage(), arg);

                return null;
            }
        });
    }
}
