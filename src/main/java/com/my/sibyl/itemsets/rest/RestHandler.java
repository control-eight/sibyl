package com.my.sibyl.itemsets.rest;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author abykovsky
 * @since 6/10/15
 */
public class RestHandler implements Handler<Buffer> {

    private static final Log LOG = LogFactory.getLog(SibylVerticle.class);

    private HttpServerRequest request;

    private Handler<Buffer> targetHandler;

    public RestHandler(HttpServerRequest request, Handler<Buffer> targetHandler) {
        this.request = request;
        this.targetHandler = targetHandler;
    }

    public void handle(Buffer event) {
        try {
            this.targetHandler.handle(event);
        } catch (Throwable e) {
            exceptionHandle(request, e);
        }
    }

    private void exceptionHandle(HttpServerRequest request, Throwable e) {
        LOG.error(e, e);

        StringWriter out = new StringWriter();
        PrintWriter printWriter = new PrintWriter(out);
        e.printStackTrace(printWriter);
        request.response().setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
        request.response().end(out.toString());
        printWriter.close();
    }
}
