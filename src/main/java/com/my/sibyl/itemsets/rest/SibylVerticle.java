package com.my.sibyl.itemsets.rest;

import com.my.sibyl.itemsets.AssociationService;
import com.my.sibyl.itemsets.AssociationServiceImpl;
import com.my.sibyl.itemsets.rest.binding.TransactionBinding;
import com.my.sibyl.itemsets.score_function.BasicScoreFunction;
import com.my.sibyl.itemsets.score_function.ConfidenceRecommendationFilter;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.codehaus.jackson.map.ObjectMapper;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author abykovsky
 * @since 5/27/15
 */
public class SibylVerticle extends Verticle {

    private static final Log LOG = LogFactory.getLog(SibylVerticle.class);

    private HttpServer server;

    private AssociationService associationService;

    private HConnection connection;

    @Override
    public void start() {
        server = vertx.createHttpServer();

        System.out.println("Connecting to HBase...");
        Configuration myConf = HBaseConfiguration.create();
        try {
            connection = HConnectionManager.createConnection(myConf);
            associationService = new AssociationServiceImpl(connection);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RouteMatcher matcher = new RouteMatcher();
        matcher.get("/api/associations/v1/transactions/", request -> {
            request.response().end(request.uri() + " works!");
        });
        matcher.post("/api/v1/transactions/:instance", this::handleAddTransaction);
        matcher.get("/api/v1/recommendations/:instance/:basketItems", this::handleGetRecommendations);
        matcher.get("/api/v1/test/recommendations/:instance", this::handleGetTestRecommendations);
        server.requestHandler(matcher).listen(8080, "localhost");

        LOG.info("Successfully started.");
    }

    private void handleAddTransaction(final HttpServerRequest request) {

        request.bodyHandler(buffer -> {

            LOG.info("Request comes");

            String instance = request.params().get("instance");

            ObjectMapper mapper = new ObjectMapper();
            TransactionBinding transactionBinding;
            try {
                transactionBinding = mapper.readValue(buffer.getBytes(), TransactionBinding.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            LOG.info(transactionBinding);

            try {
                associationService.addTransactionBinding(instance, transactionBinding);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            request.response().end("Transaction " + transactionBinding.getId() + " to instance \"" + instance
                    + "\" was added successfully!");
        });

        request.exceptionHandler(throwable -> {
            LOG.error(throwable, throwable);

            PrintWriter printWriter = new PrintWriter(new StringWriter());
            throwable.printStackTrace(printWriter);
            request.response().end(printWriter.toString());
            printWriter.close();
        });
    }

    private void handleGetRecommendations(final HttpServerRequest request) {
        request.bodyHandler(buffer -> {

            LOG.info("Request comes");

            String instance = request.params().get("instance");
            String[] basketItemses = StringUtils.split(request.params().get("basketItems"), ",");

            if(basketItemses == null || basketItemses.length == 0)
                throw new RuntimeException("Basket items can't be empty!");

            List<String> basketItems = Arrays.asList(basketItemses);

            ScoreFunction<Recommendation> scoreFunction = createBasicScoreFunction();

            try {
                List<ScoreFunctionResult<String>> results = associationService
                        .getRecommendations(instance, basketItems, scoreFunction);

                ObjectMapper mapper = new ObjectMapper();

                StringWriter out = new StringWriter();
                PrintWriter printWriter = new PrintWriter(out);
                mapper.writeValue(printWriter, results);

                request.response().end(out.toString());

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        request.exceptionHandler(throwable -> {
            LOG.error(throwable, throwable);

            PrintWriter printWriter = new PrintWriter(new StringWriter());
            throwable.printStackTrace(printWriter);
            request.response().end(printWriter.toString());
            printWriter.close();
        });
    }

    private ScoreFunction<Recommendation> createBasicScoreFunction() {
        final boolean isLiftInUse = true;
        final double confidence = 0.0005;
        //final double confidence = 0.1;
        final int maxResults = 10;
        return new BasicScoreFunction(maxResults,
                Collections.singletonList(new ConfidenceRecommendationFilter() {
                    @Override
                    public boolean filter(Double value) {
                        return value < confidence;
                    }
                }), isLiftInUse);
    }

    private void handleGetTestRecommendations(final HttpServerRequest request) {

        LOG.info("Request comes");
        String count = request.params().get("count");
        String instance = request.params().get("instance");

        try {
            Map<String, Long> results = associationService.getItemSetWithCountMore(instance,
                    Integer.parseInt(count));
            ObjectMapper mapper = new ObjectMapper();

            StringWriter out = new StringWriter();
            PrintWriter printWriter = new PrintWriter(out);
            mapper.writeValue(printWriter, results);

            request.response().end(out.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            if(this.connection == null) return;

            this.connection.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
