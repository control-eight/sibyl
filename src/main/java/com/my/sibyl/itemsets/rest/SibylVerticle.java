package com.my.sibyl.itemsets.rest;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.my.sibyl.itemsets.AssociationService;
import com.my.sibyl.itemsets.AssociationServiceImpl;
import com.my.sibyl.itemsets.ConfigurationHolder;
import com.my.sibyl.itemsets.InstancesService;
import com.my.sibyl.itemsets.InstancesServiceImpl;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.guice.AppInjector;
import com.my.sibyl.itemsets.model.Instance;
import com.my.sibyl.itemsets.model.Measure;
import com.my.sibyl.itemsets.rest.binding.InstanceBinding;
import com.my.sibyl.itemsets.rest.binding.InstanceStatus;
import com.my.sibyl.itemsets.rest.binding.TransactionBinding;
import com.my.sibyl.itemsets.score_function.BasicScoreFunction;
import com.my.sibyl.itemsets.score_function.Recommendation;
import com.my.sibyl.itemsets.score_function.ScoreFunction;
import com.my.sibyl.itemsets.score_function.ScoreFunctionResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.codehaus.jackson.map.ObjectMapper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
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

    private InstancesService instancesService;

    private String host;

    private Integer port;

    private HeartbeatVerticle heartbeatVerticle;

    private HConnection connection;

    private ObjectMapper mapper;

    public SibylVerticle() {
    }

    public SibylVerticle(String host) {
        this.host = host;
    }

    @Override
    public void start() {

        if(host == null) {
            host = ConfigurationHolder.getConfiguration().getString("host");
        }
        if(port == null) {
            port = ConfigurationHolder.getConfiguration().getInt("port");
        }

        server = vertx.createHttpServer();

        //init google guice
        Injector injector = Guice.createInjector(new AppInjector());
        associationService = injector.getInstance(AssociationServiceImpl.class);
        instancesService = injector.getInstance(InstancesServiceImpl.class);
        connection = injector.getInstance(HConnection.class);
        mapper = injector.getInstance(ObjectMapper.class);

        RouteMatcher matcher = new RouteMatcher();
        startHeartBeat(matcher);
        matcher.get("/api/associations/v1/transactions/", request -> {
            request.response().end(request.uri() + " works!");
        });
        matcher.post("/api/v1/transactions/:instance", this::handleAddTransaction);
        matcher.get("/api/v1/recommendations/:instance/:basketItems", this::handleGetRecommendations);
        matcher.post("/api/v1/instances", this::handleAddInstance);
        matcher.get("/api/v1/instances/:instance", this::handleGetInstance);
        matcher.get("/api/v1/test/recommendations/:instance", this::handleGetTestRecommendations);

        LOG.info("Host: " + (host == null ? "default" : host));
        server.requestHandler(matcher).listen(port, (host == null || host.isEmpty()) ? "localhost" : host);

        LOG.info("Successfully started.");
    }

    private void startHeartBeat(RouteMatcher routeMatcher) {
        heartbeatVerticle = new HeartbeatVerticle();
        heartbeatVerticle.setRouteMatcher(routeMatcher);
        heartbeatVerticle.setVertx(getVertx());
        heartbeatVerticle.setContainer(getContainer());

        boolean heartbeatServer = Boolean.TRUE.equals(getContainer().config().getBoolean("heartbeat"));

        if(heartbeatServer) {
            heartbeatVerticle.startMonitorInstance(host);
        } else {
            heartbeatVerticle.startUsualInstance(host);
        }
    }

    private void handleAddTransaction(final HttpServerRequest request) {

        request.bodyHandler(createBodyHandler(request, buffer -> {

            LOG.info("Request comes");

            String instance = request.params().get("instance");

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
        }));
    }

    private void handleGetRecommendations(final HttpServerRequest request) {
        request.bodyHandler(createBodyHandler(request, buffer -> {

            LOG.info("Request comes");

            String instance = request.params().get("instance");
            String[] basketItemses = StringUtils.split(request.params().get("basketItems"), ",");

            if (basketItemses == null || basketItemses.length == 0)
                throw new RuntimeException("Basket items can't be empty!");

            List<String> basketItems = Arrays.asList(basketItemses);

            ScoreFunction scoreFunction = createBasicScoreFunction();

            try {
                List<ScoreFunctionResult<String>> results = associationService
                        .getRecommendations(instance, basketItems, scoreFunction);

                StringWriter out = new StringWriter();
                PrintWriter printWriter = new PrintWriter(out);

                mapper.writeValue(printWriter, results);

                request.response().end(out.toString());

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private ScoreFunction createBasicScoreFunction() {
        ScoreFunction scoreFunction = new BasicScoreFunction(
                Arrays.asList(new ImmutablePair<>(Measure.COUNT, 10),
                        new ImmutablePair<>(Measure.CONFIDENCE, 0.0005)),
                Arrays.asList(Measure.LIFT, Measure.COUNT),
                Arrays.asList(Measure.COUNT, Measure.SUPPORT, Measure.CONFIDENCE, Measure.LIFT),
                10
        );
        return scoreFunction;
    }

    private void handleGetTestRecommendations(final HttpServerRequest request) {

        request.bodyHandler(createBodyHandler(request, buffer -> {

            LOG.info("Request comes");
            String count = request.params().get("count");
            String instance = request.params().get("instance");

            try {
                Map<String, Long> results = associationService.getItemSetWithCountMore(instance,
                        Integer.parseInt(count));

                StringWriter out = new StringWriter();
                PrintWriter printWriter = new PrintWriter(out);
                mapper.writeValue(printWriter, results);

                request.response().end(out.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private void handleAddInstance(final HttpServerRequest request) {
        request.bodyHandler(createBodyHandler(request, buffer -> {

            LOG.info("Request comes");

            InstanceBinding instanceBinding;
            try {
                instanceBinding = mapper.readValue(buffer.getBytes(), InstanceBinding.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            LOG.info(instanceBinding);

            instancesService.createInstance(convertInstance(instanceBinding));

            request.response().end("Instance \"" + instanceBinding.getName() + "\" was created successfully!");
        }));
    }

    private Handler<Buffer> createBodyHandler(final HttpServerRequest request, Handler<Buffer> bodyHandler) {
        return new RestHandler(request, bodyHandler);
    }

    private void handleGetInstance(final HttpServerRequest request) {

        request.bodyHandler(createBodyHandler(request, buffer -> {

            LOG.info("Request comes");
            String instance = request.params().get("instance");

            try {
                Instance instanceObj = instancesService.getInstance(instance);

                StringWriter out = new StringWriter();
                PrintWriter printWriter = new PrintWriter(out);

                InstanceStatus instanceStatus = convertInstanceToStatus(instanceObj);
                instanceStatus.setTransactionsCount(associationService.getTransactionsCount(instance));

                mapper.writeValue(printWriter, instanceStatus);

                request.response().end(out.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private InstanceBinding convertInstance(Instance instanceObj) {
        InstanceBinding instance = new InstanceBinding();
        convertInstance(instanceObj, instance);
        return instance;
    }

    private void convertInstance(Instance instanceObj, InstanceBinding instance) {
        instance.setName(instanceObj.getName());
        instance.setDescription(instanceObj.getDescription());
        instance.setMeasures(instanceObj.getMeasures());
        instance.setDataLoadFiles(instanceObj.getDataLoadFiles());
        instance.setStartLoadDate(instanceObj.getStartLoadDate());
        instance.setEndLoadDate(instanceObj.getEndLoadDate());
        instance.setSlidingWindowSize(instanceObj.getSlidingWindowSize());
    }

    private InstanceStatus convertInstanceToStatus(Instance instanceObj) {
        InstanceStatus instance = new InstanceStatus();
        convertInstance(instanceObj, instance);
        return instance;
    }

    private Instance convertInstance(InstanceBinding instanceBinding) {
        Instance instance = new Instance();
        instance.setName(instanceBinding.getName());
        instance.setDescription(instanceBinding.getDescription());
        instance.setMeasures(instanceBinding.getMeasures());
        instance.setDataLoadFiles(instanceBinding.getDataLoadFiles());
        instance.setStartLoadDate(instanceBinding.getStartLoadDate());
        instance.setEndLoadDate(instanceBinding.getEndLoadDate());
        instance.setSlidingWindowSize(instanceBinding.getSlidingWindowSize());
        return instance;
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
