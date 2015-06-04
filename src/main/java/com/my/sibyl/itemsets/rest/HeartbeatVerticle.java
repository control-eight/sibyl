package com.my.sibyl.itemsets.rest;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author abykovsky
 * @since 6/2/15
 */
public class HeartbeatVerticle extends Verticle {

    private static final Log LOG = LogFactory.getLog(HeartbeatVerticle.class);

    private static final int DELAY = 5000;
    private static final int ACTIVE_TIMEOUT = 30_000;
    private static final int VERSION_DIFF = ACTIVE_TIMEOUT / DELAY;

    private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    private AtomicLong instanceId;

    private AtomicLong version = new AtomicLong(0l);

    private static final SimpleDateFormat formatter = new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);

    private HttpServer server;

    private RouteMatcher routeMatcher;

    private String host;

    public void setRouteMatcher(RouteMatcher routeMatcher) {
        this.routeMatcher = routeMatcher;
    }

    public HeartbeatVerticle() {
    }

    @Override
    public void start() {

        boolean heartbeatServer = Boolean.TRUE.equals(getContainer().config().getBoolean("heartbeat"));

        if(heartbeatServer) {
            server = getVertx().createHttpServer();
            startMonitorInstance("localhost");
        } else {
            startUsualInstance("localhost");
        }
    }

    public void startMonitorInstance(String host) {
        this.host = host;
        LOG.info("Start heartbeat server.");
        ConcurrentMap<String, Long> cluster = getHazelcastInstance().getMap("cluster");
        cluster.putIfAbsent("instanceId", 0l);

        long timerID = vertx.setPeriodic(DELAY, timerID1 -> {
            vertx.eventBus().publish("heartbeat", "heartbeatReply " + version.incrementAndGet());
        });
        LOG.info("Timer with id " + timerID + " is started.");

        if(routeMatcher == null) {
            routeMatcher = new RouteMatcher();
        }
        routeMatcher.get("/api/v1/monitor/cluster", this::handleGetClusterInfo);
        if(server != null) {
            server.requestHandler(routeMatcher).listen(8080, (host == null || host.isEmpty()) ? "localhost" : host);
        }
    }

    public void handleGetClusterInfo(HttpServerRequest httpServerRequest) {
        final List<JsonObject> clusterInfo = new ArrayList<>();

        HazelcastInstance hz = getHazelcastInstance();
        ConcurrentMap<Long, byte[]> instances = hz.getMap("instances"); // shared distributed map

        instances.forEach((key, value) -> {
            clusterInfo.add(new JsonObject(new String(value, StandardCharsets.US_ASCII)));
        });

        //TODO too many operation, it could be simlified
        List<JsonObject> inactiveList = new ArrayList<>();
        clusterInfo.stream().filter(jsonObject -> version.get() - jsonObject.getLong("version") > VERSION_DIFF)
                .forEach(inactiveList::add);

        List<JsonObject> activeList = ListUtils.subtract(clusterInfo, inactiveList);

        Collections.sort(activeList, (o1, o2) -> {
            int result = o1.getLong("version").compareTo(o2.getLong("version"));
            if (result != 0) return -result;
            return o1.getLong("id").compareTo(o2.getLong("id"));
        });
        Collections.sort(inactiveList, (o1, o2) -> {
            int result = o1.getLong("version").compareTo(o2.getLong("version"));
            if (result != 0) return -result;
            return o1.getLong("id").compareTo(o2.getLong("id"));
        });

        JsonObject result = new JsonObject();

        JsonArray activeJsonArray = new JsonArray();
        activeList.forEach(activeJsonArray::add);

        JsonArray inactiveJsonArray = new JsonArray();
        inactiveList.forEach(inactiveJsonArray::add);

        result.putArray("activeList", activeJsonArray);
        result.putArray("inactiveList", inactiveJsonArray);

        httpServerRequest.response().end(result.encode());
    }

    public void startUsualInstance(String host) {
        this.host = host;
        LOG.info("Start heartbeat client.");
        vertx.eventBus().registerHandler("heartbeat", new Handler<Message<String>>() {
            @Override
            public void handle(final Message<String> message) {
                final String messageBody = message.body();

                if (instanceId == null) {
                    ConcurrentMap<String, Long> cluster = getHazelcastInstance().getMap("cluster");
                    instanceId = new AtomicLong(cluster.compute("instanceId", (key, value) -> value + 1));
                }
                ConcurrentMap<Long, byte[]> instances = getHazelcastInstance().getMap("instances"); // shared distributed map

                if (!instances.containsKey(instanceId.get())) {
                    JsonObject jsonObject = initInstanceInformation(messageBody, host);
                    instances.putIfAbsent(instanceId.get(), jsonObject.encode().getBytes(StandardCharsets.US_ASCII));
                } else {
                    JsonObject jsonObject = updateInstanceInformation(messageBody, instances);
                    instances.put(instanceId.get(), jsonObject.encode().getBytes(StandardCharsets.US_ASCII));
                }
            }
        }, new AsyncResultHandler<Void>() {
            public void handle(AsyncResult<Void> asyncResult) {
                System.out.println("The handler has been registered across the cluster ok? " + asyncResult.succeeded());
            }
        });
    }

    private JsonObject initInstanceInformation(String messageBody, String host) {
        JsonObject instanceInformation = new JsonObject();
        instanceInformation.putNumber("id", instanceId.get());
        instanceInformation.putString("host", host);
        instanceInformation.putNumber("version", Long.parseLong(messageBody.split(" ")[1]));
        instanceInformation.putString("lastUpdate", formatter.format(new Date()));
        return instanceInformation;
    }

    private JsonObject updateInstanceInformation(String messageBody, ConcurrentMap<Long, byte[]> instances) {
        byte[] value = instances.get(instanceId.get());
        JsonObject jsonObject = new JsonObject(new String(value, StandardCharsets.US_ASCII));
        jsonObject.putNumber("version", Long.parseLong(messageBody.split(" ")[1]));
        jsonObject.putString("lastUpdate", new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS).format(new Date()));
        return jsonObject;
    }

    private HazelcastInstance getHazelcastInstance() {
        Set<HazelcastInstance> hzInstances = Hazelcast.getAllHazelcastInstances();
        return hzInstances.stream().findFirst().get();
    }
}
