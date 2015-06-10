package com.my.sibyl.itemsets.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.matcher.Matchers;
import com.my.sibyl.itemsets.dao.InstancesDao;
import com.my.sibyl.itemsets.dao.ItemSetsDao;
import com.my.sibyl.itemsets.dao.TransactionsDao;
import com.my.sibyl.itemsets.hbase.dao.InstancesDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.ItemSetsDaoImpl;
import com.my.sibyl.itemsets.hbase.dao.TransactionsDaoImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author abykovsky
 * @since 6/9/15
 */
public class AppModule extends AbstractModule {

    private static final Log LOG = LogFactory.getLog(AppModule.class);

    @Override
    protected void configure() {
        /*MonitorInterceptor monitorInterceptor = new MonitorInterceptor();
        requestInjection(monitorInterceptor);
        bindInterceptor(Matchers.subclassesOf(ItemSetsDaoImpl.class), Matchers.any(), monitorInterceptor);*/

        bind(InstancesDao.class).to(InstancesDaoImpl.class);
        bind(ItemSetsDao.class).to(ItemSetsDaoImpl.class);
        bind(TransactionsDao.class).to(TransactionsDaoImpl.class);
    }

    @Provides
    public HConnection provideHConnection() throws IOException {
        LOG.info("Connecting to HBase...");
        Configuration myConf = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(myConf);
        LOG.info("Connected.");
        return connection;
    }

    @Provides
    public ObjectMapper provideObjectMapper() {
        ObjectMapper result = new ObjectMapper();
        SimpleModule testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
        testModule.addSerializer(Double.class, new JsonSerializer<Double>() {
            @Override
            public void serialize(Double vDouble, JsonGenerator jsonGenerator,
                                  SerializerProvider serializerProvider)
                    throws IOException {
                jsonGenerator.writeNumber(BigDecimal.valueOf(vDouble.doubleValue())
                        .setScale(5, BigDecimal.ROUND_UP).doubleValue());
            }
        });
        result.registerModule(testModule);
        return result;
    }
}
