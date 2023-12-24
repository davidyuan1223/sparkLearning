package org.apache.spark.network.client;

import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TransportClientFactorySuite {
    private TransportConf conf;
    private TransportContext context;
    private TransportServer server1;
    private TransportServer server2;

    @Before
    public void setUp(){
        conf = new TransportConf("shuffle", MapConfigProvider.EMPTY);
        NoOpRpcHandler rpcHandler = new NoOpRpcHandler();
        context=new TransportContext(conf,rpcHandler);
        server1=context.createServer();
        server2=context.createServer();
    }

    @After
    public void tearDown(){
        JavaUtils.closeQuietly(server1);
        JavaUtils.closeQuietly(server2);
        JavaUtils.closeQuietly(context);
    }

    private void testClientReuse(int maxConnections, boolean concurrent) throws IOException, InterruptedException {
        Map<String ,String > configMap=new HashMap<>();
        configMap.put("spark.shuffle.io.numConnectionsPerPeer",Integer.toString(maxConnections));
        TransportConf conf = new TransportConf("shuffle", new MapConfigProvider(configMap));
        NoOpRpcHandler rpcHandler = new NoOpRpcHandler();
        try(TransportContext context = new TransportContext(conf, rpcHandler)) {
            TransportClientFactory factory = context.createClientFactory();
            Set<TransportClient> clients=Collections.synchronizedSet(new HashSet<>());
            AtomicInteger failed = new AtomicInteger();
            Thread[] attempts = new Thread[maxConnections * 10];
            for (int i = 0; i < attempts.length; i++) {
                attempts[i]=new Thread(()->{
                    try {
                        TransportClient client = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
                        assertTrue(client.isActive());
                        clients.add(client);
                    }catch (IOException e){
                        failed.incrementAndGet();
                    }catch (InterruptedException e){
                        throw new RuntimeException(e);
                    }
                });
                if (concurrent) {
                    attempts[i].start();
                }else {
                    attempts[i].run();
                }
            }
            for (Thread attempt : attempts) {
                attempt.join();
            }
            Assert.assertEquals(0,failed.get());
            Assert.assertTrue(clients.size()<=maxConnections);
            for (TransportClient client : clients) {
                client.close();
            }
            factory.close();
        }
    }

    @Test
    public void reuseClientsUpToConfigVariable() throws Exception{
        testClientReuse(1,false);
        testClientReuse(2,false);
        testClientReuse(3,false);
        testClientReuse(4,false);
    }

    @Test
    public void reuseClientsUpToConfigVariableConcurrent()throws Exception{
        testClientReuse(1,true);
        testClientReuse(2,true);
        testClientReuse(3,true);
        testClientReuse(4,true);
    }

    @Test
    public void returnDifferentClientsForDifferentServers()throws IOException,InterruptedException{
        TransportClientFactory factory = context.createClientFactory();
        TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
        TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
        assertTrue(c1.isActive());
        assertTrue(c2.isActive());
        assertNotSame(c1,c2);
        factory.close();
    }


    @Test
    public void neverReturnInactiveClients() throws Exception{
        TransportClientFactory factory = context.createClientFactory();
        TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
        c1.close();
        long start = System.currentTimeMillis();
        while (c1.isActive() && (System.currentTimeMillis() - start) < 3000) {
            Thread.sleep(10);
        }
        assertFalse(c1.isActive());
        TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
        assertNotSame(c1,c2);
        assertTrue(c2.isActive());
        factory.close();
    }

    @Test
    public void closeBlockClientsWithFactory()throws Exception{
        TransportClientFactory factory = context.createClientFactory();
        TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
        TransportClient c2 = factory.createClient(TestUtils.getLocalHost(), server2.getPort());
        assertTrue(c1.isActive());
        assertTrue(c2.isActive());
        factory.close();
        assertFalse(c1.isActive());
        assertFalse(c2.isActive());
    }

    @Test
    public void closeIdleConnectionForRequestTimeOut()throws Exception{
        TransportConf conf = new TransportConf("shuffle", new ConfigProvider() {
            @Override
            public String get(String name) {
                if ("spark.shuffle.io.connectionTimeout".equals(name)) {
                    return "1s";
                }
                String value = System.getProperty(name);
                if (value == null) {
                    throw new NoSuchElementException();
                }
                return value;
            }

            @Override
            public Iterable<Map.Entry<String, String>> getAll() {
                throw new UnsupportedOperationException();
            }
        });
        try(
                TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
                TransportClientFactory factory = context.createClientFactory()
        ){
            TransportClient c1 = factory.createClient(TestUtils.getLocalHost(), server1.getPort());
            assertTrue(c1.isActive());
            long expiredTime = System.currentTimeMillis() + 10000;
            while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
                Thread.sleep(10);
            }
            assertFalse(c1.isActive());
        }
    }

    @Test
    public void closeFactoryBeforeCreateClient() throws IOException {
        TransportClientFactory factory = context.createClientFactory();
        factory.close();
        Assert.assertThrows(IOException.class,
                ()-> factory.createClient(TestUtils.getLocalHost(),server1.getPort()));
    }

    @Test
    public void fastFailConnectionInTimeWindow() throws IOException {
        TransportClientFactory factory = context.createClientFactory();
        TransportServer server = context.createServer();
        int unreachablePort = server.getPort();
        server.close();
        Assert.assertThrows(IOException.class,
                ()->factory.createClient(TestUtils.getLocalHost(),unreachablePort,true));
        Assert.assertThrows("fail this connection directly",IOException.class,
                ()->factory.createClient(TestUtils.getLocalHost(),unreachablePort,true));
    }
}
