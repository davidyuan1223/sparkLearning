package org.apache.spark.network.util;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public class NettyMemoryMetrics implements MetricSet {
    private final PooledByteBufAllocator pooledAllocator;
    private final boolean verboseMetricEnabled;
    private final Map<String , Metric> allMetrics;
    private final String metricPrefix;

    @VisibleForTesting
    static final Set<String > VERBOSE_METRICS = new HashSet<>();

    static {
        VERBOSE_METRICS.addAll(Arrays.asList(
                "numAllocations"
                ,"numTinyAllocations"
                ,"numSmallAllocations"
                ,"numNormalAllocations"
                ,"numHugeAllocations"
                ,"numDeallocations,"
                ,"numTinyDeallocations"
                ,"numSmallDeallocations"
                ,"numNormalDeallocations"
                ,"numHugeDeallocations"
                ,"numActiveAllocations"
                ,"numActiveTinyAllocations"
                ,"numActiveSmallAllocations"
                ,"numActiveNormalAllocations"
                ,"numActiveHugeAllocations"
                ,"numActiveBytes"
        ));
    }
    public NettyMemoryMetrics(PooledByteBufAllocator pooledAllocator,
                              String metricPrefix,
                              TransportConf conf){
        this.pooledAllocator=pooledAllocator;
        this.allMetrics=new HashMap<>();
        this.metricPrefix=metricPrefix;
        this.verboseMetricEnabled=conf.verboseMetrics();
        registerMetrics(this.pooledAllocator);
    }

    private void registerMetrics(PooledByteBufAllocator pooledAllocator){
        PooledByteBufAllocatorMetric pooledAllocatorMetric = pooledAllocator.metric();
        allMetrics.put(MetricRegistry.name(metricPrefix,"usedHeapMemory"),
                (Gauge<Long>) pooledAllocatorMetric::usedHeapMemory);
        allMetrics.put(MetricRegistry.name(metricPrefix,"usedDirectoryMemory"),
                (Gauge<Long>)pooledAllocatorMetric::usedDirectMemory);
        if (verboseMetricEnabled) {
            int directArenaIndex=0;
            for (PoolArenaMetric metric : pooledAllocatorMetric.directArenas()) {
                registerMetric(metric,"directArena"+directArenaIndex);
                directArenaIndex++;
            }
            int heapArenaIndex=0;
            for (PoolArenaMetric metric : pooledAllocatorMetric.heapArenas()) {
                registerMetric(metric,"heapArena"+heapArenaIndex);
                heapArenaIndex++;
            }
        }
    }

    private void registerMetric(PoolArenaMetric arenaMetric, String arenaName) {
        for (String methodName : VERBOSE_METRICS) {
            Method m;
            try {
                m=PoolArenaMetric.class.getMethod(methodName);
            }catch (Exception e){
                continue;
            }
            if (!Modifier.isPublic(m.getModifiers())) {
                continue;
            }
            Class<?> returnType = m.getReturnType();
            String metricName = MetricRegistry.name(metricPrefix, arenaName, m.getName());
            if (returnType.equals(int.class)) {
                allMetrics.put(metricName,(Gauge<Integer>)()->{
                    try {
                        return (Integer) m.invoke(arenaMetric);
                    }catch (Exception e){
                        return -1;
                    }
                });
            } else if (returnType.equals(long.class)) {
                allMetrics.put(metricName,(Gauge<Long>)()->{
                    try {
                        return (Long) m.invoke(arenaMetric);
                    }catch (Exception e){
                        return -1L;
                    }
                });
            }
        }
    }

    public Map<String, Metric> getMetrics() {
        return Collections.unmodifiableMap(allMetrics);
    }
}
