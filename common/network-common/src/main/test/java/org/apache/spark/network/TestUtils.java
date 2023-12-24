package org.apache.spark.network;

import java.net.InetAddress;

public class TestUtils {
    public static String getLocalHost(){
        try {
            return (System.getenv().containsKey("SPARK_LOCAL_IP"))?
                    System.getenv("SPARK_LOCAL_IP"):
                    InetAddress.getLocalHost().getHostAddress();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
