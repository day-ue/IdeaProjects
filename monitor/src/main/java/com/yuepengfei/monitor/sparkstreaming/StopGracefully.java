package com.yuepengfei.monitor.sparkstreaming;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.spark_project.jetty.server.Request;
import org.spark_project.jetty.server.Server;
import org.spark_project.jetty.server.handler.AbstractHandler;
import org.spark_project.jetty.server.handler.ContextHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
@Slf4j
public class StopGracefully {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        JavaReceiverInputDStream<String> dStream = creatStream(ssc);
        dStream.print();

        stop(ssc);
        ssc.start();
        ssc.awaitTermination();
    }

    private static void stop(JavaStreamingContext ssc) throws Exception {
        Server server = new Server(10000);
        ContextHandler context = new ContextHandler();
        context.setContextPath("/close");
        context.setHandler(new AbstractHandler() {
            @Override
            public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
                log.warn("开始关闭......");
                ssc.stop(true,true);//优雅的关闭
                httpServletResponse.setContentType("text/html; charset=utf-8");
                httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                PrintWriter writer = httpServletResponse.getWriter();
                writer.println("close success");
                request.setHandled(true);
                log.warn("关闭成功.....");
                try {
                    server.stop();
                } catch (Exception e) {
                    System.out.println("这里报打断异常属于正常现象");
                }
            }
        });
        server.setHandler(context);
        server.start();
    }

    private static JavaReceiverInputDStream<String> creatStream(JavaStreamingContext ssc) {
        JavaReceiverInputDStream<String> dStream = ssc.receiverStream(new Receiver<String>(StorageLevel.MEMORY_AND_DISK_2()) {
            volatile boolean flag = true;

            @Override
            public void onStart() {
                new Thread(new Runnable() {
                    @SneakyThrows
                    @Override
                    public void run() {
                        String[] words = {"spark", "flink", "elasticsearch", "hbase", "mysql"};
                        Random random = new Random();
                        while (flag) {
                            store(words[random.nextInt(5)]);
                            Thread.sleep(100);

                        }
                    }
                }).start();
            }

            @Override
            public void onStop() {
                flag = false;
                System.out.println("停止接受数据");
            }
        });
        return dStream;
    }

}
