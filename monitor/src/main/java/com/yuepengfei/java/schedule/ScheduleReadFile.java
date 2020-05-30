package com.yuepengfei.java.schedule;

import com.yuepengfei.monitor.es.EsUtils;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ScheduleReadFile {
    public static void main(String[] args) {

        //获取当前时间
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime time = now.withHour(0).withMinute(0).withSecond(0).withNano(0);
        if (now.compareTo(time) > 0) {
            time = time.plusDays(1);
        }


        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(() -> {
                    try {
                        readFile("D:\\code\\day_ue\\mylearn");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                , 1 //Duration.between(now, time).toMillis()
                , 1000 * 60 * 60 * 24
                , TimeUnit.MILLISECONDS);
    }

    static void readFile(String path) throws IOException {

        File[] files = new File(path).listFiles();
        ArrayList<HashMap<String, String>> hashMaps = new ArrayList<>();

        for (File file : files) {
            if (file.isFile() && file.getName().contains("md")) {
                String fileName = file.getName();
                int fileNameHashCode = file.getName().hashCode();
                System.out.println(fileName);
                System.out.println(fileNameHashCode);
                FileReader fileReader = null;
                BufferedReader bufferedReader = null;
                StringBuilder result = new StringBuilder();
                try {
                    fileReader = new FileReader(file);
                    bufferedReader = new BufferedReader(fileReader);
                    String tmpStr = null;
                    while ((tmpStr = bufferedReader.readLine()) != null) {
                        result.append("\r\n");
                        result.append(tmpStr);
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                HashMap<String, String> hashMap = new HashMap<>();
                hashMap.put("fileName", fileName);
                hashMap.put("key", String.valueOf(fileNameHashCode));
                hashMap.put("fileNameHashCode", String.valueOf(fileNameHashCode));
                hashMap.put("document", result.toString());
                hashMaps.add(hashMap);
            } else if (file.isDirectory()){
                readFile(file.getAbsolutePath());
            } else {
                System.out.println(file.getName() + "非采集文件");
            }
        }
        ;
        //将结果写入到es
        System.out.println(hashMaps);
        RestHighLevelClient client = EsUtils.getClient("192.168.124.131");
        int i = EsUtils.addBulk(client, "my_document", "doc", hashMaps);
        System.out.println(i);
        client.close();
    }


}
