package com.yuepengfei.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * es更新比较快，api不稳定。此工具类基于6.4版本的rest风格
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.4/java-rest-high-getting-started-maven.html
 */

public class EsUtils {

    /**
     * 初始化客户端
     * @return RestHighLevelClient
     */
    public static RestHighLevelClient getClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.240.131", 9200, "http")));
        return client;
    }


    //增
    //删

    /**
     * 6.4版本并没有支持deletebyquery, 这里自己实现，先查询，批量提交删除
     * @param client
     * @param index
     * @param typeDoc
     * @param key
     * @param value
     */
    public static int deleteByQuery(RestHighLevelClient client, String index, String typeDoc, String key, String value) throws IOException {
        HashMap<String, String> hm = queryTermScroll(client, index, typeDoc, key, value);
        BulkRequest bulkRequest = new BulkRequest();
        for (String id : hm.keySet()) {
            bulkRequest.add(new DeleteRequest(index, typeDoc, id));
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        int status = bulkResponse.status().getStatus();
        return status;
    }

    //改
    //查

    /**
     * 底层默认就返回十条数据，因此这个api我们并不常用，我们多用分页查询
     * @param client
     * @param index
     * @param typeDoc
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> queryTerm(RestHighLevelClient client, String index, String typeDoc,String key, String value) throws IOException {
        SearchRequest searchRequest = new SearchRequest().indices(index).types(typeDoc);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders
                .termQuery(key,value))
                .from(0)
                .size(10)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);

        HashMap<String, String> hm = new HashMap<>();
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            String id = hit.getId();
            String sourceAsString = hit.getSourceAsString();
            hm.put(id, sourceAsString);
        }
        return hm;
    }

    /**
     * 分页查询
     * @param client
     * @param index
     * @param typeDoc
     * @param key
     * @param value
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> queryTermScroll(RestHighLevelClient client, String index, String typeDoc, String key, String value) throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(index).types(typeDoc);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(key, value));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        HashMap<String, String> hm = new HashMap<>();

        while (searchHits != null && searchHits.length > 0) {
            for (SearchHit searchHit : searchHits) {
                hm.put(searchHit.getId(), searchHit.getSourceAsString());
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        return hm;
    }


    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = EsUtils.getClient();
        HashMap<String, String> hm = EsUtils.queryTermScroll(client, "word_time_flag", "doc", "word", "spark");
        for (String key : hm.keySet()) {
            System.out.println(key+"    --> "+hm.get(key));
        }
        client.close();
    }


}
