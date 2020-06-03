package com.usian;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.naming.directory.SearchResult;
import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexReaderTest {

    @Autowired
    public RestHighLevelClient restHighLevelClient;

    //搜索指定id文档
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("java1906","course","1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        boolean exists = getResponse.isExists();
        System.out.println(exists);
        String sourceAsString = getResponse.getSourceAsString();
        System.out.println(sourceAsString);
    }

    //搜索全部文档
    @Test
    public void testSearchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("java1906").types("course");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println("共搜索到"+hits.getTotalHits()+"条文档");

        SearchHit[] hits1 = hits.getHits();
        for (int i = 0; i < hits1.length; i++) {
            SearchHit documentFields = hits1[i];
            System.out.println(documentFields.getId());
            System.out.println(documentFields.getSourceAsString());
        }
    }
}
