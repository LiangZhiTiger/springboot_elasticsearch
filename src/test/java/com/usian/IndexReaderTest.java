package com.usian;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexReaderTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest searchRequest;

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

    @Before
    public void initSearchRequest(){
        searchRequest=new SearchRequest("java1906").types("course");
    }

    //搜索全部文档
    @Test
    public void testSearchAll() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayDoc(searchResponse);
    }

    //分页查询
    @Test
    public void testSearchPage() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("price", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayDoc(searchResponse);
    }

    //match查询
    @Test
    public void testMatchQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        displayDoc(searchResponse);
    }

    //MultiMatch查询,跨field查询
    @Test
    public void testMultiMatchQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发",new String[]{"name","description"}));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        displayDoc(searchResponse);
    }

    //多个field满足条件查询
    @Test
    public void testBooleanQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //must
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("description","开发"));

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayDoc(searchResponse);
    }

    //区间查询
    @Test
    public void testFilterQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(6).lte(10));

        SearchSourceBuilder querySourceBuilder = searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(querySourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        displayDoc(searchResponse);
    }


    public void displayDoc(SearchResponse searchResponse){
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
