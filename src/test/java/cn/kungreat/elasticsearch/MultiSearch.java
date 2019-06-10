package cn.kungreat.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.InetAddress;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MultiSearch {

    @Test
    public void mget(){
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));

            MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                    .add("test", "one", "z9LDzGkBDqgFFGUoXcWd","ztK3zGkBDqgFFGUoT8XQ")
                    .add("test", "one", "8")
                    .get();

/*            MultiGetResponse 实现了 Iterable<MultiGetItemResponse>  可以使用 增强for
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                }
            }*/
            // 使用普通for
            MultiGetItemResponse[] responses = multiGetItemResponses.getResponses();
            for(int x=0;x<responses.length;x++){
                GetResponse response = responses[x].getResponse();
                if (response != null && response.isExists()) {
                    String json = response.getSourceAsString();
                    System.out.println(json);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }
    @Test
    public void bulk(){
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            //批量添加  也可使用  map 和对象bean
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            bulkRequest.add(client.prepareIndex("test", "one", "10")
                    .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "first")
                            .field("birthday", new Date())
                            .field("context", "trying out Elasticsearch")
                            .endObject()
                    )
            );
            bulkRequest.add(client.prepareIndex("test", "one", "11")
                    .setSource(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "who")
                            .field("birthday", new Date())
                            .field("context", "another post")
                            .endObject()
                    )
            );
            BulkResponse bulkResponse = bulkRequest.get();
            System.out.println(bulkResponse.status());
            if (bulkResponse.hasFailures()) {
                System.out.println("errors");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void delete(){
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchQuery("name", "first"))
                    .source("test").get();  //source("test")  指定索引
            long deleted = bulkByScrollResponse.getDeleted();
            System.out.println(deleted); //删除的个数

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void matchAll(){
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            //matchAllQuery
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            SearchResponse searchResponse = client.prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                    .setSize(2).setFrom(1).get();
            System.out.println(searchResponse);

            //multiMatchQuery    一个值在多个field 中
            SearchResponse searchResponse1 = client.prepareSearch("test").setQuery(QueryBuilders.multiMatchQuery("hello", "context","name"))
                    .setSize(2).setFrom(0).get();
            System.out.println(searchResponse1);

            // termQuery
            SearchResponse searchResponse2 = client.prepareSearch("test").setQuery(QueryBuilders.termQuery("name", "kun")).get();
            System.out.println(searchResponse2);
            // termsQuery
            SearchResponse searchResponse3 = client.prepareSearch("test").setQuery(QueryBuilders.termsQuery("name", "kun", "who")).get();
            System.out.println(searchResponse3);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void rangQuery(){
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            // rangeQuery
            SearchResponse searchResponse = client.prepareSearch("test").setQuery(QueryBuilders.rangeQuery("birthday")
            .from("2019-03-29").to("2019-03-31").format("yyyy-MM-dd")).get();
            System.out.println(searchResponse);

            // prefixQuery 前缀查询  name 中以 K 开头的
            SearchResponse searchResponse1 = client.prepareSearch("test").setQuery(QueryBuilders.prefixQuery("name", "k"))
                    .get();
            System.out.println(searchResponse1);

            // wildcardQuery  模糊查询 支持 ? * 通配符
            SearchResponse searchResponse2 = client.prepareSearch("test").setQuery(QueryBuilders.wildcardQuery("name", "k?n"))
                    .get();
            System.out.println(searchResponse2);
            // fuzzyQuery 自动将拼写错误的搜索文本，进行纠正，纠正以后去尝试匹配索引中的数据 纠正在一定的范围内如果差别大无法搜索出来
            SearchResponse searchResponse3 = client.prepareSearch("test").setQuery(QueryBuilders.fuzzyQuery("name", "knu"))
                    .get();
            System.out.println(searchResponse3);
            // idsQuery   多个ID
            SearchResponse searchResponse4 = client.prepareSearch("test").setQuery(QueryBuilders.idsQuery().addIds("1", "8"))
                    .get();
            System.out.println(searchResponse4);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }
    @Test
    public void aggregation(){
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            // AggregationBuilders.max("Max")  返回的field 名称
            SearchResponse searchResponse = client.prepareSearch("test").addAggregation(AggregationBuilders.max("Max")
                    .field("age"))
                    .get();
            System.out.println(searchResponse);
            // terms 分组 返回field 值 和统计个数
            SearchResponse searchResponse1 = client.prepareSearch("test").addAggregation(AggregationBuilders.terms("terms")
                    .field("age"))
                    .get();
            System.out.println(searchResponse1);

            // filter  过滤后 统计个数
            SearchResponse searchResponse2 = client.prepareSearch("test").addAggregation(AggregationBuilders.filter("filter",
                    QueryBuilders.termQuery("context", "hello"))).get();
            System.out.println(searchResponse2);
            // filters  过滤后 统计个数
            SearchResponse searchResponse3 = client.prepareSearch("test").addAggregation(AggregationBuilders.filters("filter",
                    new FiltersAggregator.KeyedFilter("context", QueryBuilders.termQuery("context", "hello")),
                    new FiltersAggregator.KeyedFilter("age", QueryBuilders.termQuery("age", "18")))).get();
            System.out.println(searchResponse3);
            // range
            SearchResponse searchResponse4 = client.prepareSearch("test").addAggregation(AggregationBuilders.range("range")
                    .field("age").addUnboundedTo(50).addRange(10, 20).addUnboundedFrom(10)).get();
            System.out.println(searchResponse4);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }
    @Test
    public void query(){
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            //  queryStringQuery  AND 边接  根据值 去查询指定索引下所有数据  + 必须有  -表示不能有   是查所有field
            SearchResponse searchResponse = client.prepareSearch("test").setQuery(QueryBuilders.queryStringQuery("+another -who"))
                    .get();
            System.out.println(searchResponse);
            // simpleQueryStringQuery  和上边一样  但是 or 连接
            SearchResponse searchResponse1 = client.prepareSearch("test").setQuery(QueryBuilders.simpleQueryStringQuery("+another -who"))
                    .get();
            System.out.println(searchResponse1);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(client != null){
                client.close();
            }
        }
    }


 /*   QueryBuilders.termsQuery("tags",      // field
            "blue", "pill")                 // values
            .minimumMatch(1);               // How many terms must match
 */


    @Test
    public void boolQuery(){
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            // boolQuery 多条件拼接
            SearchResponse searchResponse = client.prepareSearch("test").setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.idsQuery().addIds("8","ztK3zGkBDqgFFGUoT8XQ")).mustNot(QueryBuilders.idsQuery().addIds("1")).should(QueryBuilders.idsQuery().addIds("11"))
                        .filter(QueryBuilders.matchQuery("name","kun")))
                    .get();
            System.out.println(searchResponse);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(client != null){
                client.close();
            }
        }
    }
}