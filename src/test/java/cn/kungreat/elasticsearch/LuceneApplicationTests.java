package cn.kungreat.elasticsearch;

import cn.kungreat.elasticsearch.domain.One;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.util.DateUtil;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class LuceneApplicationTests {



/*  创建 mapping
     PUT  http://192.168.147.128:9200/test
    {
        "settings":{
            "number_of_shards":3,
                    "number_of_replicas":0
        },
            "mappings":{
            "one":{
                "properties":{
                    "id":{
                        "type":"long"
                    },
                    "title":{
                        "type":"keyword"
                    },
                    "context":{
                        "type":"text",
                                "analyzer":"ik_max_word"
                    },
                    "name":{
                        "type":"text",
                                "analyzer":"ik_max_word"
                    },
                    "age":{
                        "type":"long"
                    },
                    "birthday":{
                        "type":"date"
                    }
                }
            }
        }
    }*/


    @Test
    public void contextLoads() {
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//            client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            GetResponse response = client.prepareGet("kun", "node", "1").get();
            System.out.println(response.getSourceAsString());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void noCluster(){
        // 没有集群的时候   当你有设置集群名称的时候 就要使用集群的设置
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
        try {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("host1"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("host2"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void add(){
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
//          client.addTransportAddresses()  可以添加多个客户端
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));

/*            map 方式添加
            Map<String, Object> json = new HashMap<>();
            json.put("id",2);
            json.put("title","java2");
            json.put("context","hard work");
            json.put("age","18");
            json.put("birthday","2019-03-30");
            json.put("name","kun");
//            client.prepareIndex("test", "one","1")  可以指定ID 不指定由ES自动生成
            IndexResponse response = client.prepareIndex("test", "one")
                    .setSource(json, XContentType.JSON)
                    .get();*/

            One one = new One();
            one.setAge(28);
            one.setBirthday(DateUtil.now());
            one.setContext("this is dreams");
            // instance a json mapper
            ObjectMapper mapper = new ObjectMapper(); // create once, reuse
            // generate json  client.prepareIndex("test", "one","1")  可以指定ID 不指定由ES自动生成
            byte[] json = mapper.writeValueAsBytes(one);
            IndexResponse response = client.prepareIndex("test", "one","2")
                    .setSource(json, XContentType.JSON)
                    .get();
            System.out.println(response.status());
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
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            DeleteResponse response = client.prepareDelete("test", "one", "1").get();
            System.out.println(response.status());
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void update(){
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index("test");
            updateRequest.type("one");
            updateRequest.id("ztK3zGkBDqgFFGUoT8XQ");
            // 也可以使用 map 和 对象bean 作为传值  请参考   add() 方法
            updateRequest.doc(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("context", "hello es")
                    .endObject());
            UpdateResponse updateResponse = client.update(updateRequest).get();
            System.out.println(updateResponse.status());
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }

    @Test
    public void upsert(){
        // ES 集群设置 指定集群名称
        Settings settings = Settings.builder().put("cluster.name", "Kun").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.147.128"), 9300));
            // 也可以使用 map 和 对象bean 作为传值  请参考   add() 方法
            IndexRequest indexRequest = new IndexRequest("test", "one", "8")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("age", "30")
                            .field("context","hello - word")
                            .endObject());
            UpdateRequest updateRequest = new UpdateRequest("test", "one", "8")
                    .doc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("name", "Joe Smith")
                            .field("age", "30")
                            .field("context","hello - word")
                            .endObject())
                    .upsert(indexRequest);
            UpdateResponse updateResponse = client.update(updateRequest).get();
            System.out.println(updateResponse.status());
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(client != null){
                client.close();
            }
        }
    }
}