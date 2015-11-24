package sampleelasticsearch;

import java.net.InetAddress;
import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class IndexerSearcher {

	public IndexerSearcher() {
		
		// TODO Auto-generated constructor stub
		
		
	}
	
	public static void main(String[] args){
		try{
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", "thor").build();
		Client client = TransportClient.builder().settings(settings).build()
						.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		
		/*IndexResponse response = client.prepareIndex("twitter", "tweet", "qwes-2hue-hyt3-olk8")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("name", "SQL Performance")
		                        .field("technology","SQL")
		                        .field("item","Performance")
		                        .field("description", "Manages DB performance.")
		                        .field("Highlight","Recommended by Westpac and BTFG")
		                    .endObject()
		                  )
		        .get();
		
		// Index name
		String _index = response.getIndex();
		// Type name
		String _type = response.getType();
		// Document ID (generated or not)
		String _id = response.getId();
		// Version (if it's the first time you index this document, you will get: 1)
		long _version = response.getVersion();
		// isCreated() is true if the document is a new one, false if it has been updated
		boolean created = response.isCreated();
		
		System.out.println("index: " + _index +" type: "+ _type+" id: "+_id+" created:" +created);
		*/
		String queryString = "linux";

		String allField = "_all";

		MultiMatchQueryBuilder mmqb = QueryBuilders.multiMatchQuery(queryString, allField);
		
		RefreshResponse rsr= client.admin()
		  .indices()
		  .prepareRefresh()
		  .execute()
		  .actionGet();
		
		System.out.println(rsr.getSuccessfulShards());
		
		SearchResponse response1 = client.prepareSearch("twitter")
                //.setTypes("tweet")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(mmqb)
                .setExplain(true)
                .execute()
                .actionGet();
		
        SearchHit[] results = response1.getHits().getHits();

        System.out.println("Current results: " + results.length);

        for (SearchHit hit : results) {

            System.out.println("------------------------------");

            Map<String,Object> result = hit.getSource();   

            System.out.println("Data: "+result.get("name"));

        }
        

        boolean indexExists = client.admin().indices().prepareExists("twitter").execute().actionGet().isExists();
        System.out.println("IndexExists"+ indexExists);

        SearchResponse allHits = client.prepareSearch("twitter")
                                    .addFields("user", "message","postDate")
                                    //.setQuery(mmqb)
                                    .setQuery(QueryBuilders.matchAllQuery())
                                    .execute().actionGet();
		
        SearchHit[] results1 = allHits.getHits().getHits();

        System.out.println("Current results: " + results1.length);

       
        
        for (SearchHit hit : results1) {

            System.out.println("------------------------------");
           // System.out.println(results1[0].getInnerHits().keySet().toArray());
            
            
            
            Map<String,Object> result = hit.sourceAsMap();
            Map<String, SearchHits> result1 = hit.getInnerHits();
            
            System.out.println("Source Exists or not: "+ hit.isSourceEmpty());
            
            if (hit.getFields().containsKey("item")) {
                System.out.println("field.title: " 
                                  + hit.getFields().get("item").getValue());
                }
                //System.out.println("source.title: " 
                //                   + hit.getSource().get("user"));
                
          //  System.out.println(result1.get("user"));
           //System.out.println(hit.getFields().get("user").getValue() + " "+ hit.getFields().get("postDate").getValue() +" "+ hit.getFields().get("message").getValue());

        }
        
        
		client.close();
		}
		catch(Exception e){
			System.out.println("eror" + e.getMessage());
		}
	}

}
