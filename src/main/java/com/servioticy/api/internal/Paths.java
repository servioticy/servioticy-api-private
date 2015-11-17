package com.servioticy.api.internal;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.servioticy.api.commons.data.*;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;


@Path("/")
public class Paths {

  @Context UriInfo uriInfo;

  @GET
  @Produces("application/json")
  public Response getAllSOs(@Context HttpHeaders hh) {

    String sos = CouchBase.getAllSOs();

    return Response.ok(sos)
             .header("Server", "api.servIoTicy")
                   .header("Date", new Date(System.currentTimeMillis()))
                   .build();
  }

  @Path("/aliveWO")
  @GET
  @Produces("application/json")
  public Response getAllUpdatesLastMinute(@Context HttpHeaders hh) {

    // Get the Service Object Data
    String response = SearchEngine.getAllUpdatesLastMinute();
    
    return Response.ok(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}")
  @GET
  @Produces("application/json")
  public Response getSO(@Context HttpHeaders hh, @PathParam("soId") String soId) {

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    return Response.ok(so.responseGetSO())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/security/{soId}")
  @GET
  @Produces("application/json")
  public Response getSecuritySO(@Context HttpHeaders hh, @PathParam("soId") String soId) {

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    return Response.ok(so.responsePrivateGetSO())
           .header("Server", "api.compose")
           .header("Date", new Date(System.currentTimeMillis()))
           .build();
  }

  @Path("/security/{soId}")
  @PUT
  @Produces("application/json")
  public Response putSecuritySO(@Context HttpHeaders hh, @PathParam("soId") String soId, String body) {

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // Update the Service Object
    so.updateSecurity(body);

    // Store in Couchbase
    CouchBase.setSO(so);

    // Construct the response uri
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI soUri = ub.path(so.getId()).build();

    return Response.ok(soUri)
             .entity(so.responseUpdateSO())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/security/subscriptions/{subsId}")
  @GET
  @Produces("application/json")
  public Response getSecuritySubscription(@Context HttpHeaders hh,
		  			@PathParam("subsId") String subsId) {

    // Get the Service Object
    Subscription subs = CouchBase.getSubscription(subsId);
    if (subs == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Subscription was not found.");


    return Response.ok(subs.getString())
    .header("Server", "api.servIoTicy")
    .header("Date", new Date(System.currentTimeMillis()))
    .build();
  }

  @Path("/security/info/{key}")
  @POST
  @Produces("application/json")
  public Response postSecurityInfo(@Context HttpHeaders hh,
                    @PathParam("key") String key, String body) {

    // Store in Couchbase
    CouchBase.setString(key, body);

    // Construct the response uri
    UriBuilder ub = uriInfo.getAbsolutePathBuilder();
    URI infoUri = ub.path(key).build();

    return Response.ok(infoUri)
             .entity("{ \"key\": \"" + key + "\"}")
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/security/info/{key}")
  @GET
  @Produces("application/json")
  public Response getSecurityInfo(@Context HttpHeaders hh, @PathParam("key") String key) {

    // Get the json
    String json = CouchBase.getString(key);
    if (json == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The info was not found.");

    return Response.ok(json)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/security/info/{key}")
  @DELETE
  @Produces("application/json")
  public Response deleteSecurityInfo(@Context HttpHeaders hh, @PathParam("key") String key) {

    // Delete the document
    CouchBase.deleteString(key);

    return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}/subscriptions")
  @GET
  @Produces("application/json")
  public Response getSubscriptions(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    String response = so.responseInternalSubscriptions(streamId, false);

    // Generate response
    if (response == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}/lastUpdate")
  @GET
  @Produces("application/json")
  public Response getLastUpdate(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // Get the Service Object Data
    long lastUpdate = SearchEngine.getLastUpdateTimeStamp(soId,streamId);
    Data data = CouchBase.getData(soId,streamId,lastUpdate);


    if (data == null) {
      System.out.println("Returned data is null");
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
    }
    System.out.println("Returned data is: "+data.responsePrivateLastUpdate());
    return Response.ok(data.getString())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/groups/lastUpdate")
  @POST
  @Produces("application/json")
  public Response getNewLastGroupUpdate(@Context HttpHeaders hh, String body) throws JsonProcessingException, IOException {

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Create Group petition
    Group group = new Group(body);

    String response = group.lastUpdate();

    if (response.equals("{}"))
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }
  /*public Response getLastGroupUpdate(@Context HttpHeaders hh, String body) throws JsonProcessingException, IOException {

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Create Group petition
    Group group = new Group(body);

    String response = group.lastUpdate();

    if (response.equals("{}"))
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(response)
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }*/

  @Path("/{soId}/streams/{streamId}")
  @PUT
  @Produces("application/json")
  public Response updateInternalSOData(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, String body) {

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Get the Service Object
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // Create Data
    Data data = new Data(so, streamId, body);

    // Store in Couchbase
    CouchBase.setData(data);

    return Response.ok(body)
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }


  private List<String> processQueryResult(HttpResponse response) throws IOException, FeedException {
    if(response.getEntity() == null){
      return null;
    }
    SyndFeedInput input = new SyndFeedInput();
    SyndFeed feed = input.build(new XmlReader(response.getEntity().getContent()));
    if(feed == null){
      return null;
    }
    List<String> soIds = new ArrayList<String>();
    for(Object entry: feed.getEntries()) {
      String title = ((SyndEntryImpl)entry).getTitle();
      soIds.add(title.split("/services/")[1].split("/")[1]);
    }
    return soIds;
  }

  public List<String> performQuery(String query)
          throws ExecutionException, InterruptedException, IOException, FeedException {
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
    List<Future<HttpResponse>> responses = new ArrayList<Future<HttpResponse>>();
    List<String> soIds = new ArrayList<String>();
    HttpRequestBase httpMethod = new HttpGet(query);

    return processQueryResult(httpClient.execute(httpMethod, null).get());
  }
  /** Create subscriptions to destination for all the query results
   *
   * @param destination
   */
  public void updateDynSubscriptions(String accessToken, String destination, String userId, List<SO> sos,
                                  String streamId, String groupId) {
    String body;

    if (groupId == null)
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, "Error retrieving group info");

    // TODO Remove current subscriptions

    for (SO so : sos) {

      body = "{ " + "\"callback\" : " + "\"internal\", \"destination\":  \"" + destination + "\", \"customFields\": { \"groupId\": \"" + groupId + "\" }" + " }";

      // Create Subscription
      Subscription subs = new Subscription(accessToken, userId, so, streamId, body);

      // Store in Couchbase
      CouchBase.setSubscription(subs);
    }
  }

  @Path("/{soId}/dyngroups/{groupId}/{accessToken}")
  @PUT
  @Produces("application/json")
  public Response updateSODynGroups(@Context HttpHeaders hh, @PathParam("soId") String soId,
                                    @PathParam("groupId") String groupId,
                                    @PathParam("accessToken") String accessToken, String body) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> soMap;
      Map<String, Object> groupsMap;
      Map<String, Object> dyngroupMap;
      // Check if exists request data
      //    if (body.isEmpty())
      //      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

      // Get the Service Object
      SO so = CouchBase.getSO(soId);

      if (so == null)
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

      soMap = mapper.readValue(so.getString(), new TypeReference<Map<String, Object>>() {});
      groupsMap = (Map<String,Object>)soMap.get("groups");
      if (groupsMap == null) {
        groupsMap = new HashMap<String, Object>();
      }

      if (!(soMap.containsKey("dyngroups") && ((Map<String, Object>)soMap.get("dyngroups")).containsKey(groupId))) {
        throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "dyngroup not found");
      }
      dyngroupMap = (Map<String, Object>) ((Map<String, Object>)soMap.get("dyngroups")).get(groupId);

      // Fill the groups data
      Map<String, Object> groupMap = new HashMap<String, Object>();
      List<String> soIds = performQuery((String) dyngroupMap.get("query"));
      List<SO> sos = new ArrayList<SO>();
      List<String> cleanSoIds = new ArrayList<String>();
      // Check that the SOs exist
      String streamId = (String) dyngroupMap.get("stream");

      int i = 0;
      for(String origSoId : soIds){
        SO origSO = CouchBase.getSO(origSoId);
        if(origSO!=null && origSO.getStream(streamId)!=null){
          sos.add(origSO);
          cleanSoIds.add(origSoId);
        }
      }
      soIds = cleanSoIds;
      groupMap.put("soIds", soIds);
      groupMap.put("stream", streamId);
      groupsMap.put(groupId, groupMap);

      // Update SO
      soMap.put("groups", groupsMap);
      so = new SO(mapper.writeValueAsString(soMap));
      CouchBase.setSO(so);

      // Create subscriptions
      updateDynSubscriptions(accessToken, soId, so.getUserId(), sos, streamId, groupId);

      return Response.ok(body)
              .header("Server", "api.compose")
              .header("Date", new Date(System.currentTimeMillis()))
              .build();
    }
    catch (Exception e) {
      throw new ServIoTWebApplicationException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

}
