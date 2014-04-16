package com.servioticy.api.internal;

import java.io.IOException;
import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.data.Group;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;


@Path("/")
public class Paths {

  @Path("/{soId}")
  @GET
  @Produces("application/json")
  public Response getSO(@Context HttpHeaders hh, @PathParam("soId") String soId) {

    // Get the Service Object
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    return Response.ok(so.responseGetSO())
           .header("Server", "api.compose")
           .header("Date", new Date(System.currentTimeMillis()))
           .build();
  }

  @Path("/{soId}/streams/{streamId}/subscriptions")
  @GET
  @Produces("application/json")
  public Response getSubscriptions(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId) {

    // Get the Service Object
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    String response = so.responseSubscriptions(streamId);

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
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // Get the Service Object Data
    long lastUpdate = SearchEngine.getLastUpdate(soId,streamId);    
    Data data = cb.getData(soId,streamId,lastUpdate);

    if (data == null)
      return Response.noContent()
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();

    return Response.ok(data.responseLastUpdate())
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/groups/lastUpdate")
  @POST
  @Produces("application/json")
  public Response getLastGroupUpdate(@Context HttpHeaders hh, String body) throws JsonProcessingException, IOException {

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

  @Path("/opid/{opId}")
  @GET
  @Produces("application/json")
  public Response getOpId(@Context HttpHeaders hh, @PathParam("opId") String opId,
                    @PathParam("streamId") String streamId) {

    // Get the Service Object
    CouchBase cb = new CouchBase();

    String res = cb.getOpId(opId);
    if (res == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The OpId was not found.");

    return Response.ok(res)
             .header("Server", "api.servIoTicy")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

  @Path("/{soId}/streams/{streamId}/{opId}")
  @PUT
  @Produces("application/json")
  public Response updateInternalSOData(@Context HttpHeaders hh, @PathParam("soId") String soId,
                    @PathParam("streamId") String streamId, @PathParam("opId") String opId, String body) {

    // Check if exists request data
    if (body.isEmpty())
      throw new ServIoTWebApplicationException(Response.Status.BAD_REQUEST, "No data in the request");

    // Get the Service Object
    CouchBase cb = new CouchBase();
    SO so = cb.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // Create Data
    Data data = new Data(so, streamId, body);

    // Store in Couchbase
    cb.setData(data);

    // Set the opId
    cb.setOpId(opId, Config.getOpIdExpiration());

    return Response.ok(body)
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

}
