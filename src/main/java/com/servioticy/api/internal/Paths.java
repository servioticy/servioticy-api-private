package com.servioticy.api.internal;

import java.io.IOException;
import java.net.URI;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.data.Group;
import com.servioticy.api.commons.data.SO;
import com.servioticy.api.commons.data.Subscription;
import com.servioticy.api.commons.datamodel.Data;
import com.servioticy.api.commons.elasticsearch.SearchEngine;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;
import com.servioticy.api.commons.utils.Config;


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
		  			@PathParam("subsId") String subsId, String body) {

    // Get the Service Object
    Subscription subs = CouchBase.getSubscription(subsId);
    if (subs == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Subscription was not found.");


    return Response.ok(subs.getString())
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
    String res = CouchBase.getOpId(opId);
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
    SO so = CouchBase.getSO(soId);
    if (so == null)
      throw new ServIoTWebApplicationException(Response.Status.NOT_FOUND, "The Service Object was not found.");

    // Create Data
    Data data = new Data(so, streamId, body);

    // Store in Couchbase
    CouchBase.setData(data);

    // Set the opId
    CouchBase.setOpId(opId, Config.getOpIdExpiration());

    return Response.ok(body)
             .header("Server", "api.compose")
             .header("Date", new Date(System.currentTimeMillis()))
             .build();
  }

}
