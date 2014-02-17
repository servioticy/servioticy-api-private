package com.servioticy.api.internal;

import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import com.servioticy.api.commons.data.CouchBase;
import com.servioticy.api.commons.exceptions.ServIoTWebApplicationException;


@Path("/private")
public class Paths {

	@Path("/opid/{opId}")
	@GET
	@Produces("application/json")
	public Response getSubscriptions(@Context HttpHeaders hh, @PathParam("opId") String opId,
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

	

}
