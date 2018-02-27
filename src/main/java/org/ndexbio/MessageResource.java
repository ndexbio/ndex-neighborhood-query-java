package org.ndexbio;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.security.PermitAll;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.ndexbio.model.exceptions.BadRequestException;
import org.ndexbio.model.exceptions.NdexException;
import org.ndexbio.model.object.CXSimplePathQuery;
import org.ndexbio.model.object.SimplePathQuery;
import org.ndexbio.model.object.SimpleQuery;

import javax.ws.rs.core.MediaType;

@Path("/v1")
public class MessageResource {

  @SuppressWarnings("static-method")
  @GET
  @Path("/status")
  @Produces("application/json")
  public Map<String,String> printMessage() {
     Map<String,String> result = new HashMap<>();
     result.put("status", "online");
     return result;
  }
  
  
	@POST
	@Path("/network/{networkId}/interconnectquery")
	@Produces("application/json")

	public Response  interConnectQuery(
			@PathParam("networkId") final String networkIdStr,
			final SimplePathQuery queryParameters
			) throws SolrServerException, IOException, NdexException {
		
		Set<Long> nodeIds = new TreeSet<>();

		try (SingleNetworkSolrIdxManager idxr = new SingleNetworkSolrIdxManager(networkIdStr)) {
			SolrDocumentList r = idxr.getNodeIdsByQuery(queryParameters.getSearchString(), -1);
			for (SolrDocument d : r) {
				Object f = d.getFieldValue("id");
				nodeIds.add(Long.valueOf((String) f));
			}
		}
		System.out.println("Solr returned " + nodeIds.size() + " ids.");
		PipedInputStream in = new PipedInputStream();
		 
		PipedOutputStream out;
		
 		try {
			out = new PipedOutputStream(in);
		} catch (IOException e) {
			throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
		}
		
		new InterConnectQueryWriterThread(out,networkIdStr,queryParameters,nodeIds).start();
		//setZipFlag();
		return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
		

	}
	
		private class InterConnectQueryWriterThread extends Thread {
		private OutputStream o;
		private String networkId;
		private SimplePathQuery parameters;
		private Set<Long> startingNodeIds;
		
		public InterConnectQueryWriterThread (OutputStream out, String  networkUUIDStr, SimplePathQuery query,Set<Long> nodeIds ) {
			o = out;
			networkId = networkUUIDStr;
			this.parameters = query;
			startingNodeIds = nodeIds;
		}
		
		@Override
		public void run() {
			NetworkQueryManager b = new NetworkQueryManager(networkId, parameters.getSearchDepth(), parameters.getEdgeLimit(), parameters.isErrorWhenLimitIsOver());
			try {
				b.oneStepInterConnectQuery(o, startingNodeIds);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		//		o.write("error:" + e.getMessage());
			} finally {
				try {
					o.flush();
					o.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
		@POST
		@Path("/network/{networkId}/query")
		@Produces("application/json")

		public Response  queryNetworkNodes(
				@PathParam("networkId") final String networkIdStr,
				final SimplePathQuery queryParameters
				) throws SolrServerException, IOException, NdexException {
			
			Set<Long> nodeIds = new TreeSet<>();

			try (SingleNetworkSolrIdxManager idxr = new SingleNetworkSolrIdxManager(networkIdStr)) {
				SolrDocumentList r = idxr.getNodeIdsByQuery(queryParameters.getSearchString(), -1);
				for (SolrDocument d : r) {
					Object f = d.getFieldValue("id");
					nodeIds.add(Long.valueOf((String) f));
				}
			}
			System.out.println("Solr returned " + nodeIds.size() + " ids.");
			PipedInputStream in = new PipedInputStream();
			 
			PipedOutputStream out;
			
	 		try {
				out = new PipedOutputStream(in);
			} catch (IOException e) {
				throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
			}
			
			new CXNetworkQueryWriterThread(out,networkIdStr,queryParameters,nodeIds).start();
			//setZipFlag();
			return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
			

		}
		
			private class CXNetworkQueryWriterThread extends Thread {
			private OutputStream o;
			private String networkId;
			private SimplePathQuery parameters;
			private Set<Long> startingNodeIds;
			
			public CXNetworkQueryWriterThread (OutputStream out, String  networkUUIDStr, SimplePathQuery query,Set<Long> nodeIds ) {
				o = out;
				networkId = networkUUIDStr;
				this.parameters = query;
				startingNodeIds = nodeIds;
			}
			
			@Override
			public void run() {
				NetworkQueryManager b = new NetworkQueryManager(networkId, parameters.getSearchDepth(), parameters.getEdgeLimit(), parameters.isErrorWhenLimitIsOver());
				try {
					b.neighbourhoodQuery(o, startingNodeIds);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			//		o.write("error:" + e.getMessage());
				} finally {
					try {
						o.flush();
						o.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
		
	
  
}	