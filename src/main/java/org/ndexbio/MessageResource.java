package org.ndexbio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.ndexbio.cxio.util.CxConstants;
import org.ndexbio.model.exceptions.NdexException;
import org.ndexbio.model.object.SimplePathQuery;
import org.ndexbio.model.tools.SearchUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1")
public class MessageResource {
	
	public static Logger logger = LoggerFactory.getLogger("MessageResource");

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
	@Path("/mytest")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)

    public String mytest (@Context HttpServletRequest request/*, InputStream in*/) throws IOException {
	  // String contentType = request.getContentType();
	   ServletInputStream in = request.getInputStream();
	   
	   BufferedReader br = new BufferedReader(new InputStreamReader(in));
	   String readLine;

	   while (((readLine = br.readLine()) != null)) {
	   System.out.println(readLine);
	   }
	   return "abc";
	}
  
	@POST
	@Path("/network/{networkId}/interconnectquery")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response  interConnectQuery(
			@PathParam("networkId") final String networkIdStr,
			@DefaultValue("false") @QueryParam("outputCX2") boolean outputCX2,
			@DefaultValue("false") @QueryParam("preserveCoordinates") boolean preserveNodeCoordinates,
			final SimplePathQuery queryParameters
			) throws SolrServerException, IOException, NdexException {
		
		logger.info("Interconnect Query term: " + queryParameters.getSearchString());
		logger.info("Interconnect Query edgeLimit: " + queryParameters.getEdgeLimit());

		UUID networkId = UUID.fromString(networkIdStr);
		Set<Long> nodeIds = queryParameters.getNodeIds() == null ?  
				findStartingNodeIds(networkIdStr, queryParameters.getSearchString()) : 
					new HashSet<>(queryParameters.getNodeIds());
		
		if ( nodeIds.isEmpty()) {
			return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(CxConstants.EMPTY_NETWORK).build();
		}
		
		PipedInputStream in = new PipedInputStream();
		 
		PipedOutputStream out;
		
 		try {
			out = new PipedOutputStream(in);
		} catch (IOException e) {
			in.close();
			throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
		}
		
		new InterConnectQueryWriterThread(out,networkId,queryParameters,nodeIds, outputCX2,preserveNodeCoordinates).start();
		//setZipFlag();
		return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
		

	}
	
	private static Set<Long>  findStartingNodeIds (String networkIdStr, String searchString) throws SolrServerException, IOException, NdexException {
		Set<Long> nodeIds = new TreeSet<>();

		try (SingleNetworkSolrIdxManager idxr = new SingleNetworkSolrIdxManager(networkIdStr)) {
			SolrDocumentList r = idxr.getNodeIdsByQuery(SearchUtilities.preprocessSearchTerm(searchString), 1000000);
			for (SolrDocument d : r) {
				Object f = d.getFieldValue("id");
				nodeIds.add(Long.valueOf((String) f));
			}
		}
		
		logger.info("Solr returned " + nodeIds.size() + " ids.");
		
		return nodeIds;
	}
	
	private class InterConnectQueryWriterThread extends Thread {
		private OutputStream o;
		private UUID networkId;
		private SimplePathQuery parameters;
		private Set<Long> startingNodeIds;
		private boolean outputCX2;
		private boolean preserveCoordinates;
		
		public InterConnectQueryWriterThread (OutputStream out, UUID  networkUUID, SimplePathQuery query,Set<Long> nodeIds, boolean outputCX2,
				boolean preserveNodeCoordinates ) {
			o = out;
			networkId = networkUUID;
			this.parameters = query;
			startingNodeIds = nodeIds;
			this.outputCX2 = outputCX2;
			this.preserveCoordinates = preserveNodeCoordinates;
		}
		
		@Override
		public void run() {
			NetworkQueryManager b = new NetworkQueryManager(networkId, parameters);
			try {
				if ( outputCX2)
					b.interConnectQueryCX2(o, startingNodeIds, preserveCoordinates);
				else 
				    b.interConnectQuery(o, startingNodeIds);
			} catch (IOException | NdexException e) {
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
		@Consumes(MediaType.APPLICATION_JSON)
		public Response  queryNetwork(
				@PathParam("networkId") final String networkIdStr,
				@DefaultValue("false") @QueryParam("outputCX2") boolean outputCX2,
				@DefaultValue("false") @QueryParam("perserveCoordinates") boolean perserveCoordinates,
				final SimplePathQuery queryParameters
				) throws SolrServerException, IOException, NdexException {
			
			logger.info("Neighorhood Query term: " + queryParameters.getSearchString());
			logger.info("Neighorhood Query edgeLimit: " + queryParameters.getEdgeLimit());
			UUID networkId = UUID.fromString(networkIdStr);

			Set<Long> nodeIds = queryParameters.getNodeIds() == null ?  
					findStartingNodeIds(networkIdStr, queryParameters.getSearchString()) : 
						new HashSet<>(queryParameters.getNodeIds());
			
			if ( nodeIds.isEmpty()) {
				return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(CxConstants.EMPTY_NETWORK).build();
			}
			
			PipedInputStream in = new PipedInputStream();
			 
			PipedOutputStream out;
			
	 		try {
				out = new PipedOutputStream(in);
			} catch (IOException e) {
				in.close();
				throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
			}
			
			new CXNetworkQueryWriterThread(out,networkId,queryParameters,nodeIds, outputCX2, perserveCoordinates).start();
			//setZipFlag();
			return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
			

		}
		
		private class CXNetworkQueryWriterThread extends Thread {
			private OutputStream o;
			private UUID networkId;
			private SimplePathQuery parameters;
			private Set<Long> startingNodeIds;
			private boolean outputCX2;
			private boolean preserveNodeCoordinates;
			
			public CXNetworkQueryWriterThread (OutputStream out, UUID  networkUUID, SimplePathQuery query,Set<Long> nodeIds, 
					boolean outputCX2,boolean preserveCoordinates) {
				o = out;
				networkId = networkUUID;
				this.parameters = query;
				startingNodeIds = nodeIds;
				this.outputCX2 = outputCX2;
				this.preserveNodeCoordinates = preserveCoordinates;
			}
			
			@Override
			public void run() {
				NetworkQueryManager b = new NetworkQueryManager(networkId, parameters);
				try {
					if ( outputCX2) 
						b.neighbourhoodQueryCX2(o, startingNodeIds,true, preserveNodeCoordinates);
					else
						b.neighbourhoodQuery(o, startingNodeIds);
				} catch (IOException | NdexException e) {
					e.printStackTrace();
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