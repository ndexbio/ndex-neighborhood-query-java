package org.ndexbio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.eclipse.jetty.util.log.Log;
import org.ndexbio.model.exceptions.NdexException;
import org.ndexbio.model.object.SimplePathQuery;

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
	@Path("/mytest")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)

    public String mytest (@Context HttpServletRequest request/*, InputStream in*/) throws IOException {
	   String contentType = request.getContentType();
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
			final SimplePathQuery queryParameters
			) throws SolrServerException, IOException, NdexException {
		
		Log.getRootLogger().info("Interconnect Query term: " + queryParameters.getSearchString());
		Set<Long> nodeIds = new TreeSet<>();

		UUID networkId = UUID.fromString(networkIdStr);
		try (SingleNetworkSolrIdxManager idxr = new SingleNetworkSolrIdxManager(networkIdStr)) {
			SolrDocumentList r = idxr.getNodeIdsByQuery(queryParameters.getSearchString(), 1000000);
			for (SolrDocument d : r) {
				Object f = d.getFieldValue("id");
				nodeIds.add(Long.valueOf((String) f));
			}
		}
		
		Log.getRootLogger().info("Solr returned " + nodeIds.size() + " ids.");
		PipedInputStream in = new PipedInputStream();
		 
		PipedOutputStream out;
		
 		try {
			out = new PipedOutputStream(in);
		} catch (IOException e) {
			in.close();
			throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
		}
		
		new InterConnectQueryWriterThread(out,networkId,queryParameters,nodeIds).start();
		//setZipFlag();
		return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
		

	}
	
	private class InterConnectQueryWriterThread extends Thread {
		private OutputStream o;
		private UUID networkId;
		private SimplePathQuery parameters;
		private Set<Long> startingNodeIds;
		
		public InterConnectQueryWriterThread (OutputStream out, UUID  networkUUID, SimplePathQuery query,Set<Long> nodeIds ) {
			o = out;
			networkId = networkUUID;
			this.parameters = query;
			startingNodeIds = nodeIds;
		}
		
		@Override
		public void run() {
			NetworkQueryManager b = new NetworkQueryManager(networkId, parameters);
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
		@Consumes(MediaType.APPLICATION_JSON)
		public Response  queryNetwork(
				@PathParam("networkId") final String networkIdStr,
				final SimplePathQuery queryParameters
				) throws SolrServerException, IOException, NdexException {
			
			Log.getRootLogger().info("Neighorhood Query term: " + queryParameters.getSearchString());
			UUID networkId = UUID.fromString(networkIdStr);

			Set<Long> nodeIds = new TreeSet<>();

			try (SingleNetworkSolrIdxManager idxr = new SingleNetworkSolrIdxManager(networkIdStr)) {
				SolrDocumentList r = idxr.getNodeIdsByQuery(queryParameters.getSearchString(), 1000000);
				for (SolrDocument d : r) {
					Object f = d.getFieldValue("id");
					nodeIds.add(Long.valueOf((String) f));
				}
			}
			Log.getRootLogger().info("Solr returned " + nodeIds.size() + " ids.");
			PipedInputStream in = new PipedInputStream();
			 
			PipedOutputStream out;
			
	 		try {
				out = new PipedOutputStream(in);
			} catch (IOException e) {
				in.close();
				throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
			}
			
			new CXNetworkQueryWriterThread(out,networkId,queryParameters,nodeIds).start();
			//setZipFlag();
			return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
			

		}
		
		private class CXNetworkQueryWriterThread extends Thread {
			private OutputStream o;
			private UUID networkId;
			private SimplePathQuery parameters;
			private Set<Long> startingNodeIds;
			
			public CXNetworkQueryWriterThread (OutputStream out, UUID  networkUUID, SimplePathQuery query,Set<Long> nodeIds ) {
				o = out;
				networkId = networkUUID;
				this.parameters = query;
				startingNodeIds = nodeIds;
			}
			
			@Override
			public void run() {
				NetworkQueryManager b = new NetworkQueryManager(networkId, parameters);
				try {
					b.neighbourhoodQuery(o, startingNodeIds);
				} catch (IOException e) {
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