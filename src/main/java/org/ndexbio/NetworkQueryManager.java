package org.ndexbio;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.cxio.aspects.datamodels.CyVisualPropertiesElement;
import org.cxio.aspects.datamodels.EdgeAttributesElement;
import org.cxio.aspects.datamodels.EdgesElement;
import org.cxio.aspects.datamodels.NetworkAttributesElement;
import org.cxio.aspects.datamodels.NodeAttributesElement;
import org.cxio.aspects.datamodels.NodesElement;
import org.cxio.core.AspectIterator;
import org.cxio.core.NdexCXNetworkWriter;
import org.cxio.metadata.MetaDataCollection;
import org.cxio.metadata.MetaDataElement;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.ndexbio.model.cx.CitationElement;
import org.ndexbio.model.cx.EdgeCitationLinksElement;
import org.ndexbio.model.cx.EdgeSupportLinksElement;
import org.ndexbio.model.cx.FunctionTermElement;
import org.ndexbio.model.cx.NamespacesElement;
import org.ndexbio.model.cx.NodeCitationLinksElement;
import org.ndexbio.model.cx.NodeSupportLinksElement;
import org.ndexbio.model.cx.SupportElement;
import org.ndexbio.model.exceptions.BadRequestException;

import com.fasterxml.jackson.core.JsonProcessingException;

public class NetworkQueryManager {

	static Logger accLogger = Log.getRootLogger();
//  	Log.getRootLogger().info("Embedded Jetty logging started.", new Object[]{});

	private int depth;
	private String netId;
	private static final Long consistencyGrp = Long.valueOf(1L);
	private static final String mdeVer = "1.0";
	
	private static String pathPrefix = "/opt/ndex/data/";
	private boolean usingOldVisualPropertyAspect;
	private int edgeLimit;
	private boolean errorOverLimit;
	
	public NetworkQueryManager (String networkId, int depth, int limit, boolean errorWhenOverLimit) {
	
		this.netId = networkId;
		this.depth = depth;
		this.edgeLimit = limit;
		this.errorOverLimit = errorWhenOverLimit;
	}
	
	public static void setDataFilePathPrefix(String path) {
		pathPrefix = path;
	}
	
	/**
	 * Neighbourhood query
	 * @param out
	 * @param nodeIds
	 * @throws IOException
	 * @throws BadRequestException 
	 */
	public void neighbourhoodQuery(OutputStream out, Set<Long> nodeIds) throws IOException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		Set<Long> edgeIds = new TreeSet<> ();
		usingOldVisualPropertyAspect = false;
		
		boolean limitIsOver = false;
		
		NdexCXNetworkWriter writer = new NdexCXNetworkWriter(out);
		MetaDataCollection md = prepareMetadata() ;
		writer.start();
		writer.writeMetadata(md);
		
		MetaDataCollection postmd = new MetaDataCollection();
		
		writeContextAspect(writer, md, postmd);
	
		if (md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			Set<Long> startingNodeIds = nodeIds;
			writer.startAspectFragment(EdgesElement.ASPECT_NAME);
			writer.openFragment();
			
			int cnt = 0;
			
			for (int i = 0; i < depth; i++) {
				if (limitIsOver) 
					break;
				Set<Long> newNodeIds = new TreeSet<> ();
				try (AspectIterator<EdgesElement> ei = new AspectIterator<>(UUID.fromString(netId),
						EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						EdgesElement edge = ei.next();
						if (!edgeIds.contains(Long.valueOf(edge.getId()))) {
							if (startingNodeIds.contains(Long.valueOf(edge.getSource()))
									|| startingNodeIds.contains(Long.valueOf(edge.getTarget()))) {
								cnt ++;
								if ( edgeLimit > 0 && cnt > edgeLimit) {
									if ( this.errorOverLimit) {
										writer.closeFragment();
										writer.endAspectFragment();
										writer.end(false, "EdgeLimitExceeded");
										return;
									}
									limitIsOver = true;
									break;
								}
								writer.writeElement(edge);
								edgeIds.add(Long.valueOf(edge.getId()));
								if (!nodeIds.contains(edge.getSource()))
									newNodeIds.add(edge.getSource());
								if ( !nodeIds.contains(edge.getTarget()))
									newNodeIds.add(Long.valueOf(edge.getTarget()));
							}

						}

					}
					nodeIds.addAll(newNodeIds);
					startingNodeIds=newNodeIds;
				}

			}
			System.out.println("Query returned " + writer.getFragmentLength() + " edges.");
			writer.closeFragment();
			writer.endAspectFragment();
			
			if  (edgeIds.size()>0) {
				MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME,mdeVer);
				mde.setElementCount((long)edgeIds.size());
				mde.setIdCounter(Collections.max(edgeIds));
				postmd.add(mde);
			}
		}
		
		System.out.println ( "done writing out edges.");
		//write nodes
		writer.startAspectFragment(NodesElement.ASPECT_NAME);
		writer.openFragment();
		try (AspectIterator<NodesElement> ei = new AspectIterator<>(UUID.fromString(netId),
					NodesElement.ASPECT_NAME, NodesElement.class, pathPrefix+netId + "/")) {
			while (ei.hasNext()) {
				NodesElement node = ei.next();
				if (nodeIds.contains(Long.valueOf(node.getId()))) {
						writer.writeElement(node);
				}
			}
		}
		writer.closeFragment();
		writer.endAspectFragment();
		if ( nodeIds.size()>0) {
			MetaDataElement mde1 = new MetaDataElement(NodesElement.ASPECT_NAME,mdeVer);
			mde1.setElementCount((long)nodeIds.size());
			mde1.setIdCounter(Collections.max(nodeIds));
			postmd.add(mde1);
		}
		
		writeOtherAspectsForSubnetwork(nodeIds, edgeIds, writer, md, postmd, limitIsOver,
				"Neighborhood query result on network");
		
		writer.writeMetadata(postmd);
		writer.end();
		long t2 = Calendar.getInstance().getTimeInMillis();

		System.out.println ("Done - " + (t2-t1)/1000f + " seconds.");
	}

	private void writeOtherAspectsForSubnetwork(Set<Long> nodeIds, Set<Long> edgeIds, NdexCXNetworkWriter writer,
			MetaDataCollection md, MetaDataCollection postmd, boolean limitIsOver, String networkNamePrefix) throws IOException, JsonProcessingException {
		//process node attribute aspect
		if (md.getMetaDataElement(NodeAttributesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NodeAttributesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NodeAttributesElement> ei = new AspectIterator<>(UUID.fromString(netId),
						NodeAttributesElement.ASPECT_NAME, NodeAttributesElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						NodeAttributesElement nodeAttr = ei.next();
						if (nodeIds.contains(nodeAttr.getPropertyOf())) {
								writer.writeElement(nodeAttr);
						}
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(NodeAttributesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}
		
		//process edge attribute aspect
		if (md.getMetaDataElement(EdgeAttributesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(EdgeAttributesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<EdgeAttributesElement> ei = new AspectIterator<>(UUID.fromString(netId),
						EdgeAttributesElement.ASPECT_NAME, EdgeAttributesElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						EdgeAttributesElement edgeAttr = ei.next();
						if (edgeIds.contains(edgeAttr.getPropertyOf())) {
								writer.writeElement(edgeAttr);
						}
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(EdgeAttributesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}
		
		//write networkAttributes
		writer.startAspectFragment(NetworkAttributesElement.ASPECT_NAME);
		writer.openFragment();

		if (limitIsOver) {
			writer.writeElement(new NetworkAttributesElement(null, "EdgeLimitExceeded", "true"));
		}
		if (md.getMetaDataElement(NetworkAttributesElement.ASPECT_NAME) != null) {
			try (AspectIterator<NetworkAttributesElement> ei = new AspectIterator<>(UUID.fromString(netId),
					NetworkAttributesElement.ASPECT_NAME, NetworkAttributesElement.class, pathPrefix + netId + "/")) {
				while (ei.hasNext()) {
					NetworkAttributesElement attr = ei.next();
					if (attr.getName().equals("name"))
						attr.setSingleStringValue(networkNamePrefix + " - " + attr.getValue());
					writer.writeElement(attr);
				}
			}
		}
		writer.closeFragment();
		writer.endAspectFragment();
		MetaDataElement mde2 = new MetaDataElement(NetworkAttributesElement.ASPECT_NAME, mdeVer);
		mde2.setElementCount((long) writer.getFragmentLength());
		postmd.add(mde2);

		//process cyVisualProperty aspect
		if (md.getMetaDataElement(CyVisualPropertiesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(CyVisualPropertiesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<CyVisualPropertiesElement> it = new AspectIterator<>(UUID.fromString(netId),
					this.usingOldVisualPropertyAspect ? "visualProperties":CyVisualPropertiesElement.ASPECT_NAME, 
							CyVisualPropertiesElement.class, pathPrefix+netId + "/")) {
				while (it.hasNext()) {
					CyVisualPropertiesElement elmt = it.next();
					if ( elmt.getProperties_of().equals("nodes")) {
						if ( nodeIds.contains(elmt.getApplies_to())) {
							writer.writeElement(elmt);
						}
					} else if (elmt.getProperties_of().equals("edges")) {
						if ( edgeIds.contains(elmt.getApplies_to())) {
							writer.writeElement(elmt);
						}
					} else {
						writer.writeElement(elmt);
					}
				}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(CyVisualPropertiesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	
		
		// process function terms
		
		if (md.getMetaDataElement(FunctionTermElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(FunctionTermElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<FunctionTermElement> ei = new AspectIterator<>(UUID.fromString(netId),
					FunctionTermElement.ASPECT_NAME, FunctionTermElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						FunctionTermElement ft = ei.next();
						if (nodeIds.contains(ft.getNodeID())) {
								writer.writeElement(ft);
						}
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(FunctionTermElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	
		
		
		Set<Long> citationIds = new TreeSet<> ();
		
		//process citation links aspects
		if (md.getMetaDataElement(NodeCitationLinksElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NodeCitationLinksElement.ASPECT_NAME);
			writer.openFragment();
			NodeCitationLinksElement worker = new NodeCitationLinksElement();
			try (AspectIterator<NodeCitationLinksElement> ei = new AspectIterator<>(UUID.fromString(netId),
					NodeCitationLinksElement.ASPECT_NAME, NodeCitationLinksElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						NodeCitationLinksElement ft = ei.next();
						worker.getSourceIds().clear();
						for ( Long nid : ft.getSourceIds()) {
							if ( nodeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setCitationIds(ft.getCitationIds());
							writer.writeElement(worker);
							citationIds.addAll(worker.getCitationIds());
						}	
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(NodeCitationLinksElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	
				
		
		if (md.getMetaDataElement(EdgeCitationLinksElement.ASPECT_NAME) != null) {
			EdgeCitationLinksElement worker = new EdgeCitationLinksElement();
			writer.startAspectFragment(EdgeCitationLinksElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<EdgeCitationLinksElement> ei = new AspectIterator<>(UUID.fromString(netId),
					EdgeCitationLinksElement.ASPECT_NAME, EdgeCitationLinksElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						EdgeCitationLinksElement ft = ei.next();
						for ( Long nid : ft.getSourceIds()) {
							if ( edgeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setCitationIds(ft.getCitationIds());
							writer.writeElement(worker);
							citationIds.addAll(worker.getCitationIds());
							worker.getSourceIds().clear();
						}	
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(EdgeCitationLinksElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	
				
			
		if( !citationIds.isEmpty()) {
			writer.startAspectFragment(CitationElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<CitationElement> ei = new AspectIterator<>(UUID.fromString(netId),
				CitationElement.ASPECT_NAME, CitationElement.class, pathPrefix+netId + "/")) {
				while (ei.hasNext()) {
					CitationElement ft = ei.next();
					if ( citationIds.contains(ft.getId()))
						writer.writeElement(ft);
				}	
			}
			
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(CitationElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	

		// support and related aspects
		Set<Long> supportIds = new TreeSet<> ();
		
		//process support links aspects
		if (md.getMetaDataElement(NodeSupportLinksElement.ASPECT_NAME) != null) {
			NodeSupportLinksElement worker = new NodeSupportLinksElement();
			writer.startAspectFragment(NodeSupportLinksElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NodeSupportLinksElement> ei = new AspectIterator<>(UUID.fromString(netId),
					NodeSupportLinksElement.ASPECT_NAME, NodeSupportLinksElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						NodeSupportLinksElement ft = ei.next();
						for ( Long nid : ft.getSourceIds()) {
							if ( nodeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setSupportIds(ft.getSupportIds());
							writer.writeElement(worker);
							supportIds.addAll(worker.getSupportIds());
							worker.getSourceIds().clear();
						}	
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(NodeSupportLinksElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	
				
		
		if (md.getMetaDataElement(EdgeSupportLinksElement.ASPECT_NAME) != null) {
			EdgeSupportLinksElement worker = new EdgeSupportLinksElement();
			writer.startAspectFragment(EdgeSupportLinksElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<EdgeSupportLinksElement> ei = new AspectIterator<>(UUID.fromString(netId),
					EdgeSupportLinksElement.ASPECT_NAME, EdgeSupportLinksElement.class, pathPrefix+netId + "/")) {
					while (ei.hasNext()) {
						EdgeSupportLinksElement ft = ei.next();
						for ( Long nid : ft.getSourceIds()) {
							if ( edgeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setSupportIds(ft.getSupportIds());
							writer.writeElement(worker);
							supportIds.addAll(worker.getSupportIds());
							worker.getSourceIds().clear();
						}	
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(EdgeSupportLinksElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
		}	

				
		if( !supportIds.isEmpty()) {
			writer.startAspectFragment(SupportElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<SupportElement> ei = new AspectIterator<>(UUID.fromString(netId),
					SupportElement.ASPECT_NAME, SupportElement.class, pathPrefix+netId + "/")) {
				while (ei.hasNext()) {
					SupportElement ft = ei.next();
					if ( supportIds.contains(ft.getId()))
						writer.writeElement(ft);
				}	
			}
			
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(SupportElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)writer.getFragmentLength());
			postmd.add(mde);
					
		}
	}
	
	
	private MetaDataCollection prepareMetadata() {
		MetaDataCollection md = new MetaDataCollection();
		File dir = new File(pathPrefix+netId+"/aspects");
		  File[] directoryListing = dir.listFiles();
		  for (File child : directoryListing) {
			  String aspName = child.getName();
			  MetaDataElement e;
			  if (aspName.equals("visualProperties")) {
				  this.usingOldVisualPropertyAspect = true;
				   e = new MetaDataElement (CyVisualPropertiesElement.ASPECT_NAME, mdeVer);
			  } else 
			       e = new MetaDataElement (aspName, mdeVer);
			  e.setConsistencyGroup(consistencyGrp);
			  md.add(e);			  
		  }
		  
		  return md;
	}
	
	
	public void oneStepInterConnectQuery(OutputStream out, Set<Long> nodeIds ) throws IOException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		Map<Long, EdgesElement> edgeTable = new TreeMap<> ();
		
		//NodeId -> unique neighbor node ids
		Map<Long,NodeDegreeHelper> nodeNeighborIdTable = new TreeMap<>();
		
/*		for ( Long nodeId:nodeIds) {
			NodeDegreeHelper h = new NodeDegreeHelper();
			h.setNodeId(nodeId);
			nodeNeighborIdTable.put(nodeId, h);
		} */
		
		usingOldVisualPropertyAspect = false;
		
		NdexCXNetworkWriter writer = new NdexCXNetworkWriter(out);
		MetaDataCollection md = prepareMetadata() ;
		writer.start();
		writer.writeMetadata(md);
		
		MetaDataCollection postmd = new MetaDataCollection();
		
		writeContextAspect(writer, md, postmd);
	
		if (md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			try (AspectIterator<EdgesElement> ei = new AspectIterator<>(UUID.fromString(netId),
					EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix + netId + "/")) {
				while (ei.hasNext()) {
					EdgesElement edge = ei.next();					
					if (nodeIds.contains(edge.getSource())) {
						edgeTable.put(Long.valueOf(edge.getId()), edge);
						if ( ! nodeIds.contains(edge.getTarget())) {
							NodeDegreeHelper h = nodeNeighborIdTable.get(edge.getTarget());
							if (h != null && h.isToBeDeleted()) {
								if (h.getNodeId().equals(edge.getSource())) {
									h.addEdge(edge.getId());
								} else {
									h.setToBeDeleted(false);
									h.removeAllEdges();
								}
							} else if ( h == null) {
								NodeDegreeHelper newHelper = new NodeDegreeHelper(edge.getSource(), edge.getId());
								nodeNeighborIdTable.put(edge.getTarget(), newHelper);
							}
						}
					} else if (nodeIds.contains(Long.valueOf(edge.getTarget()))) {
//						writer.writeElement(edge);
						edgeTable.put(Long.valueOf(edge.getId()), edge);
						if ( ! nodeIds.contains(edge.getSource())) {
							NodeDegreeHelper h = nodeNeighborIdTable.get(edge.getSource());
						
							if (h != null && h.isToBeDeleted() ) {
								if (h.getNodeId().equals(edge.getTarget())) {
									h.addEdge(edge.getId());
								} else {
									h.setToBeDeleted(false);
									h.removeAllEdges();
								}
							} else if ( h == null) {
								NodeDegreeHelper newHelper = new NodeDegreeHelper(edge.getTarget(), edge.getId());
								nodeNeighborIdTable.put(edge.getSource(), newHelper);
							}
						}
					}

				}
			}
		}
		
		System.out.println( edgeTable.size()  + " edges from neighborhood query.");
		//trim the nodes that only connect to one starting nodes.
		Set<Long> finalNodes = new TreeSet<>();
		for (Map.Entry<Long, NodeDegreeHelper> e : nodeNeighborIdTable.entrySet()) {
			NodeDegreeHelper h = e.getValue();
			if ( h.isToBeDeleted()) {
				for ( Long edgeId : h.getEdgeIds())
					edgeTable.remove( edgeId);				
			} else {
				finalNodes.add(e.getKey());
			}
		}
		
		System.out.println( edgeTable.size()  + " edges after trim.");
		
		boolean limitIsOver= false;
		if ( edgeLimit >0 && edgeTable.size() > edgeLimit) {
			if ( this.errorOverLimit) {
				writer.end(false, "EdgeLimitExceeded");
				return;
			}
			limitIsOver = true;
			//redo the edges and nodes
			finalNodes.clear();
			int i = 0;
			Map<Long,EdgesElement> newTable = new HashMap<>(edgeTable.size());
			for (Map.Entry<Long,EdgesElement> entry :edgeTable.entrySet()) { 
				if (i == edgeLimit )
					break;
				i++;	
				newTable.put(entry.getKey(), entry.getValue());
				finalNodes.add(entry.getValue().getSource());
				finalNodes.add(entry.getValue().getTarget());
			}
			edgeTable = newTable;
		}
	
		finalNodes.addAll(nodeIds);
		
		// write edge aspect 
		if ( edgeTable.size() > 0 ) {
			writer.startAspectFragment(EdgesElement.ASPECT_NAME);
			writer.openFragment();

			for (EdgesElement e : edgeTable.values()) {
					writer.writeElement(e);
			}
	
			writer.closeFragment();
			writer.endAspectFragment();
			System.out.println("Query returned " + writer.getFragmentLength() + " edges.");

			MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME, mdeVer);
			mde.setElementCount((long) edgeTable.size());
			mde.setIdCounter(Collections.max(edgeTable.keySet()));
			postmd.add(mde);
		}
		
		
		System.out.println ( "done writing out edges.");
		
		//write nodes
		writer.startAspectFragment(NodesElement.ASPECT_NAME);
		writer.openFragment();
		try (AspectIterator<NodesElement> ei = new AspectIterator<>(UUID.fromString(netId),
					NodesElement.ASPECT_NAME, NodesElement.class, pathPrefix+netId + "/")) {
			while (ei.hasNext()) {
				NodesElement node = ei.next();
				if (finalNodes.contains(Long.valueOf(node.getId()))) {
						writer.writeElement(node);
				}
			}
		}
		writer.closeFragment();
		writer.endAspectFragment();
		if ( nodeIds.size()>0) {
			MetaDataElement mde1 = new MetaDataElement(NodesElement.ASPECT_NAME,mdeVer);
			mde1.setElementCount((long)finalNodes.size());
			mde1.setIdCounter(Collections.max(nodeIds));
			postmd.add(mde1);
		}
		
		writeOtherAspectsForSubnetwork(finalNodes, edgeTable.keySet(), writer, md, postmd, limitIsOver,
				"Interconnect query result on network");
		
		writer.writeMetadata(postmd);
		writer.end();
		long t2 = Calendar.getInstance().getTimeInMillis();

		System.out.println ("Done - " + (t2-t1)/1000f + " seconds.");
	//	accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeTable.size() + " edges and " + finalNodes.size() + " nodes.",
	//			new Object[]{});
	}

	private void writeContextAspect(NdexCXNetworkWriter writer, MetaDataCollection md, MetaDataCollection postmd)
			throws IOException, JsonProcessingException {
		//process namespace aspect	
		if (md.getMetaDataElement(NamespacesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NamespacesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NamespacesElement> ei = new AspectIterator<>(UUID.fromString(netId),
						NamespacesElement.ASPECT_NAME, NamespacesElement.class, pathPrefix+netId + "/")) {
				while (ei.hasNext()) {
						NamespacesElement node = ei.next();
							writer.writeElement(node);
				}
			}

			writer.closeFragment();
			writer.endAspectFragment();	
			MetaDataElement mde = new MetaDataElement(NamespacesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(1L);
			postmd.add(mde);
		}
	}
	
	private class NodeDegreeHelper {
		
		private boolean tobeDeleted;
		private Long nodeId;
		private List<Long> edgeIds;
		

		public NodeDegreeHelper (Long newNodeId, Long newEdgeId) {
			this.setToBeDeleted(true);
			edgeIds = new ArrayList<>();
			this.nodeId = newNodeId;
			edgeIds.add(newEdgeId);
		}

		public boolean isToBeDeleted() {
			return tobeDeleted;
		}

		public void setToBeDeleted(boolean toBeDeleted) {
			this.tobeDeleted = toBeDeleted;
		}

		public Long getNodeId() {
			return nodeId;
		}


		public List<Long> getEdgeIds() {
			return edgeIds;
		}
		
		public void addEdge(Long id) { this.edgeIds.add(id); }
		
		public void removeAllEdges() {edgeIds = null;}

	}

}
