package org.ndexbio;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.ndexbio.cx2.aspect.element.core.CxAttributeDeclaration;
import org.ndexbio.cx2.aspect.element.core.CxEdge;
import org.ndexbio.cx2.aspect.element.core.CxEdgeBypass;
import org.ndexbio.cx2.aspect.element.core.CxMetadata;
import org.ndexbio.cx2.aspect.element.core.CxNetworkAttribute;
import org.ndexbio.cx2.aspect.element.core.CxNode;
import org.ndexbio.cx2.aspect.element.core.CxNodeBypass;
import org.ndexbio.cx2.aspect.element.core.CxVisualProperty;
import org.ndexbio.cx2.aspect.element.core.DeclarationEntry;
import org.ndexbio.cx2.aspect.element.cytoscape.VisualEditorProperties;
import org.ndexbio.cx2.io.CXWriter;
import org.ndexbio.cxio.aspects.datamodels.ATTRIBUTE_DATA_TYPE;
import org.ndexbio.cxio.aspects.datamodels.CyVisualPropertiesElement;
import org.ndexbio.cxio.aspects.datamodels.EdgeAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.EdgesElement;
import org.ndexbio.cxio.aspects.datamodels.NetworkAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.NodeAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.NodesElement;
import org.ndexbio.cxio.core.AspectIterator;
import org.ndexbio.cxio.core.NdexCXNetworkWriter;
import org.ndexbio.cxio.metadata.MetaDataCollection;
import org.ndexbio.cxio.metadata.MetaDataElement;
import org.ndexbio.model.cx.CitationElement;
import org.ndexbio.model.cx.EdgeCitationLinksElement;
import org.ndexbio.model.cx.EdgeSupportLinksElement;
import org.ndexbio.model.cx.FunctionTermElement;
import org.ndexbio.model.cx.NamespacesElement;
import org.ndexbio.model.cx.NodeCitationLinksElement;
import org.ndexbio.model.cx.NodeSupportLinksElement;
import org.ndexbio.model.cx.SupportElement;
import org.ndexbio.model.exceptions.BadRequestException;
import org.ndexbio.model.exceptions.NdexException;
import org.ndexbio.model.network.query.FilterCriterion;
import org.ndexbio.model.object.SimplePathQuery;
import org.ndexbio.model.tools.EdgeFilter;
import org.ndexbio.model.tools.NodeDegreeHelper;

import com.fasterxml.jackson.core.JsonProcessingException;

public class NetworkQueryManager {

	static Logger accLogger = Log.getRootLogger();
//  	Log.getRootLogger().info("Embedded Jetty logging started.", new Object[]{});

	private int depth;
	private String netId;
	private static final Long consistencyGrp = Long.valueOf(1L);
	private static final String mdeVer = "1.0";
	private static final String provDerivedFrom = "prov:wasDerivedFrom";
	private static final String provGeneratedBy = "prov:wasGeneratedBy";
	private static final String queryNode = "querynode";
	private static final String edgeLimitExceeded = "EdgeLimitExceeded";
	
	private static final String parentNetworkModificationTime = "parentNetworkModificationTime";
	
	private static String pathPrefix = "/opt/ndex/data/";
	private boolean usingOldVisualPropertyAspect;
	private int edgeLimit;
	private boolean errorOverLimit;
	private boolean directOnly;
	private String searchTerms;
	
	public NetworkQueryManager (UUID networkId, SimplePathQuery query) {
	
		this.netId = networkId.toString();
		this.depth = query.getSearchDepth();
		this.edgeLimit = query.getEdgeLimit();
		this.errorOverLimit = query.getErrorWhenLimitIsOver();
		this.directOnly = query.getDirectOnly();
		this.searchTerms = query.getSearchString();
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
		Set<Long> queryNodeIds = new HashSet<>(nodeIds);
		
		NdexCXNetworkWriter writer = new NdexCXNetworkWriter(out, true);
		MetaDataCollection md = prepareMetadata() ;
		writer.start();
		writer.writeMetadata(md);
		
		MetaDataCollection postmd = new MetaDataCollection();
		
		writeContextAspect(writer, md, postmd);

		int cnt = 0;
	
		if (md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			Set<Long> startingNodeIds = nodeIds;
			writer.startAspectFragment(EdgesElement.ASPECT_NAME);
			writer.openFragment();
			
			for (int i = 0; i < depth; i++) {
				if (limitIsOver) 
					break;
				Set<Long> newNodeIds = new TreeSet<> ();
				try (AspectIterator<EdgesElement> ei = new AspectIterator<>( netId,EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix)) {
					while (ei.hasNext()) {
						EdgesElement edge = ei.next();
						if (!edgeIds.contains(edge.getId())) {
							if (startingNodeIds.contains(edge.getSource())
									|| startingNodeIds.contains(edge.getTarget())) {
								cnt ++;
								if ( edgeLimit > 0 && cnt > edgeLimit) {
									if ( this.errorOverLimit) {
										writer.closeFragment();
										writer.endAspectFragment();
										writer.end(false, edgeLimitExceeded);
										return;
									}
									limitIsOver = true;
									break;
								}
								writer.writeElement(edge);
								edgeIds.add(edge.getId());
								if (!nodeIds.contains(edge.getSource()))
									newNodeIds.add(edge.getSource());
								if ( !nodeIds.contains(edge.getTarget()))
									newNodeIds.add(edge.getTarget());
							}

						}

					}
					nodeIds.addAll(newNodeIds);
					startingNodeIds=newNodeIds;
				}

			}
			accLogger.info("Query returned " + writer.getFragmentLength() + " edges.");
			writer.closeFragment();
			writer.endAspectFragment();
			
		}
		
		accLogger.info ( "done writing out edges.");
		//write nodes
		writer.startAspectFragment(NodesElement.ASPECT_NAME);
		writer.openFragment();
		try (AspectIterator<NodesElement> ei = new AspectIterator<>(netId, NodesElement.ASPECT_NAME, NodesElement.class, pathPrefix)) {
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
			mde1.setIdCounter(nodeIds.isEmpty()? 0L : Collections.max(nodeIds));
			postmd.add(mde1);
		}
		
		//check if we need to output the full neighborhood.
		if ( !directOnly && md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(EdgesElement.ASPECT_NAME);
			writer.openFragment();

			try (AspectIterator<EdgesElement> ei = new AspectIterator<>( netId,EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					EdgesElement edge = ei.next();
					if ( (!edgeIds.contains(edge.getId())) && nodeIds.contains(edge.getSource())
								&& nodeIds.contains(edge.getTarget())) {
							cnt ++;
							if ( edgeLimit > 0 && cnt > edgeLimit) {
								if ( this.errorOverLimit) {
									writer.closeFragment();
									writer.endAspectFragment();
									writer.end(false, edgeLimitExceeded);
									return;
								}
								limitIsOver = true;
								break;
							}
							writer.writeElement(edge);
							edgeIds.add(edge.getId());
					}
				}
			}
			writer.closeFragment();
			writer.endAspectFragment();
		}
		
		if  (edgeIds.size()>0) {
			MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)edgeIds.size());
			mde.setIdCounter(edgeIds.isEmpty()? 0L : Collections.max(edgeIds));
			postmd.add(mde);
		}

		String queryName = directOnly ? "Adjacent" : "Neighborhood" ;
		ArrayList<NetworkAttributesElement> provenanceRecords = new ArrayList<> (2);
		provenanceRecords.add(new NetworkAttributesElement (null, provDerivedFrom, netId));
		provenanceRecords.add(new NetworkAttributesElement (null, provGeneratedBy,
				"NDEx "+ queryName + " Query/v1.1 (Depth=" + this.depth +"; Query terms=\""+ this.searchTerms + "\")"));
		
		writeOtherAspectsForSubnetwork(nodeIds, edgeIds, writer, md, postmd, limitIsOver,
				queryName + "  query result on network" , provenanceRecords, queryNodeIds);
		
		writer.writeMetadata(postmd);
		writer.end();
		long t2 = Calendar.getInstance().getTimeInMillis();

		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeIds.size() + " edges and " + nodeIds.size() + " nodes.",
				new Object[]{});
	}

	private void writeOtherAspectsForSubnetwork(Set<Long> nodeIds, Set<Long> edgeIds, NdexCXNetworkWriter writer,
			MetaDataCollection md, MetaDataCollection postmd, boolean limitIsOver, String networkNamePrefix,
			Collection<NetworkAttributesElement> extraNetworkAttributes, Set<Long> queryNodeIds) throws IOException, JsonProcessingException {
		
		boolean hasOldQueryNode = false;  // a flag to tell if there were any old queryNode=true attributes in the result nodes.
		//process node attribute aspect
		if (  md.getMetaDataElement(NodeAttributesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NodeAttributesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NodeAttributesElement> ei = new AspectIterator<>(netId, NodeAttributesElement.ASPECT_NAME, NodeAttributesElement.class, pathPrefix)) {
					while (ei.hasNext()) {
						NodeAttributesElement nodeAttr = ei.next();
						if (nodeIds.contains(nodeAttr.getPropertyOf())) {
							//If the parent network has querynode=true in the result, we rename it to _querynode=true 
							if ( nodeAttr.getName().equals(queryNode) && nodeAttr.getDataType() == ATTRIBUTE_DATA_TYPE.BOOLEAN
									&& nodeAttr.getValue().equals("true") ) {
								hasOldQueryNode = true;
								writer.writeElement(new NodeAttributesElement(null, nodeAttr.getPropertyOf(), "_" +queryNode, "true", ATTRIBUTE_DATA_TYPE.BOOLEAN));
							} else
								writer.writeElement(nodeAttr);
						}
					}
			}
			
			// add the queryNode attribute on starting nodes
			for (Long nodeId: queryNodeIds) {
				writer.writeElement( new NodeAttributesElement(null, nodeId,queryNode, "true", ATTRIBUTE_DATA_TYPE.BOOLEAN));
			}
			
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(NodeAttributesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}
		
		// if the result has old query nodes, we add a comment in the result network.
		if ( hasOldQueryNode )
			extraNetworkAttributes.add(new NetworkAttributesElement(null, "NDEx_Query_Comments", 
				"All node attributes 'querynode' in your parenent network have been renamed to _querynode in the query result."));
		//process edge attribute aspect
		if (md.getMetaDataElement(EdgeAttributesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(EdgeAttributesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<EdgeAttributesElement> ei = new AspectIterator<>(netId,EdgeAttributesElement.ASPECT_NAME, EdgeAttributesElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}
		
		//write networkAttributes
		writer.startAspectFragment(NetworkAttributesElement.ASPECT_NAME);
		writer.openFragment();

		if (limitIsOver) {
			writer.writeElement(new NetworkAttributesElement(null, edgeLimitExceeded, "true"));
		}
		if (md.getMetaDataElement(NetworkAttributesElement.ASPECT_NAME) != null) {
			try (AspectIterator<NetworkAttributesElement> ei = new AspectIterator<>(netId,NetworkAttributesElement.ASPECT_NAME, NetworkAttributesElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					NetworkAttributesElement attr = ei.next();
					String attrName = attr.getName();
					//Strip out the old provenance Info from the network because we are going to inject new ones in.
					if ( (! attrName.equals(provDerivedFrom)) && (! attrName.equals(provGeneratedBy))) {
						if (attr.getName().equals("name"))
							attr.setSingleStringValue(networkNamePrefix + " - " + attr.getValue());
						writer.writeElement(attr);
					}
				}
			}
		}
		
		for ( NetworkAttributesElement attr : extraNetworkAttributes) {
			writer.writeElement(attr);			
		}
		
		writer.closeFragment();
		writer.endAspectFragment();
		MetaDataElement mde2 = new MetaDataElement(NetworkAttributesElement.ASPECT_NAME, mdeVer);
		mde2.setElementCount( writer.getFragmentLength());
		postmd.add(mde2);

		//process cyVisualProperty aspect
		if (md.getMetaDataElement(CyVisualPropertiesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(CyVisualPropertiesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<CyVisualPropertiesElement> it = new AspectIterator<>(netId,
					this.usingOldVisualPropertyAspect ? "visualProperties":CyVisualPropertiesElement.ASPECT_NAME, 
							CyVisualPropertiesElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
		
		// process function terms
		
		if (md.getMetaDataElement(FunctionTermElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(FunctionTermElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<FunctionTermElement> ei = new AspectIterator<>(netId,
					FunctionTermElement.ASPECT_NAME, FunctionTermElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
		
		
		Set<Long> citationIds = new TreeSet<> ();
		
		//process citation links aspects
		if (md.getMetaDataElement(NodeCitationLinksElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NodeCitationLinksElement.ASPECT_NAME);
			writer.openFragment();
			NodeCitationLinksElement worker = new NodeCitationLinksElement();
			try (AspectIterator<NodeCitationLinksElement> ei = new AspectIterator<>(netId,
					NodeCitationLinksElement.ASPECT_NAME, NodeCitationLinksElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
				
		
		if (md.getMetaDataElement(EdgeCitationLinksElement.ASPECT_NAME) != null) {
			EdgeCitationLinksElement worker = new EdgeCitationLinksElement();
			writer.startAspectFragment(EdgeCitationLinksElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<EdgeCitationLinksElement> ei = new AspectIterator<>(netId,
					EdgeCitationLinksElement.ASPECT_NAME, EdgeCitationLinksElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
				
			
		if( !citationIds.isEmpty()) {
			long citationCntr = 0;
			writer.startAspectFragment(CitationElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<CitationElement> ei = new AspectIterator<>(netId,
				CitationElement.ASPECT_NAME, CitationElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					CitationElement ft = ei.next();
					if ( citationIds.contains(ft.getId())) {
						writer.writeElement(ft);
						if (ft.getId() > citationCntr)
							citationCntr = ft.getId();
					}	
				}	
			}
			
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(CitationElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(writer.getFragmentLength());
			mde.setIdCounter(citationCntr);
			postmd.add(mde);
		}	

		// support and related aspects
		Set<Long> supportIds = new TreeSet<> ();
		
		//process support links aspects
		if (md.getMetaDataElement(NodeSupportLinksElement.ASPECT_NAME) != null) {
			NodeSupportLinksElement worker = new NodeSupportLinksElement();
			writer.startAspectFragment(NodeSupportLinksElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NodeSupportLinksElement> ei = new AspectIterator<>(netId,
					NodeSupportLinksElement.ASPECT_NAME, NodeSupportLinksElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
				
		
		if (md.getMetaDataElement(EdgeSupportLinksElement.ASPECT_NAME) != null) {
			EdgeSupportLinksElement worker = new EdgeSupportLinksElement();
			writer.startAspectFragment(EdgeSupportLinksElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<EdgeSupportLinksElement> ei = new AspectIterator<>(netId,
					EdgeSupportLinksElement.ASPECT_NAME, EdgeSupportLinksElement.class, pathPrefix)) {
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
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	

				
		if( !supportIds.isEmpty()) {
			long supportCntr = 0;
			writer.startAspectFragment(SupportElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<SupportElement> ei = new AspectIterator<>(netId,
					SupportElement.ASPECT_NAME, SupportElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					SupportElement ft = ei.next();
					if ( supportIds.contains(ft.getId())) {
						writer.writeElement(ft);
						if ( supportCntr < ft.getId())
							supportCntr=ft.getId();
					}	
				}	
			}
			
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(SupportElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(writer.getFragmentLength());
			mde.setIdCounter(supportCntr);
			postmd.add(mde);
					
		}
	}
	
	
	private MetaDataCollection prepareMetadata() {
		MetaDataCollection md = new MetaDataCollection();
		boolean hasNodeAttributes = false;
		File dir = new File(pathPrefix+netId+"/aspects");
		  File[] directoryListing = dir.listFiles();
		  for (File child : directoryListing) {
			  String aspName = child.getName();
			  MetaDataElement e;
			  if (aspName.equals(NodeAttributesElement.ASPECT_NAME))
				  hasNodeAttributes = true;
			  if (aspName.equals("visualProperties")) {
				  this.usingOldVisualPropertyAspect = true;
				   e = new MetaDataElement (CyVisualPropertiesElement.ASPECT_NAME, mdeVer);
			  } else 
			       e = new MetaDataElement (aspName, mdeVer);
			  e.setConsistencyGroup(consistencyGrp);
			  md.add(e);			  
		  }
		  if ( !hasNodeAttributes) 
			  md.add( new MetaDataElement(NodeAttributesElement.ASPECT_NAME,mdeVer));
		  return md;
	}
	
	private List<CxMetadata> prepareMetadataCX2() {
		List<CxMetadata> md = new ArrayList<>();
		
		boolean hasNetworkAttributes = false;
		
		File dir = new File(pathPrefix+netId+"/aspects_cx2");
		  File[] directoryListing = dir.listFiles();
		  for (File child : directoryListing) {
			  String aspName = child.getName();
			  CxMetadata e = new CxMetadata (aspName);
			  if (aspName.equals(CxNetworkAttribute.ASPECT_NAME))
				  hasNetworkAttributes = true;
			  md.add(e);			  
		  }
		  
		  if(!hasNetworkAttributes) 
			  md.add(new CxMetadata(CxNetworkAttribute.ASPECT_NAME));
		  return md;
	}
	
	public void interConnectQuery(OutputStream out, Set<Long> nodeIds) throws IOException {
		if (this.depth >=2 ) {
			oneStepInterConnectQuery(out,nodeIds);
		} else 
			directConnectQuery(out,nodeIds);
	}
	
	public void interConnectQueryCX2(OutputStream out, Set<Long> nodeIds, boolean preserveNodeCoordinates) throws IOException, NdexException {
		if (this.depth >=2 ) {
			oneStepInterConnectQueryCX2(out,nodeIds, preserveNodeCoordinates);
		} else 
			directConnectQueryCX2(out,nodeIds, preserveNodeCoordinates);
	}
	
	private void oneStepInterConnectQuery(OutputStream out, Set<Long> nodeIds ) throws IOException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		Map<Long, EdgesElement> edgeTable = new TreeMap<> ();
		
		//NodeId -> unique neighbor node ids
		Map<Long,NodeDegreeHelper> nodeNeighborIdTable = new TreeMap<>();
		
		Set<Long> queryNodeIds = new HashSet<>(nodeIds);

		usingOldVisualPropertyAspect = false;
		
		NdexCXNetworkWriter writer = new NdexCXNetworkWriter(out, true);
		MetaDataCollection md = prepareMetadata() ;
		writer.start();
		writer.writeMetadata(md);
		
		MetaDataCollection postmd = new MetaDataCollection();
		
		writeContextAspect(writer, md, postmd);
	
		if (md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			try (AspectIterator<EdgesElement> ei = new AspectIterator<>(netId,
					EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix )) {
				while (ei.hasNext()) {
					EdgesElement edge = ei.next();					
					if (nodeIds.contains(edge.getSource())) {
						edgeTable.put(edge.getId(), edge);
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
					} else if (nodeIds.contains(edge.getTarget())) {
//						writer.writeElement(edge);
						edgeTable.put(edge.getId(), edge);
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
		
		System.out.println( edgeTable.size()  + " edges from 2-step interconnect query.");
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
				writer.end(false, edgeLimitExceeded);
				System.out.println("Wrote the Exceed limit error and return.");
				return;
			}
			limitIsOver = true;
			//redo the edges and nodes
			finalNodes.clear();
			int i = 0;
			System.out.println("Redo edges and nodes.");

			Map<Long,EdgesElement> newTable = new HashMap<>(edgeTable.size());
			for (Map.Entry<Long,EdgesElement> entry :edgeTable.entrySet()) { 
				if (i >= edgeLimit )
					break;
				i++;	
				newTable.put(entry.getKey(), entry.getValue());
				finalNodes.add(entry.getValue().getSource());
				finalNodes.add(entry.getValue().getTarget());
			}
			edgeTable = newTable;
		}
	
		System.out.println("Start writing edges.");

		// write edge aspect 
		writer.startAspectFragment(EdgesElement.ASPECT_NAME);
		writer.openFragment();

		Set<Long> finalEdgeIds = new TreeSet<> ();
		
		// write the edges in the table first
		if ( edgeTable.size() > 0 ) {
			for (EdgesElement e : edgeTable.values()) {
				writer.writeElement(e);
				finalEdgeIds.add(e.getId());	
			}
		}
		
		
		// write extra edges that found between the new neighboring nodes.
		
		if (finalNodes.size()>0 && md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			try (AspectIterator<EdgesElement> ei = new AspectIterator<>(netId,
					EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix )) {
				while ( ei.hasNext()) {
					if ( edgeLimit > 0 &&(finalEdgeIds.size() > edgeLimit) ) {
						if ( this.errorOverLimit) {
							writer.closeFragment();
							writer.endAspectFragment();
							writer.end(false, edgeLimitExceeded);
							return;
						}
						limitIsOver = true;
						break;
					}
					
					EdgesElement edge = ei.next();					
					if ((!finalEdgeIds.contains( edge.getId())) && 
							finalNodes.contains(edge.getSource()) && finalNodes.contains(edge.getTarget())) {
						writer.writeElement(edge);
						finalEdgeIds.add(edge.getId());
					}
				}	
			}
		}	
		
		writer.closeFragment();
		writer.endAspectFragment();
		System.out.println("Query returned " + writer.getFragmentLength() + " edges.");

		if (md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME, mdeVer);
			mde.setElementCount(Long.valueOf(finalEdgeIds.size() ));
			mde.setIdCounter(finalEdgeIds.isEmpty()? 0L : Collections.max(finalEdgeIds).longValue());
			postmd.add(mde);
		}
		
		System.out.println ( "done writing out edges.");

		finalNodes.addAll(nodeIds);
		
		//write nodes
		writer.startAspectFragment(NodesElement.ASPECT_NAME);
		writer.openFragment();
		try (AspectIterator<NodesElement> ei = new AspectIterator<>(netId,
					NodesElement.ASPECT_NAME, NodesElement.class, pathPrefix)) {
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
			mde1.setElementCount(Long.valueOf(finalNodes.size()));
			mde1.setIdCounter(nodeIds.isEmpty()? 0L: Collections.max(nodeIds));
			postmd.add(mde1);
		}
		
		ArrayList<NetworkAttributesElement> provenanceRecords = new ArrayList<> (2);
		provenanceRecords.add(new NetworkAttributesElement (null, "prov:wasDerivedFrom", netId));
		provenanceRecords.add(new NetworkAttributesElement (null, "prov:wasGeneratedBy",
				"NDEx Interconnect Query/v1.1 (Query=\""+ this.searchTerms + "\")"));

		
		writeOtherAspectsForSubnetwork(finalNodes, finalEdgeIds, writer, md, postmd, limitIsOver,
				"Interconnect query result on network", provenanceRecords, queryNodeIds);
		
		writer.writeMetadata(postmd);
		writer.end();
		long t2 = Calendar.getInstance().getTimeInMillis();

	//	System ("Done - " + (t2-t1)/1000f + " seconds.");
		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + finalEdgeIds.size() + " edges and " + finalNodes.size() + " nodes.",
				new Object[]{});
	}

	private void directConnectQuery(OutputStream out, Set<Long> nodeIds ) throws IOException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		
		Set<Long> edgeIds = new TreeSet<> ();
		Set<Long> queryNodeIds = new HashSet<>(nodeIds);

		
		usingOldVisualPropertyAspect = false;
		
		NdexCXNetworkWriter writer = new NdexCXNetworkWriter(out, true);
		MetaDataCollection md = prepareMetadata() ;
		writer.start();
		writer.writeMetadata(md);
		
		MetaDataCollection postmd = new MetaDataCollection();
		
		writeContextAspect(writer, md, postmd);
	
		//write nodes
		int cnt = 0; 
		writer.startAspectFragment(NodesElement.ASPECT_NAME);
		writer.openFragment();
		try (AspectIterator<NodesElement> ei = new AspectIterator<>(netId,
					NodesElement.ASPECT_NAME, NodesElement.class, pathPrefix)) {
			while (ei.hasNext()) {
				NodesElement node = ei.next();
				if (nodeIds.contains(Long.valueOf(node.getId()))) {
						writer.writeElement(node);
						cnt++;
						if (cnt == nodeIds.size())
							break;
				}
			}
		}
		writer.closeFragment();
		writer.endAspectFragment();
		if ( nodeIds.size()>0) {
			MetaDataElement mde1 = new MetaDataElement(NodesElement.ASPECT_NAME,mdeVer);
			mde1.setElementCount(Long.valueOf(nodeIds.size()));
			mde1.setIdCounter(nodeIds.isEmpty()? 0L:Collections.max(nodeIds));
			postmd.add(mde1);
		}
		
		cnt = 0;
		boolean limitIsOver = false;
		writer.startAspectFragment(EdgesElement.ASPECT_NAME);
		writer.openFragment();

		if (md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			try (AspectIterator<EdgesElement> ei = new AspectIterator<>(netId,
					EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix )) {
				while (ei.hasNext()) {
					EdgesElement edge = ei.next();					
					if (nodeIds.contains(edge.getSource()) && nodeIds.contains(edge.getTarget())) {
						cnt ++;
						if ( edgeLimit > 0 && cnt > edgeLimit) {
							if ( this.errorOverLimit) {
								writer.closeFragment();
								writer.endAspectFragment();
								writer.end(false, edgeLimitExceeded);
								return;
							}
							limitIsOver = true;
							break;
						}
						writer.writeElement(edge);
						edgeIds.add(edge.getId());
					} 
				}
			}
		}
		
		System.out.println( edgeIds.size()  + " edges from directConnection query.");
		writer.closeFragment();
		writer.endAspectFragment();

		
		MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME, mdeVer);
		mde.setElementCount(Long.valueOf(edgeIds.size()));
		mde.setIdCounter((edgeIds.isEmpty() ? 0L : Collections.max(edgeIds)));
		postmd.add(mde);
		
		ArrayList<NetworkAttributesElement> provenanceRecords = new ArrayList<> (2);
		provenanceRecords.add(new NetworkAttributesElement (null, "prov:wasDerivedFrom", netId));
		provenanceRecords.add(new NetworkAttributesElement (null, "prov:wasGeneratedBy",
				"NDEx Direct Query/v1.1 (Query terms=\""+ this.searchTerms + "\")"));

		
		writeOtherAspectsForSubnetwork(nodeIds, edgeIds, writer, md, postmd, limitIsOver,
				"Direct query result on network", provenanceRecords, queryNodeIds);
		
		writer.writeMetadata(postmd);
		writer.end();
		long t2 = Calendar.getInstance().getTimeInMillis();

		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeIds.size() + " edges and " +
		    nodeIds.size() + " nodes.",
				new Object[]{});
	}
	
	
	
	private void writeContextAspect(NdexCXNetworkWriter writer, MetaDataCollection md, MetaDataCollection postmd)
			throws IOException, JsonProcessingException {
		//process namespace aspect	
		if (md.getMetaDataElement(NamespacesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NamespacesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NamespacesElement> ei = new AspectIterator<>(netId,
						NamespacesElement.ASPECT_NAME, NamespacesElement.class, pathPrefix)) {
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
	
	
	/**
	 * Generate a new unique name for the querynode attribute on node
	 * @param decl
	 * @return
	 * @throws NdexException 
	 */
	private static String getNewNameForQuerynodeAttr(Map<String,DeclarationEntry> nodeAttrs) throws NdexException {
		int counter = 1;
		while ( counter < 3000) {
			String newAttr = queryNode + "(" + counter + ")";
			if(!nodeAttrs.containsKey(newAttr))
				return newAttr;
		}
		throw new NdexException("Failed to find new attribute name for queryNode attribute on node.");
	}
	
	/**
	 * CX2 version of neighborhood query. This function returns writes the result network in CX2 format to the output stream. 
	 * @param out
	 * @param nodeIds
	 * @throws IOException
	 * @throws NdexException 
	 */
	public void neighbourhoodQueryCX2(OutputStream out, Set<Long> nodeIds, boolean flagQueryNodeInResult, 
			boolean preserveNodeCoordinates) throws IOException, NdexException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		Set<Long> edgeIds = new TreeSet<> ();
		usingOldVisualPropertyAspect = false;
		
		boolean limitIsOver = false;
		Set<Long> queryNodeIds = new HashSet<>(nodeIds);
		String queryName = directOnly ? "Adjacent" : "Neighborhood" ;

		
		CXWriter writer = new CXWriter(out, true);
		List<CxMetadata> md = prepareMetadataCX2() ;
		writer.writeMetadata(md);
				
		int cnt = 0;
		
		//check if the network has old queryNode column on nodes. null mean no attribute called "querynode" on nodes. 
		CxAttributeDeclaration decl = getAttrDeclarationFromAspectFile(md);
        
		String newQueryNodeAttr = null;

		try {
			newQueryNodeAttr = renameQueryNodeAttrName(decl,writer);
		} catch (NdexException e) {
				writer.printError(e.getMessage());
				return;
		}	
        
		//prepare networkAttributes and write declaration out;
		CxNetworkAttribute netAttrs = prepareNetworkAttributesAndWriteOutDeclaration(decl, md,newQueryNodeAttr, writer,queryName);
		
		boolean hasEdges = md.stream().anyMatch(m -> m.getName().equals(CxEdge.ASPECT_NAME));
	
		if (hasEdges) {
			
			Set<Long> startingNodeIds = nodeIds;
			writer.startAspectFragment(CxEdge.ASPECT_NAME);
			
			for (int i = 0; i < depth; i++) {
				if (limitIsOver) 
					break;
				Set<Long> newNodeIds = new TreeSet<> ();
				String edgeFilePath = pathPrefix + netId + "/aspects_cx2/" + CxEdge.ASPECT_NAME;
				try (AspectIterator<CxEdge> ei = new AspectIterator<>( edgeFilePath, CxEdge.class)) {
					while (ei.hasNext()) {
						CxEdge edge = ei.next();
						if (!edgeIds.contains(edge.getId())) {
							if (startingNodeIds.contains(edge.getSource())
									|| startingNodeIds.contains(edge.getTarget())) {
								cnt ++;
								if ( edgeLimit > 0 && cnt > edgeLimit) {
									if ( this.errorOverLimit) {
										writer.endAspectFragment();
										writer.printError(edgeLimitExceeded);
										return;
									}
									limitIsOver = true;
									break;
								}
								writer.writeElementInFragment(edge);
								edgeIds.add(edge.getId());
								if (!nodeIds.contains(edge.getSource()))
									newNodeIds.add(edge.getSource());
								if ( !nodeIds.contains(edge.getTarget()))
									newNodeIds.add(edge.getTarget());
							}

						}

					}
					nodeIds.addAll(newNodeIds);
					startingNodeIds=newNodeIds;
				}

			}
			accLogger.info("Query returned " + cnt + " edges.");
			writer.endAspectFragment();
			
		}
		
		accLogger.info ( "done writing out edges.");
		//write nodes
		writer.startAspectFragment(CxNode.ASPECT_NAME);
		try (AspectIterator<CxNode> ei = new AspectIterator<>( pathPrefix + netId + "/aspects_cx2/" + CxNode.ASPECT_NAME, CxNode.class)) {
			while (ei.hasNext()) {
				CxNode node = ei.next();
				if (nodeIds.contains(Long.valueOf(node.getId()))) {
					if ( queryNodeIds.contains(node.getId())) {
						Map<String,Object> attrs = node.getAttributes();
					    if (newQueryNodeAttr !=null ) {   // rename querynode to a new attribute name
					    		Object v = node.getAttributes().remove(queryNode) ;
					    		attrs.put(newQueryNodeAttr, v);
					    }
					    attrs.put(queryNode, true);
					}
					if(!preserveNodeCoordinates)
						node.removeCoordinates();
					writer.writeElementInFragment(node);
				}
			}
		}
		
		writer.endAspectFragment();
/*		if ( nodeIds.size()>0) {
			MetaDataElement mde1 = new MetaDataElement(NodesElement.ASPECT_NAME,mdeVer);
			mde1.setElementCount((long)nodeIds.size());
			mde1.setIdCounter(nodeIds.isEmpty()? 0L : Collections.max(nodeIds));
			postmd.add(mde1);
		} */
		
		//check if we need to output the full neighborhood.
		if ( !directOnly && hasEdges) {
			writer.startAspectFragment(CxEdge.ASPECT_NAME);

			try (AspectIterator<CxEdge> ei = new AspectIterator<>( pathPrefix + netId + "/aspects_cx2/" + CxEdge.ASPECT_NAME, CxEdge.class)) {
				while (ei.hasNext()) {
					CxEdge edge = ei.next();
					if ( (!edgeIds.contains(edge.getId())) && nodeIds.contains(edge.getSource())
								&& nodeIds.contains(edge.getTarget())) {
							cnt ++;
							if ( edgeLimit > 0 && cnt > edgeLimit) {
								if ( this.errorOverLimit) {
									writer.endAspectFragment();
									writer.printError(edgeLimitExceeded);
									return;
								}
								limitIsOver = true;
								break;
							}
							writer.writeElementInFragment(edge);
							edgeIds.add(edge.getId());
					}
				}
			}
			writer.endAspectFragment();
		}
		
/*		if  (edgeIds.size()>0) {
			MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)edgeIds.size());
			mde.setIdCounter(edgeIds.isEmpty()? 0L : Collections.max(edgeIds));
			postmd.add(mde);
		}

		String queryName = directOnly ? "Adjacent" : "Neighborhood" ;
		ArrayList<NetworkAttributesElement> provenanceRecords = new ArrayList<> (2);
		provenanceRecords.add(new NetworkAttributesElement (null, provDerivedFrom, netId));
		provenanceRecords.add(new NetworkAttributesElement (null, provGeneratedBy,
				"NDEx "+ queryName + " Query/v1.1 (Depth=" + this.depth +"; Query terms=\""+ this.searchTerms + "\")"));
*/		

		//finalize network attributes and write them out.
		if (limitIsOver) {
			netAttrs.add(edgeLimitExceeded, Boolean.TRUE);
		}
		
		writer.startAspectFragment(CxNetworkAttribute.ASPECT_NAME);
		writer.writeElementInFragment(netAttrs);
		writer.endAspectFragment();

		writeOtherAspectsForSubnetworkCX2(nodeIds, edgeIds, writer, limitIsOver,
				queryName + " query result on network", md);
		
		//writer.writeMetadata(postmd);
		writer.finish();
		long t2 = Calendar.getInstance().getTimeInMillis();

		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeIds.size() + " edges and " + nodeIds.size() + " nodes.",
				new Object[]{});
	}

	
	private CxAttributeDeclaration getAttrDeclarationFromAspectFile(List<CxMetadata> md) throws JsonProcessingException, IOException {
		CxAttributeDeclaration decl= null;
		if (md.stream().anyMatch(m -> m.getName().equals(CxAttributeDeclaration.ASPECT_NAME))) {
			try (AspectIterator<CxAttributeDeclaration> ei = new AspectIterator<>(  pathPrefix + netId + "/aspects_cx2/" + CxAttributeDeclaration.ASPECT_NAME, CxAttributeDeclaration.class)) {
				while (ei.hasNext()) {
				  decl = ei.next();
				  break;
				}
			}
		}
		if ( decl == null) {
			decl = new CxAttributeDeclaration();
		}	
		return decl;
	}
	
	private String renameQueryNodeAttrName(CxAttributeDeclaration decl, CXWriter writer) throws NdexException {
		String newQueryNodeAttr = null;
		Map<String,DeclarationEntry> nodeAttrs = decl.getAttributesInAspect(CxNode.ASPECT_NAME);	
		if ( nodeAttrs.containsKey(queryNode)) {
			DeclarationEntry oldE = nodeAttrs.remove(queryNode);
			newQueryNodeAttr = getNewNameForQuerynodeAttr(nodeAttrs);
		    nodeAttrs.put(newQueryNodeAttr, oldE);
		}
		nodeAttrs.put(queryNode, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.BOOLEAN,Boolean.FALSE,null));
	
        return newQueryNodeAttr;
	}
	
	private CxNetworkAttribute prepareNetworkAttributesAndWriteOutDeclaration(CxAttributeDeclaration decl, 
			List<CxMetadata> md, String newQueryNodeAttr, CXWriter writer, String queryName) throws JsonProcessingException, IOException, NdexException {
	
		Map<String,DeclarationEntry> netAttrDecl = decl.getAttributesInAspect(CxNetworkAttribute.ASPECT_NAME);
		if ( netAttrDecl == null) {
			netAttrDecl = new HashMap<>();
			decl.add(CxNetworkAttribute.ASPECT_NAME, netAttrDecl);
		}
        
		if (!netAttrDecl.containsKey(CxNetworkAttribute.nameAttribute)) {
			netAttrDecl.put(CxNetworkAttribute.nameAttribute, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.STRING,null,null));
		}

		
		decl.addAttributeDeclaration(CxNode.ASPECT_NAME, queryNode, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.BOOLEAN,Boolean.FALSE,null));
		netAttrDecl.put(provDerivedFrom, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.STRING,null,null));
		netAttrDecl.put(provGeneratedBy, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.STRING,null,null));
		netAttrDecl.put(edgeLimitExceeded, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.BOOLEAN,null,null));
		
		//prepare networkAttributes
		CxNetworkAttribute netAttrs = null;
		if (md.stream().anyMatch(m -> m.getName().equals(CxNetworkAttribute.ASPECT_NAME))) {
			try (AspectIterator<CxNetworkAttribute> nai = new AspectIterator<>(  pathPrefix + netId + "/aspects_cx2/" + CxNetworkAttribute.ASPECT_NAME,
					CxNetworkAttribute.class)) {
				while (nai.hasNext()) {
				  netAttrs = nai.next();
				  break;	  
				}
			} 
		}
		if(netAttrs == null)
			netAttrs = new CxNetworkAttribute();

		//String queryName = directOnly ? "Adjacent" : "Neighborhood" ;

		String oldname = netAttrs.getNetworkName();
        if (oldname == null)
        	oldname = "";
        netAttrs.add(CxNetworkAttribute.nameAttribute, queryName + " query on network - "+ oldname);
		
		netAttrs.add(provDerivedFrom, netId.toString());
		netAttrs.add(provGeneratedBy, "NDEx "+ queryName + " Query/v1.1 (Depth=" + this.depth +
				"; Query terms=\""+ (this.searchTerms != null?this.searchTerms:"") + "\")");
		
		if (newQueryNodeAttr !=null) {
			String commentsAttr = "NDEx_Query_Comments";
			netAttrs.add(commentsAttr, "All node attributes 'querynode' in your parenent network have been renamed to '" + newQueryNodeAttr + "' in the query result.");
			decl.addAttributeDeclaration(CxNetworkAttribute.ASPECT_NAME, commentsAttr, new DeclarationEntry(ATTRIBUTE_DATA_TYPE.STRING,null,null));
		}
		
		//write out declaration.
		writer.startAspectFragment(CxAttributeDeclaration.ASPECT_NAME);
		writer.writeElementInFragment(decl);
		writer.endAspectFragment();
		return netAttrs;
	}
	
	
	private void writeOtherAspectsForSubnetworkCX2(Set<Long> nodeIds, Set<Long> edgeIds, CXWriter writer,
			 boolean limitIsOver, String networkNamePrefix,  List<CxMetadata> md) throws IOException, JsonProcessingException, NdexException {

		//write out styles
		if (md.stream().anyMatch( x -> x.getName().equals(CxVisualProperty.ASPECT_NAME))) {
			writer.startAspectFragment(CxVisualProperty.ASPECT_NAME);
			try (AspectIterator<CxVisualProperty> it = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxVisualProperty.ASPECT_NAME, CxVisualProperty.class)) {
				while (it.hasNext()) {
					writer.writeElementInFragment(it.next());
				}
			}
			writer.endAspectFragment();
		}	
		
		//process bypass aspects
		if (md.stream().anyMatch( x -> x.getName().equals(CxNodeBypass.ASPECT_NAME))) {
			writer.startAspectFragment(CxNodeBypass.ASPECT_NAME);
			try (AspectIterator<CxNodeBypass> it = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxNodeBypass.ASPECT_NAME, CxNodeBypass.class)) {
				while (it.hasNext()) {
					CxNodeBypass elmt = it.next();
					if ( nodeIds.contains(elmt.getId()) ){
						writer.writeElementInFragment(elmt);
					}
				}
			}
			writer.endAspectFragment();
		}	
		if (md.stream().anyMatch( x -> x.getName().equals(CxEdgeBypass.ASPECT_NAME))) {
			writer.startAspectFragment(CxEdgeBypass.ASPECT_NAME);
			try (AspectIterator<CxEdgeBypass> it = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxEdgeBypass.ASPECT_NAME, CxEdgeBypass.class)) {
				while (it.hasNext()) {
					CxEdgeBypass elmt = it.next();
					if ( edgeIds.contains(elmt.getId()) ){
						writer.writeElementInFragment(elmt);
					}
				}
			}
			writer.endAspectFragment();
		}	

		if ( md.stream().anyMatch(x -> x.getName().equals(VisualEditorProperties.ASPECT_NAME))) {
			writer.writeAspectFromAspectFile(VisualEditorProperties.ASPECT_NAME, 
					pathPrefix + netId + "/aspects_cx2/" + VisualEditorProperties.ASPECT_NAME);
			
		}
		
		// process function terms
		
		if (md.stream().anyMatch( x -> x.getName().equals(FunctionTermElement.ASPECT_NAME))) {
			writer.startAspectFragment(FunctionTermElement.ASPECT_NAME);
			try (AspectIterator<FunctionTermElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + FunctionTermElement.ASPECT_NAME, FunctionTermElement.class)) {
					while (ei.hasNext()) {
						FunctionTermElement ft = ei.next();
						if (nodeIds.contains(ft.getNodeID())) {
								writer.writeCx1ElementInFragment(ft);
						}
					}
			}
			writer.endAspectFragment();
		}	
		
		
		Set<Long> citationIds = new TreeSet<> ();
		
		//process citation links aspects
		if (md.stream().anyMatch( x -> x.getName().equals(NodeCitationLinksElement.ASPECT_NAME))) {
			writer.startAspectFragment(NodeCitationLinksElement.ASPECT_NAME);
			NodeCitationLinksElement worker = new NodeCitationLinksElement();
			try (AspectIterator<NodeCitationLinksElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + NodeCitationLinksElement.ASPECT_NAME, NodeCitationLinksElement.class)) {
					while (ei.hasNext()) {
						NodeCitationLinksElement ft = ei.next();
						worker.getSourceIds().clear();
						for ( Long nid : ft.getSourceIds()) {
							if ( nodeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setCitationIds(ft.getCitationIds());
							writer.writeCx1ElementInFragment(worker);
							citationIds.addAll(worker.getCitationIds());
						}	
					}
			}
			writer.endAspectFragment();
		}	
				
		
		if (md.stream().anyMatch( x -> x.getName().equals(EdgeCitationLinksElement.ASPECT_NAME))) {
			EdgeCitationLinksElement worker = new EdgeCitationLinksElement();
			writer.startAspectFragment(EdgeCitationLinksElement.ASPECT_NAME);
			try (AspectIterator<EdgeCitationLinksElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + EdgeCitationLinksElement.ASPECT_NAME, EdgeCitationLinksElement.class)) {
					while (ei.hasNext()) {
						EdgeCitationLinksElement ft = ei.next();
						for ( Long nid : ft.getSourceIds()) {
							if ( edgeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setCitationIds(ft.getCitationIds());
							writer.writeCx1ElementInFragment(worker);
							citationIds.addAll(worker.getCitationIds());
							worker.getSourceIds().clear();
						}	
					}
			}
			writer.endAspectFragment();
		}	
				
			
		if( !citationIds.isEmpty()) {
			long citationCntr = 0;
			writer.startAspectFragment(CitationElement.ASPECT_NAME);
			try (AspectIterator<CitationElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CitationElement.ASPECT_NAME, CitationElement.class)) {
				while (ei.hasNext()) {
					CitationElement ft = ei.next();
					if ( citationIds.contains(ft.getId())) {
						writer.writeCx1ElementInFragment(ft);
						if (ft.getId() > citationCntr)
							citationCntr = ft.getId();
					}	
				}	
			}
			
			writer.endAspectFragment();
		}	

		// support and related aspects
		Set<Long> supportIds = new TreeSet<> ();
		
		//process support links aspects
		if (md.stream().anyMatch( x -> x.getName().equals(NodeSupportLinksElement.ASPECT_NAME))) {
			NodeSupportLinksElement worker = new NodeSupportLinksElement();
			writer.startAspectFragment(NodeSupportLinksElement.ASPECT_NAME);
			try (AspectIterator<NodeSupportLinksElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/spects_cx2/" + NodeSupportLinksElement.ASPECT_NAME, NodeSupportLinksElement.class)) {
					while (ei.hasNext()) {
						NodeSupportLinksElement ft = ei.next();
						for ( Long nid : ft.getSourceIds()) {
							if ( nodeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setSupportIds(ft.getSupportIds());
							writer.writeCx1ElementInFragment(worker);
							supportIds.addAll(worker.getSupportIds());
							worker.getSourceIds().clear();
						}	
					}
			}
			writer.endAspectFragment();
		}	
				
		
		if (md.stream().anyMatch( x -> x.getName().equals(EdgeSupportLinksElement.ASPECT_NAME) )) {
			EdgeSupportLinksElement worker = new EdgeSupportLinksElement();
			writer.startAspectFragment(EdgeSupportLinksElement.ASPECT_NAME);
			try (AspectIterator<EdgeSupportLinksElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" +EdgeSupportLinksElement.ASPECT_NAME, EdgeSupportLinksElement.class)) {
					while (ei.hasNext()) {
						EdgeSupportLinksElement ft = ei.next();
						for ( Long nid : ft.getSourceIds()) {
							if ( edgeIds.contains(nid))
								worker.getSourceIds().add(nid);
						}
						if ( worker.getSourceIds().size()>0) {
							worker.setSupportIds(ft.getSupportIds());
							writer.writeCx1ElementInFragment(worker);
							supportIds.addAll(worker.getSupportIds());
							worker.getSourceIds().clear();
						}	
					}
			}
			writer.endAspectFragment();
			
		}	

				
		if( !supportIds.isEmpty()) {
			long supportCntr = 0;
			writer.startAspectFragment(SupportElement.ASPECT_NAME);
			try (AspectIterator<SupportElement> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + SupportElement.ASPECT_NAME, SupportElement.class)) {
				while (ei.hasNext()) {
					SupportElement ft = ei.next();
					if ( supportIds.contains(ft.getId())) {
						writer.writeCx1ElementInFragment(ft);
						if ( supportCntr < ft.getId())
							supportCntr=ft.getId();
					}	
				}	
			}
			
			writer.endAspectFragment();
					
		}
	}
	
	
	private void directConnectQueryCX2(OutputStream out, Set<Long> nodeIds, boolean preserveNodeCoordinates ) throws IOException, NdexException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		
		Set<Long> edgeIds = new TreeSet<> ();
		//Set<Long> queryNodeIds = new HashSet<>(nodeIds);
		String queryName = "Direct";

				
		CXWriter writer = new CXWriter(out, true);
		List<CxMetadata> md = prepareMetadataCX2() ;
		writer.writeMetadata(md);
		
		CxAttributeDeclaration decl = getAttrDeclarationFromAspectFile(md);

		String newQueryNodeAttr = null;

		try {
			newQueryNodeAttr = renameQueryNodeAttrName(decl,writer);
		} catch (NdexException e) {
				writer.printError(e.getMessage());
				return;
		}	
        
		//prepare networkAttributes and write declaration out;
		CxNetworkAttribute netAttrs = prepareNetworkAttributesAndWriteOutDeclaration(decl, md,newQueryNodeAttr, writer,queryName);
	
		//write nodes
		int cnt = 0; 
		writer.startAspectFragment(CxNode.ASPECT_NAME);
		if (md.stream().anyMatch(x -> x.getName().equals(CxNode.ASPECT_NAME))) {
			try (AspectIterator<CxNode> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxNode.ASPECT_NAME, CxNode.class)) {
				while (ei.hasNext()) {
					CxNode node = ei.next();
					if (nodeIds.contains(Long.valueOf(node.getId()))) {
						if ( !preserveNodeCoordinates)
							node.removeCoordinates();
						writer.writeElementInFragment(node);
						cnt++;
						if (cnt == nodeIds.size())
							break;
					}
				}
			}
		}
		writer.endAspectFragment();
	/*	if ( nodeIds.size()>0) {
			MetaDataElement mde1 = new MetaDataElement(NodesElement.ASPECT_NAME,mdeVer);
			mde1.setElementCount(Long.valueOf(nodeIds.size()));
			mde1.setIdCounter(nodeIds.isEmpty()? 0L:Collections.max(nodeIds));
			postmd.add(mde1);
		}*/
		
		cnt = 0;
		boolean limitIsOver = false;
		writer.startAspectFragment(CxEdge.ASPECT_NAME);

		if (md.stream().anyMatch(x -> x.getName().equals(EdgesElement.ASPECT_NAME) )) {
			try (AspectIterator<CxEdge> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" +CxEdge.ASPECT_NAME, CxEdge.class )) {
				while (ei.hasNext()) {
					CxEdge edge = ei.next();					
					if (nodeIds.contains(edge.getSource()) && nodeIds.contains(edge.getTarget())) {
						cnt ++;
						if ( edgeLimit > 0 && cnt > edgeLimit) {
							if ( this.errorOverLimit) {
								writer.endAspectFragment();
								writer.printError( edgeLimitExceeded);
								return;
							}
							limitIsOver = true;
							break;
						}
						writer.writeElementInFragment (edge);
						edgeIds.add(edge.getId());
					} 
				}
			}
		}
		
		System.out.println( edgeIds.size()  + " edges from directConnection query.");
		writer.endAspectFragment();

		
		//finalize network attributes and write them out.
		if (limitIsOver) {
			netAttrs.add(edgeLimitExceeded, Boolean.TRUE);
		}
		
		writer.startAspectFragment(CxNetworkAttribute.ASPECT_NAME);
		writer.writeElementInFragment(netAttrs);
		writer.endAspectFragment();
		
		writeOtherAspectsForSubnetworkCX2(nodeIds, edgeIds, writer, limitIsOver,
				"Direct query result on network", md);
		
		writer.finish();
		long t2 = Calendar.getInstance().getTimeInMillis();

		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeIds.size() + " edges and " +
		    nodeIds.size() + " nodes.",
				new Object[]{});
	}

	private void oneStepInterConnectQueryCX2(OutputStream out, Set<Long> nodeIds, boolean preserveNodeCoordinates ) throws IOException, NdexException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		Map<Long, CxEdge> edgeTable = new TreeMap<> ();
		String queryName = "Interconnect";
		
		//NodeId -> unique neighbor node ids
		Map<Long,NodeDegreeHelper> nodeNeighborIdTable = new TreeMap<>();
		
		//Set<Long> queryNodeIds = new HashSet<>(nodeIds);
		
		CXWriter writer = new CXWriter(out, true);
		List<CxMetadata> md = prepareMetadataCX2() ;
		writer.writeMetadata(md);
	
		//check if the network has old queryNode column on nodes. null mean no attribute called "querynode" on nodes. 
		CxAttributeDeclaration decl = getAttrDeclarationFromAspectFile(md);
        
		String newQueryNodeAttr = null;

		try {
			newQueryNodeAttr = renameQueryNodeAttrName(decl,writer);
		} catch (NdexException e) {
				writer.printError(e.getMessage());
				return;
		}	
        
		//prepare networkAttributes and write declaration out;
		CxNetworkAttribute netAttrs = prepareNetworkAttributesAndWriteOutDeclaration(decl, md,newQueryNodeAttr, writer,queryName);
			
		boolean hasEdges = md.stream().anyMatch(x->x.getName().equals(CxEdge.ASPECT_NAME));
		if (hasEdges) {
			try (AspectIterator<CxEdge> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" +CxEdge.ASPECT_NAME, CxEdge.class )) {
				while (ei.hasNext()) {
					CxEdge edge = ei.next();					
					if (nodeIds.contains(edge.getSource())) {
						edgeTable.put(edge.getId(), edge);
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
					} else if (nodeIds.contains(edge.getTarget())) {
//						writer.writeElement(edge);
						edgeTable.put(edge.getId(), edge);
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
		
		System.out.println( edgeTable.size()  + " edges from 2-step interconnect query.");
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
				writer.printError(edgeLimitExceeded);
				System.out.println("Wrote the Exceed limit error and return.");
				return;
			}
			limitIsOver = true;
			//redo the edges and nodes
			finalNodes.clear();
			int i = 0;
			System.out.println("Redo edges and nodes.");

			Map<Long,CxEdge> newTable = new HashMap<>(edgeTable.size());
			for (Map.Entry<Long,CxEdge> entry :edgeTable.entrySet()) { 
				if (i >= edgeLimit )
					break;
				i++;	
				newTable.put(entry.getKey(), entry.getValue());
				finalNodes.add(entry.getValue().getSource());
				finalNodes.add(entry.getValue().getTarget());
			}
			edgeTable = newTable;
		}
	
		System.out.println("Start writing edges.");

		// write edge aspect 
		writer.startAspectFragment(EdgesElement.ASPECT_NAME);

		Set<Long> finalEdgeIds = new TreeSet<> ();
		
		// write the edges in the table first
		if ( edgeTable.size() > 0 ) {
			for (CxEdge e : edgeTable.values()) {
				writer.writeElementInFragment(e);
				finalEdgeIds.add(e.getId());	
			}
		}
		
		
		// write extra edges that found between the new neighboring nodes.
		
		if (finalNodes.size()>0 && hasEdges) {
			try (AspectIterator<CxEdge> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" +CxEdge.ASPECT_NAME, CxEdge.class )) {
				while ( ei.hasNext()) {
					if ( edgeLimit > 0 &&(finalEdgeIds.size() > edgeLimit) ) {
						if ( this.errorOverLimit) {
							writer.endAspectFragment();
							writer.printError(edgeLimitExceeded);
							return;
						}
						limitIsOver = true;
						break;
					}
					
					CxEdge edge = ei.next();					
					if ((!finalEdgeIds.contains( edge.getId())) && 
							finalNodes.contains(edge.getSource()) && finalNodes.contains(edge.getTarget())) {
						writer.writeElementInFragment(edge);
						finalEdgeIds.add(edge.getId());
					}
				}	
			}
		}	
		
		writer.endAspectFragment();

		System.out.println ( "done writing out edges.");

		finalNodes.addAll(nodeIds);
		
		//write nodes
		writer.startAspectFragment(CxNode.ASPECT_NAME);
		try (AspectIterator<CxNode> ei = new AspectIterator<>(
				pathPrefix + netId + "/aspects_cx2/" +CxNode.ASPECT_NAME, CxNode.class)) {
			while (ei.hasNext()) {
				CxNode node = ei.next();
				if (finalNodes.contains(Long.valueOf(node.getId()))) {
					if ( !preserveNodeCoordinates)
						node.removeCoordinates();
					writer.writeElementInFragment(node);
				}
			}
		}
		writer.endAspectFragment();
		
		//finalize network attributes and write them out.
		if (limitIsOver) {
			netAttrs.add(edgeLimitExceeded, Boolean.TRUE);
		}
		
		writer.startAspectFragment(CxNetworkAttribute.ASPECT_NAME);
		writer.writeElementInFragment(netAttrs);
		writer.endAspectFragment();
		
		writeOtherAspectsForSubnetworkCX2(finalNodes, finalEdgeIds, writer,limitIsOver,
				"Interconnect query result on network", md);
		
		writer.finish();
		long t2 = Calendar.getInstance().getTimeInMillis();

	//	System ("Done - " + (t2-t1)/1000f + " seconds.");
		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + finalEdgeIds.size() + " edges and " + finalNodes.size() + " nodes.",
				new Object[]{});
	}
	
	
	/**
	 * Query function for Hiview. This is a special direct query. It only add one extra network attribute to the result network, 
	 * the parentNetwork's last modification time. It doesn't add the query node attribute. It supports filter criteria so it can 
	 * rank and filter edges in query results.   
	 * @param out
	 * @param nodeIds
	 * @param parentNetworkTimestamp
	 * @throws IOException
	 * @throws NdexException
	 */
	public void filteredDirectQueryCX2(OutputStream out, Set<Long> nodeIds, FilterCriterion edgeFilterCriterion, long parentNetworkTimestamp,
			int edgeLimit, String order, boolean preserveNodeCoordinates ) throws IOException, NdexException {
		long t1 = Calendar.getInstance().getTimeInMillis();
		
		Set<Long> edgeIds = new TreeSet<> ();
				
		CXWriter writer = new CXWriter(out, true);
		List<CxMetadata> md = prepareMetadataCX2() ;
		writer.writeMetadata(md);
		
		CxAttributeDeclaration decl = getAttrDeclarationFromAspectFile(md);
        
		//prepare networkAttributes and write declaration out;
		decl.addAttributeDeclaration(CxNetworkAttribute.ASPECT_NAME, parentNetworkModificationTime, new  DeclarationEntry(ATTRIBUTE_DATA_TYPE.LONG,null,null));

		//prepare networkAttributes
		CxNetworkAttribute netAttrs = null;
		if (md.stream().anyMatch(m -> m.getName().equals(CxNetworkAttribute.ASPECT_NAME))) {
			try (AspectIterator<CxNetworkAttribute> nai = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxNetworkAttribute.ASPECT_NAME, CxNetworkAttribute.class)) {
				while (nai.hasNext()) {
					netAttrs = nai.next();
					break;
				}
			}
		}
		if (netAttrs == null)
			netAttrs = new CxNetworkAttribute();
		
		netAttrs.add(parentNetworkModificationTime, parentNetworkTimestamp);
		
		writer.startAspectFragment(CxNetworkAttribute.ASPECT_NAME);
		writer.writeElementInFragment(netAttrs);
		writer.endAspectFragment();
		
		//write nodes
		int cnt = 0; 
		writer.startAspectFragment(CxNode.ASPECT_NAME);
		if (md.stream().anyMatch(x -> x.getName().equals(CxNode.ASPECT_NAME))) {
			try (AspectIterator<CxNode> ei = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxNode.ASPECT_NAME, CxNode.class)) {
				while (ei.hasNext()) {
					CxNode node = ei.next();
					if (nodeIds.contains(Long.valueOf(node.getId()))) {
						if ( !preserveNodeCoordinates)
							node.removeCoordinates();
						writer.writeElementInFragment(node);
						cnt++;
						if (cnt == nodeIds.size())
							break;
					}
				}
			}
		}
		writer.endAspectFragment();
		
		cnt = 0;
		
		writer.startAspectFragment(CxEdge.ASPECT_NAME);

		if ( edgeFilterCriterion !=null ) {
			EdgeFilter edgeFilter = new EdgeFilter(edgeFilterCriterion, edgeLimit, order, pathPrefix + netId + "/aspects_cx2/");
        
			for ( CxEdge edge: edgeFilter.filterTopN()) {
				writer.writeElementInFragment(edge);
				edgeIds.add(edge.getId());
			}	
		} else {
			if (md.stream().anyMatch(x -> x.getName().equals(EdgesElement.ASPECT_NAME))) {
				try (AspectIterator<CxEdge> ei = new AspectIterator<>(
						pathPrefix + netId + "/aspects_cx2/" + CxEdge.ASPECT_NAME, CxEdge.class)) {
					while (ei.hasNext()) {
						CxEdge edge = ei.next();
						if (nodeIds.contains(edge.getSource()) && nodeIds.contains(edge.getTarget())) {
							cnt++;
							if (edgeLimit > 0 && cnt > edgeLimit) {
								break;
							}
							writer.writeElementInFragment(edge);
							edgeIds.add(edge.getId());
						}
					}
				}
			}
		}
		System.out.println( edgeIds.size()  + " edges from filtered direct query.");
		writer.endAspectFragment();

	
		writeStyles(nodeIds, edgeIds, writer,md);
		
		writer.finish();
		long t2 = Calendar.getInstance().getTimeInMillis();

		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeIds.size() + " edges and " +
		    nodeIds.size() + " nodes.",
				new Object[]{});
	}

	
	private void writeStyles(Set<Long> nodeIds, Set<Long> edgeIds, CXWriter writer,
			  List<CxMetadata> md) throws IOException, JsonProcessingException, NdexException {

		//write out styles
		if (md.stream().anyMatch( x -> x.getName().equals(CxVisualProperty.ASPECT_NAME))) {
			writer.startAspectFragment(CxVisualProperty.ASPECT_NAME);
			try (AspectIterator<CxVisualProperty> it = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxVisualProperty.ASPECT_NAME, CxVisualProperty.class)) {
				while (it.hasNext()) {
					writer.writeElementInFragment(it.next());
				}
			}
			writer.endAspectFragment();
		}	
		
		//process bypass aspects
		if (md.stream().anyMatch( x -> x.getName().equals(CxNodeBypass.ASPECT_NAME))) {
			writer.startAspectFragment(CxNodeBypass.ASPECT_NAME);
			try (AspectIterator<CxNodeBypass> it = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxNodeBypass.ASPECT_NAME, CxNodeBypass.class)) {
				while (it.hasNext()) {
					CxNodeBypass elmt = it.next();
					if ( nodeIds.contains(elmt.getId()) ){
						writer.writeElementInFragment(elmt);
					}
				}
			}
			writer.endAspectFragment();
		}	
		if (md.stream().anyMatch( x -> x.getName().equals(CxEdgeBypass.ASPECT_NAME))) {
			writer.startAspectFragment(CxEdgeBypass.ASPECT_NAME);
			try (AspectIterator<CxEdgeBypass> it = new AspectIterator<>(
					pathPrefix + netId + "/aspects_cx2/" + CxEdgeBypass.ASPECT_NAME, CxEdgeBypass.class)) {
				while (it.hasNext()) {
					CxEdgeBypass elmt = it.next();
					if ( edgeIds.contains(elmt.getId()) ){
						writer.writeElementInFragment(elmt);
					}
				}
			}
			writer.endAspectFragment();
		}	

		if ( md.stream().anyMatch(x -> x.getName().equals(VisualEditorProperties.ASPECT_NAME))) {
			writer.writeAspectFromAspectFile(VisualEditorProperties.ASPECT_NAME, 
					pathPrefix + netId + "/aspects_cx2/" + VisualEditorProperties.ASPECT_NAME);
			
		}
	
		
	
	}

/*	
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
*/
}
