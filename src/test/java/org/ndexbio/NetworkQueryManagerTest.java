package org.ndexbio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.ndexbio.cx2.aspect.element.core.Cx2Network;
import org.ndexbio.cx2.aspect.element.core.CxEdge;
import org.ndexbio.cx2.aspect.element.core.CxNode;
import org.ndexbio.cxio.aspects.datamodels.EdgesElement;
import org.ndexbio.cxio.aspects.datamodels.NetworkAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.NodesElement;
import org.ndexbio.cxio.core.readers.NiceCXNetworkReader;
import org.ndexbio.model.cx.NiceCXNetwork;
import org.ndexbio.model.exceptions.NdexException;
import org.ndexbio.model.object.SimplePathQuery;

/**
 *
 * @author churas
 */
public class NetworkQueryManagerTest {
	
	public static String ORIG_PATH_PREFIX = "/opt/ndex/data/";
	public static UUID NETWORK_ID = UUID.fromString("5d186918-4dfd-11ef-a7fd-005056ae23aa");
    
    @BeforeAll
    public static void setUp() {
		File file = new File("src" + File.separator + "test" + File.separator + "resources");
		NetworkQueryManager.setDataFilePathPrefix(file.getAbsolutePath() + File.separator);
    }
    
    @AfterAll
    public static void tearDown() {
		NetworkQueryManager.setDataFilePathPrefix(ORIG_PATH_PREFIX);
    }
	
	@Test
	public void testNeighbourHoodQueryOnB() throws IOException, NdexException {
		SimplePathQuery pathQuery = new SimplePathQuery();
		pathQuery.setSearchDepth(1);
		pathQuery.setSearchString("B");

		NetworkQueryManager query = new NetworkQueryManager(NETWORK_ID, pathQuery);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		HashSet<Long> nodeIds = new HashSet<>();
		nodeIds.add(157L);
		query.neighbourhoodQuery(bos, nodeIds);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		NiceCXNetworkReader reader = new NiceCXNetworkReader();
		NiceCXNetwork net = reader.readNiceCXNetwork(bis);
		
		HashMap<String, String> netAttrs = new HashMap<>();
		for (NetworkAttributesElement nae : net.getNetworkAttributes()){
			netAttrs.put(nae.getName(), nae.getValue());
		}
		assertEquals("Neighborhood  query result on network - NetworkQueryModes", netAttrs.get("name"));
		assertEquals((String)"NDEx Neighborhood Query/v1.1 (Depth=1; Query terms=\"B\")",
				(String)netAttrs.get("prov:wasGeneratedBy"));
		Map<Long, NodesElement> nodes = net.getNodes();
		assertEquals(5, nodes.size());

		assertEquals("B", nodes.get(157L).getNodeName());
		assertEquals("C", nodes.get(163L).getNodeName());
		assertEquals("F", nodes.get(159L).getNodeName());
		assertEquals("E", nodes.get(155L).getNodeName());
		assertEquals("A", nodes.get(165L).getNodeName());
		
		Map<Long, EdgesElement> edges = net.getEdges();
		assertEquals(7, edges.size());
		assertEquals(edges.get(179L).getSource(), (Long)163L);
		assertEquals(edges.get(179L).getTarget(), (Long)165L);
		assertEquals(edges.get(179L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(171L).getSource(), (Long)157L);
		assertEquals(edges.get(171L).getTarget(), (Long)165L);
		assertEquals(edges.get(171L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(181L).getSource(), (Long)165L);
		assertEquals(edges.get(181L).getTarget(), (Long)159L);
		assertEquals(edges.get(181L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(175L).getSource(), (Long)163L);
		assertEquals(edges.get(175L).getTarget(), (Long)159L);
		assertEquals(edges.get(175L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(173L).getSource(), (Long)163L);
		assertEquals(edges.get(173L).getTarget(), (Long)157L);
		assertEquals(edges.get(173L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(169L).getSource(), (Long)157L);
		assertEquals(edges.get(169L).getTarget(), (Long)159L);
		assertEquals(edges.get(169L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(167L).getSource(), (Long)157L);
		assertEquals(edges.get(167L).getTarget(), (Long)155L);
		assertEquals(edges.get(167L).getInteraction(), "interacts with");
	}
	
	@Test
	public void testNeighbourHoodQueryCX2OnB() throws IOException, NdexException {
		SimplePathQuery pathQuery = new SimplePathQuery();
		pathQuery.setSearchDepth(1);
		pathQuery.setSearchString("B");

		NetworkQueryManager query = new NetworkQueryManager(NETWORK_ID, pathQuery);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		HashSet<Long> nodeIds = new HashSet<>();
		nodeIds.add(157L);
		query.neighbourhoodQueryCX2(bos, nodeIds, true, true);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		Cx2Network net = new Cx2Network(bis);
		
		assertEquals("NDEx Neighborhood Query/v1.1 (Depth=1; Query terms=\"B\")",
				net.getNetworkAttributes().getElementObject().get("prov:wasGeneratedBy"));
		assertEquals("Neighborhood query on network - NetworkQueryModes",
				net.getNetworkAttributes().getElementObject().get("name"));
		
		Map<Long, CxNode> nodes = net.getNodes();
		assertEquals(5, nodes.size());
		
		assertEquals("A", nodes.get(165L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals(nodes.get(165L).getX(), -235.0, 0.01);
		assertEquals(nodes.get(165L).getY(), -83.0, 0.01);
		assertNull(nodes.get(165L).getZ());
		
		assertEquals("B", nodes.get(157L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals("C", nodes.get(163L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals("E", nodes.get(155L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals("F", nodes.get(159L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		
		Map<Long, CxEdge> edges = net.getEdges();
		assertEquals(7, edges.size());
		assertEquals(edges.get(179L).getSource(), (Long)163L);
		assertEquals(edges.get(179L).getTarget(), (Long)165L);
		assertEquals(edges.get(179L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(171L).getSource(), (Long)157L);
		assertEquals(edges.get(171L).getTarget(), (Long)165L);
		assertEquals(edges.get(171L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(181L).getSource(), (Long)165L);
		assertEquals(edges.get(181L).getTarget(), (Long)159L);
		assertEquals(edges.get(181L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(175L).getSource(), (Long)163L);
		assertEquals(edges.get(175L).getTarget(), (Long)159L);
		assertEquals(edges.get(175L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(173L).getSource(), (Long)163L);
		assertEquals(edges.get(173L).getTarget(), (Long)157L);
		assertEquals(edges.get(173L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(169L).getSource(), (Long)157L);
		assertEquals(edges.get(169L).getTarget(), (Long)159L);
		assertEquals(edges.get(169L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(167L).getSource(), (Long)157L);
		assertEquals(edges.get(167L).getTarget(), (Long)155L);
		assertEquals(edges.get(167L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
	}
	
	@Test
	public void testInterConnectQueryOnAandBDepth1() throws IOException, NdexException {
		SimplePathQuery pathQuery = new SimplePathQuery();
		pathQuery.setSearchDepth(1);
		pathQuery.setSearchString("A B");

		NetworkQueryManager query = new NetworkQueryManager(NETWORK_ID, pathQuery);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		HashSet<Long> nodeIds = new HashSet<>();
		nodeIds.add(165L);
		nodeIds.add(157L);
		query.interConnectQuery(bos, nodeIds);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		NiceCXNetworkReader reader = new NiceCXNetworkReader();
		NiceCXNetwork net = reader.readNiceCXNetwork(bis);
		
		HashMap<String, String> netAttrs = new HashMap<>();
		for (NetworkAttributesElement nae : net.getNetworkAttributes()){
			netAttrs.put(nae.getName(), nae.getValue());
		}
		assertEquals("Direct query result on network - NetworkQueryModes", netAttrs.get("name"));
		assertEquals((String)"NDEx Direct Query/v1.1 (Query terms=\"A B\")",
				(String)netAttrs.get("prov:wasGeneratedBy"));
		Map<Long, NodesElement> nodes = net.getNodes();
		assertEquals(2, nodes.size());

		assertEquals("B", nodes.get(157L).getNodeName());

		assertEquals("A", nodes.get(165L).getNodeName());
		
		Map<Long, EdgesElement> edges = net.getEdges();
		assertEquals(1, edges.size());
		
		assertEquals(edges.get(171L).getSource(), (Long)157L);
		assertEquals(edges.get(171L).getTarget(), (Long)165L);
		assertEquals(edges.get(171L).getInteraction(), "interacts with");
	}
	
	@Test
	public void testinterConnectQueryCX2OnAandBdepth1() throws IOException, NdexException {
		SimplePathQuery pathQuery = new SimplePathQuery();
		pathQuery.setSearchDepth(1);
		pathQuery.setSearchString("A B");

		NetworkQueryManager query = new NetworkQueryManager(NETWORK_ID, pathQuery);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		HashSet<Long> nodeIds = new HashSet<>();
		nodeIds.add(165L);
		nodeIds.add(157L);
		query.interConnectQueryCX2(bos, nodeIds, true);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		Cx2Network net = new Cx2Network(bis);
		
		assertEquals("NDEx Direct Query/v1.1 (Depth=1; Query terms=\"A B\")",
				net.getNetworkAttributes().getElementObject().get("prov:wasGeneratedBy"));
		assertEquals("Direct query on network - NetworkQueryModes",
				net.getNetworkAttributes().getElementObject().get("name"));
		
		Map<Long, CxNode> nodes = net.getNodes();
		assertEquals(2, nodes.size());
		
		assertEquals("A", nodes.get(165L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals(nodes.get(165L).getX(), -235.0, 0.01);
		assertEquals(nodes.get(165L).getY(), -83.0, 0.01);
		assertNull(nodes.get(165L).getZ());
		
		assertEquals("B", nodes.get(157L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		
		Map<Long, CxEdge> edges = net.getEdges();
		assertEquals(1, edges.size());
		
		assertEquals(edges.get(171L).getSource(), (Long)157L);
		assertEquals(edges.get(171L).getTarget(), (Long)165L);
		assertEquals(edges.get(171L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
	}
	
	
	@Test
	public void testInterConnectQueryOnAandBDepth2() throws IOException, NdexException {
		SimplePathQuery pathQuery = new SimplePathQuery();
		pathQuery.setSearchDepth(2);
		pathQuery.setSearchString("A B");

		NetworkQueryManager query = new NetworkQueryManager(NETWORK_ID, pathQuery);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		HashSet<Long> nodeIds = new HashSet<>();
		nodeIds.add(165L);
		nodeIds.add(157L);
		query.interConnectQuery(bos, nodeIds);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		NiceCXNetworkReader reader = new NiceCXNetworkReader();
		NiceCXNetwork net = reader.readNiceCXNetwork(bis);
		
		HashMap<String, String> netAttrs = new HashMap<>();
		for (NetworkAttributesElement nae : net.getNetworkAttributes()){
			netAttrs.put(nae.getName(), nae.getValue());
		}
		assertEquals("Interconnect query result on network - NetworkQueryModes", netAttrs.get("name"));
		assertEquals((String)"NDEx Interconnect Query/v1.1 (Query=\"A B\")",
				(String)netAttrs.get("prov:wasGeneratedBy"));
		Map<Long, NodesElement> nodes = net.getNodes();
		assertEquals(4, nodes.size());

		assertEquals("B", nodes.get(157L).getNodeName());
		assertEquals("C", nodes.get(163L).getNodeName());
		assertEquals("F", nodes.get(159L).getNodeName());
		assertEquals("A", nodes.get(165L).getNodeName());
		
		Map<Long, EdgesElement> edges = net.getEdges();
		assertEquals(6, edges.size());
		assertEquals(edges.get(179L).getSource(), (Long)163L);
		assertEquals(edges.get(179L).getTarget(), (Long)165L);
		assertEquals(edges.get(179L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(171L).getSource(), (Long)157L);
		assertEquals(edges.get(171L).getTarget(), (Long)165L);
		assertEquals(edges.get(171L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(181L).getSource(), (Long)165L);
		assertEquals(edges.get(181L).getTarget(), (Long)159L);
		assertEquals(edges.get(181L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(175L).getSource(), (Long)163L);
		assertEquals(edges.get(175L).getTarget(), (Long)159L);
		assertEquals(edges.get(175L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(173L).getSource(), (Long)163L);
		assertEquals(edges.get(173L).getTarget(), (Long)157L);
		assertEquals(edges.get(173L).getInteraction(), "interacts with");
		
		assertEquals(edges.get(169L).getSource(), (Long)157L);
		assertEquals(edges.get(169L).getTarget(), (Long)159L);
		assertEquals(edges.get(169L).getInteraction(), "interacts with");
		
	}
	
	@Test
	public void testinterConnectQueryCX2OnAandBdepth2() throws IOException, NdexException {
		SimplePathQuery pathQuery = new SimplePathQuery();
		pathQuery.setSearchDepth(2);
		pathQuery.setSearchString("A B");

		NetworkQueryManager query = new NetworkQueryManager(NETWORK_ID, pathQuery);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		HashSet<Long> nodeIds = new HashSet<>();
		nodeIds.add(165L);
		nodeIds.add(157L);
		query.interConnectQueryCX2(bos, nodeIds, true);
		
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		Cx2Network net = new Cx2Network(bis);
		
		assertEquals("NDEx Interconnect Query/v1.1 (Depth=2; Query terms=\"A B\")",
				net.getNetworkAttributes().getElementObject().get("prov:wasGeneratedBy"));
		assertEquals("Interconnect query on network - NetworkQueryModes",
				net.getNetworkAttributes().getElementObject().get("name"));
		
		Map<Long, CxNode> nodes = net.getNodes();
		assertEquals(4, nodes.size());
		
		assertEquals("A", nodes.get(165L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals(nodes.get(165L).getX(), -235.0, 0.01);
		assertEquals(nodes.get(165L).getY(), -83.0, 0.01);
		assertNull(nodes.get(165L).getZ());
		
		assertEquals("B", nodes.get(157L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals("C", nodes.get(163L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		assertEquals("F", nodes.get(159L).getNodeName(net.getAttributeDeclarations().getAttributesInAspect(CxNode.ASPECT_NAME)));
		
		Map<Long, CxEdge> edges = net.getEdges();
		assertEquals(6, edges.size());
		assertEquals(edges.get(179L).getSource(), (Long)163L);
		assertEquals(edges.get(179L).getTarget(), (Long)165L);
		assertEquals(edges.get(179L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(171L).getSource(), (Long)157L);
		assertEquals(edges.get(171L).getTarget(), (Long)165L);
		assertEquals(edges.get(171L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(181L).getSource(), (Long)165L);
		assertEquals(edges.get(181L).getTarget(), (Long)159L);
		assertEquals(edges.get(181L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(175L).getSource(), (Long)163L);
		assertEquals(edges.get(175L).getTarget(), (Long)159L);
		assertEquals(edges.get(175L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(173L).getSource(), (Long)163L);
		assertEquals(edges.get(173L).getTarget(), (Long)157L);
		assertEquals(edges.get(173L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
		assertEquals(edges.get(169L).getSource(), (Long)157L);
		assertEquals(edges.get(169L).getTarget(), (Long)159L);
		assertEquals(edges.get(169L).getInteraction(net.getAttributeDeclarations().getAttributesInAspect(CxEdge.ASPECT_NAME)), "interacts with");
		
	}
	
}
