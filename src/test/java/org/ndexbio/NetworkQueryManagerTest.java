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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
		System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n");
		
		System.out.println("Path prefix: " + file.getAbsolutePath() + File.separator);
		System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n");
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
		
		// TODO verify we got correct result
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
		//Map<Long, NodesElement> nodes = net.getNodes();
		//assertEquals(5, nodes.size());
		//assertEquals("B", nodes.get(157).getNodeName());
		//assertEquals("C", nodes.get(163).getNodeName());
		//assertEquals("F", nodes.get(159).getNodeName());
		//assertEquals("E", nodes.get(155).getNodeName());
		//assertEquals("A", nodes.get(165).getNodeName());

		
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
		System.out.println(bos.toString());
		// TODO verify we got correct result
	}
	
	
}
