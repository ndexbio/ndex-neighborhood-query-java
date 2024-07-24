package org.ndexbio;

import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.ndexbio.model.exceptions.BadRequestException;
import org.ndexbio.model.exceptions.NdexException;

/**
 *
 * @author churas
 */
public class SingleNetworkSolrIdxManagerTest {
	@BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
	
	@Test
	public void testConstructor(){
		SingleNetworkSolrIdxManager manager = new SingleNetworkSolrIdxManager("foo");
		assertEquals(manager.getClient().getDefaultCollection(), "foo");
		assertEquals(manager.getClient().getBaseURL(), SingleNetworkSolrIdxManager.DEFAULT_SOLR_URL);
		// try close
		manager.close();
	}
	
	@Test
	public void testConvertException400codeNotMatchingMessage(){
		RemoteSolrException e = new RemoteSolrException(SingleNetworkSolrIdxManager.DEFAULT_SOLR_URL,400, "some error", null);
		NdexException res = SingleNetworkSolrIdxManager.convertException(e, "ff");
		assertTrue(res instanceof BadRequestException);
		assertTrue(res.getMessage().contains("Error from server at"));
		
	}
	
	@Test
	public void testConvertException400code(){
		RemoteSolrException e = new RemoteSolrException(SingleNetworkSolrIdxManager.DEFAULT_SOLR_URL,400, 
				"uhoh", null);
		NdexException res = SingleNetworkSolrIdxManager.convertException(e, "solr");
		assertTrue(res instanceof BadRequestException);
		assertEquals("uhoh", res.getMessage());

	}
	
	@Test
	public void testConvertExceptionNon400code(){
		RemoteSolrException e = new RemoteSolrException(SingleNetworkSolrIdxManager.DEFAULT_SOLR_URL,500, 
				"yikes", null);
		NdexException res = SingleNetworkSolrIdxManager.convertException(e, "solr");
		assertFalse(res instanceof BadRequestException);
		assertTrue(res.getMessage().contains("Error from server at"));
	}
	
}
