package org.ndexbio;

import java.io.IOException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.HttpSolrClientBase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.easymock.Capture;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
	
	//test getNodeIdsByQuery limit is 0
	@Test
	public void testGetNodeIdsByQueryLimitIs0() throws Exception {
		SolrDocumentList sdList = new SolrDocumentList();
		sdList.setNumFound(5);
		
		Capture<SolrQuery> capturedSolrQuery = EasyMock.newCapture();
		
		QueryResponse mockResponse = mock(QueryResponse.class);
		expect(mockResponse.getResults()).andReturn(sdList);
		HttpSolrClientBase mockClient = mock(HttpSolrClientBase.class);
		expect(mockClient.getDefaultCollection()).andReturn("someid");
		expect(mockClient.query(capture(capturedSolrQuery), eq(SolrRequest.METHOD.POST))).andReturn(mockResponse);
		
		replay(mockResponse);
		replay(mockClient);
		SingleNetworkSolrIdxManager sManager = new SingleNetworkSolrIdxManager(mockClient);
		SolrDocumentList res = sManager.getNodeIdsByQuery("myquery", 0);
		assertEquals(5, res.getNumFound());
		
		verify(mockResponse);
		verify(mockClient);
		assertEquals((int)30000000, (int)capturedSolrQuery.getValue().getRows());
	}
	
	//test getNodeIdsByQuery limit is 100
	@Test
	public void testGetNodeIdsByQueryLimitIs100() throws Exception {
		SolrDocumentList sdList = new SolrDocumentList();
		sdList.setNumFound(5);
		
		Capture<SolrQuery> capturedSolrQuery = EasyMock.newCapture();
		
		QueryResponse mockResponse = mock(QueryResponse.class);
		expect(mockResponse.getResults()).andReturn(sdList);
		HttpSolrClientBase mockClient = mock(HttpSolrClientBase.class);
		expect(mockClient.getDefaultCollection()).andReturn("someid");
		expect(mockClient.query(capture(capturedSolrQuery), eq(SolrRequest.METHOD.POST))).andReturn(mockResponse);
		
		replay(mockResponse);
		replay(mockClient);
		SingleNetworkSolrIdxManager sManager = new SingleNetworkSolrIdxManager(mockClient);
		SolrDocumentList res = sManager.getNodeIdsByQuery("myquery", 100);
		assertEquals(5, res.getNumFound());
		
		verify(mockResponse);
		verify(mockClient);
		assertEquals((int)100, (int)capturedSolrQuery.getValue().getRows());
	}
	
	//test getNodeIdsByQuery raises exception
	@Test
	public void testGetNodeIdsByQueryRaisesException() throws Exception {
		SolrDocumentList sdList = new SolrDocumentList();
		sdList.setNumFound(5);
		
		Capture<SolrQuery> capturedSolrQuery = EasyMock.newCapture();
		
		HttpSolrClientBase mockClient = mock(HttpSolrClientBase.class);
		expect(mockClient.getDefaultCollection()).andReturn("someid");
		RemoteExecutionException rse = new RemoteExecutionException("endpoint", 500, "uhoh", new NamedList<String>());
		
		expect(mockClient.query(capture(capturedSolrQuery), eq(SolrRequest.METHOD.POST))).andThrow(rse);
		
		replay(mockClient);
		SingleNetworkSolrIdxManager sManager = new SingleNetworkSolrIdxManager(mockClient);
		try {
			sManager.getNodeIdsByQuery("myquery", 100);
			fail("Expected exception");
		} catch(NdexException nde){
			assertEquals("Error from NDEx Solr server: Error from server at endpoint: uhoh: {}", nde.getMessage());
		}
		
		verify(mockClient);
	}
	
	@Test
	public void testCloseRaisesException() throws IOException {
		HttpSolrClientBase mockClient = mock(HttpSolrClientBase.class);
		expect(mockClient.getDefaultCollection()).andReturn("someid");
		mockClient.close();
		expectLastCall().andThrow(new IOException("error"));
		replay(mockClient);
		SingleNetworkSolrIdxManager sManager = new SingleNetworkSolrIdxManager(mockClient);
		sManager.close();
		verify(mockClient);
		
	}
	
}
