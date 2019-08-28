package org.ndexbio;

import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;

import org.junit.Test;

public class MessageResourceTests {
	
	@Test
	public void CVSParsing() {
		String str = "A,Category,\"Agriculture, forestry and fishing\",";
		String r1 = MessageResource.convertCommaToSpace(str);
		assertEquals (r1 , "A Category \"Agriculture, forestry and fishing\" ");
	}

}
