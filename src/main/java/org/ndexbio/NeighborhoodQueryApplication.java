package org.ndexbio;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

public class NeighborhoodQueryApplication extends Application {

	  public NeighborhoodQueryApplication() {}

	  @Override
	  public Set<Object> getSingletons() {
	    HashSet<Object> set = new HashSet<>();
	    set.add(new MessageResource());
	    return set;
	  }
}