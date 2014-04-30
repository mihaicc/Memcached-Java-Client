package com.meetup.memcached;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

/** Usage: Singleton class, to get the instance, access self statically.
 *  Description: Kestrel keeps a list with kestrel servers and their weight.
    Data is distributed based on the weight, the servers with a small
    weight should have a small load and thus crash later.
 */
class Kestrel {
	// make these thread-safe
	static Integer[] weights;
	public static MemcachedClient mcc = new MemcachedClient();
	static SockIOPool pool;
	// set up connection pool once at class load
	static {

		// grab an instance of our connection pool
		pool = SockIOPool.getInstance();

		// set the servers and the weights
		pool.setServers( new String[]{
				"ubvu-holy1.ubervu.local:22133",
				"ubvu-holy2.ubervu.local:22133",
				"ubvu-holy3.ubervu.local:22133"});
		weights = new Integer[]{300,300,300};
		pool.setWeights( weights );

		// set some basic pool settings
		// 5 initial, 5 min, and 250 max conns
		// and set the max idle time for a conn
		// to 6 hours
		pool.setInitConn( 5 );
		pool.setMinConn( 5 );
		pool.setMaxConn( 250 );
		pool.setMaxIdle( 1000 * 60 * 60 * 6 );

		// set the sleep for the maint thread
		// it will wake up every x seconds and
		// maintain the pool size
		pool.setMaintSleep( 30 );

		// set some TCP settings
		// disable nagle
		// set the read timeout to 3 secs
		// and don’t set a connect timeout
		pool.setNagle( false );
		pool.setSocketTO( 3000 );
		pool.setSocketConnectTO( 0 );

		mcc.setPrimitiveAsString( true );

		// don’t url encode keys
		// by default the java client url encodes keys
		// to sanitize them so they will always work on the server
		// however, other clients do not do this
		mcc.setSanitizeKeys( false );
		
		// initialize the connection pool
		pool.initialize();

	}
	static Kestrel self = new Kestrel();

	/**Setting items with a lifetime of 2^31 seconds
	 * @throws Exception */
	public boolean set(String queue,String item){
		return mcc.set(queue, item);
	}

	
	public Object get(String queue){
		Object toReturn = null;
		
		//only after getting 3(number of servers) consecutive null GETs 
		//should we say that this queue has no items
		for(int i = 0;i<weights.length;i++)
			if((toReturn = mcc.get(queue))!=null)
				break;

		return toReturn;
	}
	public void Shutdown(){
		pool.shutDown();
	}
}


public class Main {
	static void stats(String queue){
		@SuppressWarnings("unchecked")
		Map<String,Map<String,Integer>> stats = Kestrel.self.mcc.stats();
		Iterator<Entry<String, Map<String, Integer>>> it = stats.entrySet().iterator();
		while(it.hasNext()){
			Entry<String,Map<String,Integer>> cu = it.next();
			System.out.print("\n"+cu.getKey()+" - "+cu.getValue().get("queue_"+queue+"_items"));
		}
		System.out.println("");
	}
	public static void main(String[] args) throws Exception {
		Kestrel client = Kestrel.self;
		String queue = "lucene-parked";
		stats(queue);
		for(int i=0;i<20;i++)client.set(queue,"something");
		stats(queue);
		Kestrel.mcc.flush("lucene-parked");
		stats(queue);
	}
}
