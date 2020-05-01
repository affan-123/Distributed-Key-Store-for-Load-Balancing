package com.niket.DistributedSystem;

import com.niket.DistributedSystem.SocketServer;
import com.niket.DistributedSystem.FailureDetection.HeartBeatManager;
import com.niket.DistributedSystem.FailureDetection.HeartBeatServer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.niket.DistributedSystem.NetworkHandler;
import com.niket.DistributedSystem.Node;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CoordinatorServer {
	//static Coordinator coordinator = null;
	//static SocketServer server = null;
	
	/**
	 * @param args
	 * @throws IOException 
	 */

	public static List<Node> getNodeList() throws Exception{
		JSONParser parser = new JSONParser();
		
		Object obj = parser.parse(new FileReader("C:\\Users\\AFFAN SHAIKH\\eclipse-workspace\\Distributed-KeyStore--master\\src\\main\\java\\com\\niket\\DistributedSystem\\config.json"));
		JSONObject jsonObject = (JSONObject) obj;
		JSONArray nodeListObject = (JSONArray) jsonObject.get("nodes");
 
		Iterator<JSONObject> iterator = nodeListObject.iterator();
		List<Node> nodeList = new ArrayList<>();
		
		while(iterator.hasNext()){
			ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
            Node node = objectMapper.readValue(iterator.next().toJSONString(), Node.class);
			nodeList.add(node);
		}

		return nodeList;
	}

	public static void main(String[] args) throws Exception {
		

		

		//ExecutorService executor = Executors.newCachedThreadPool();
		System.out.println("Adding Servers:");
		Node coordinatorNode = new Node("127.0.0.1",Integer.parseInt(args[0]),Integer.toString((Integer.parseInt(args[0])%10)-1));
		Node serverNode = new Node("127.0.0.1",Integer.parseInt(args[1]),Integer.toString((Integer.parseInt(args[0])%10)-1));

		ConsistentHashingCoordinator consistentHashingCoordinator = ConsistentHashingCoordinator.getInstance(serverNode, 16, 3);

		ExecutionThreadPool pool = ExecutionThreadPool.getInstance();
		final HeartBeatServer heartBeatServer = new HeartBeatServer(serverNode);
		if (args[2].equals("yes")){
			pool.executor.submit(()->{
				heartBeatServer.listen();
			});
		}
		

		final HeartBeatManager manager = HeartBeatManager.getInstance(serverNode);
		
		List<Node> nodeList = getNodeList();

		for(Node node : nodeList){
			if(!node.equals(serverNode)){
				manager.addNode(node, true);
			}
		}

		if (args[2].equals("yes")){
			pool.executor.submit(()->{
				manager.start();
			});
		}
		
		
     
		for(Node node : nodeList){
			//if(!node.equals(serverNode)){
				consistentHashingCoordinator.addNode(node);
			//}
		}
	

		System.out.println("Binding Server:");
		KVServer kvServer = KVServer.getInstance(1,1,2);

		System.out.println("Starting Coordinator Server");
		NetworkHandler coordinatorhandler = new CoordinatorHandler(consistentHashingCoordinator, kvServer);
		final SocketServer coordinatorServer = new SocketServer(serverNode, coordinatorhandler);
		pool.addToQueue(()->{
			try{
				coordinatorServer.listen();
			} catch(Exception e){
				System.out.println("Exception in server "+ e);
			}
		});
	


		/*System.out.println("Starting Server");
        NetworkHandler kvClienthandler = new KVClientHandler(kvServer, consistentHashingCoordinator);
		final SocketServer server = new SocketServer(serverNode, kvClienthandler);
		pool.addToQueue(()->{
			try{
				server.listen();
			} catch(Exception e){
				System.out.println("Exception in server "+ e);
			}
			
		});*/
	}

}
