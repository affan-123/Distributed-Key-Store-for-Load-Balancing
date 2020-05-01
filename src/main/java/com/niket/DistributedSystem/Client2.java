
package com.niket.DistributedSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.niket.DistributedSystem.KVClient;
import com.niket.DistributedSystem.Node;
import com.niket.DistributedSystem.FailureDetection.HeartBeatManager;
import com.niket.DistributedSystem.FailureDetection.HeartBeatServer;

import org.json.JSONArray;
import org.json.JSONObject;

public class Client2 {


	public static List<Node> getNodeList(String json) throws Exception{
		
		JSONArray nodeListObject = new JSONArray(json);
 
		//Iterator<JSONObject> iterator = nodeListObject.iterator();
		List<Node> nodeList = new ArrayList<>();
		
		for(int i=0;i<nodeListObject.length();i++){
			ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
            Node node = objectMapper.readValue( nodeListObject.getJSONObject(i).toString() , Node.class);
			nodeList.add(node);
		}

		return nodeList;
	}


	/**
	 * @param args
	 * @throws IOException
	 * 
	 */
	public static void main(String[] args) throws IOException {
		Node[] nodeList = new Node[] { new Node("localhost", Integer.parseInt(args[0]), "0"),
				new Node("localhost", Integer.parseInt(args[1]), "0")};
				//new Node("localhost", Integer.parseInt(args[2]), "0") };
		KVClient kc = null;
		Random random = new Random();
		Node node = null;
		try {
			String three = "3";
			String seven = "7";
			long before;
			long after;
			Boolean status;

			

			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			String result= kc.getRoutingInfo();
			//KVMessage msg = KVMessage.toKVMessage(result);
			List<Node> nodeListServer = getNodeList(result);
			System.out.println("Node list is "+nodeListServer.toString());
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before - after));
			System.out.println("status: " + result);

			System.out.println("Adding Servers:");
			Node coordinatorNode = new Node("127.0.0.1", Integer.parseInt(args[3]),
					Integer.toString((Integer.parseInt(args[3]) % 10) ));
			Node serverNode = new Node("127.0.0.1", Integer.parseInt(args[3]),
					Integer.toString((Integer.parseInt(args[3]) % 10)));

			ConsistentHashingCoordinator consistentHashingCoordinator = ConsistentHashingCoordinator
					.getInstance(serverNode, 16, 3);

			ExecutionThreadPool pool = ExecutionThreadPool.getInstance();
			final HeartBeatServer heartBeatServer = new HeartBeatServer(serverNode);
			if (args[4].equals("yes")){
				pool.executor.submit(()->{
					heartBeatServer.listen();
				});
			}
		

			final HeartBeatManager manager = HeartBeatManager.getInstance(serverNode);

			//List<Node> nodeListServer = getNodeList();

			for(Node inode : nodeListServer){
				if(!inode.equals(serverNode)){
					manager.addNode(inode, true);
				}
			}

			if (args[4].equals("yes")){
				pool.executor.submit(()->{
					manager.start();
				});
			}
		

		
			for(Node inode : nodeListServer){
				//if(!node.equals(serverNode)){
					consistentHashingCoordinator.addNode(inode);
				//}
			}
		

			System.out.println("Binding Server:");
			KVServer kvServer = KVServer.getInstance(2,2,2);

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

			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.connectToCoordinator(new Node("127.0.0.1", 8084, "4"));
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before - after));
			System.out.println("status: " + status);
	
			
			
			
			
			
			
			
			
			
			/*System.out.println("putting (3, 7)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			boolean status = kc.put(three, seven);
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));
			System.out.println("status: " + status);

			System.out.println("putting (3, 8) (again)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.put(three, "8");
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));
			System.out.println("status: " + status);

			System.out.println("putting (8, 11) (again)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.put("8", "11");
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));
			System.out.println("status: " + status);

			System.out.println("putting (2, 3) (again)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.put("2", "3");
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));
			System.out.println("status: " + status);
			
			String value;
			System.out.println("getting key=3");			
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			value = kc.get(three);
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));			
			System.out.println("returned: " + value);

			System.out.println("getting key=8");			
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			value = kc.get("8");	
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));	
			System.out.println("returned: " + value);

			System.out.println("getting key=2");			
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			value = kc.get("2");
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));
					
			System.out.println("returned: " + value);
			before = System.currentTimeMillis();
			kc.del(three);
			after = System.currentTimeMillis();
			System.out.println("Time required is : " + (before-after));
            */

			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
