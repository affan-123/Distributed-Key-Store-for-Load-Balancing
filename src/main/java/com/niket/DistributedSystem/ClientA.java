
package com.niket.DistributedSystem;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import com.niket.DistributedSystem.KVClient;
import com.niket.DistributedSystem.Node;


public class ClientA {
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Node[] nodeList = new Node[]{	new Node("localhost",Integer.parseInt(args[0]),"0"),
									new Node("localhost",Integer.parseInt(args[1]),"1"),
									new Node("localhost",Integer.parseInt(args[2]),"2"),
									new Node("localhost",Integer.parseInt(args[3]),"3"),
									new Node("localhost",Integer.parseInt(args[4]),"4"),
									new Node("localhost",Integer.parseInt(args[5]),"5"),
									new Node("localhost",Integer.parseInt(args[2]),"6"),
									new Node("localhost",Integer.parseInt(args[3]),"7"),
									new Node("localhost",Integer.parseInt(args[4]),"8"),
									
									
	                            };
		KVClient kc = null;
		Random random = new Random();
		Node node = null;
		try{
			String key = "3";
			String value = "7";
			long before;
			long after;
			
			System.out.println("putting (3, 7)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			boolean status = kc.put(key, value);
			after = System.currentTimeMillis();
			System.out.println("Time required to put is : " + (after-before));
			System.out.println("status: " + status);

			System.out.println("putting (3, 8) (again)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.put(key, "8");
			after = System.currentTimeMillis();
			System.out.println("Time required to put is : " + (after-before));
			System.out.println("status: " + status);

			System.out.println("putting (8, 11) (again)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.put("8", "11");
			after = System.currentTimeMillis();
			System.out.println("Time required to put is : " + (after-before));
			System.out.println("status: " + status);

			System.out.println("putting (2, 3) (again)");
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			status = kc.put("2", "3");
			after = System.currentTimeMillis();
			System.out.println("Time required to put is : " + (after-before));
			System.out.println("status: " + status);
			
		
			System.out.println("getting key=3");			
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			value = kc.get(key);
			after = System.currentTimeMillis();
			System.out.println("Time required to get is : " + (after-before));			
			System.out.println("returned: " + value);

			System.out.println("getting key=8");			
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			value = kc.get("8");	
			after = System.currentTimeMillis();
			System.out.println("Time required to get is : " + (after-before));	
			System.out.println("returned: " + value);

			System.out.println("getting key=2");			
			node = nodeList[random.nextInt(nodeList.length)];
			System.out.println("Sending request to " + node.toString());
			kc = new KVClient(node);
			before = System.currentTimeMillis();
			value = kc.get("2");
			after = System.currentTimeMillis();
			System.out.println("Time required to get is : " + (after-before));
					
			System.out.println("returned: " + value);
			//before = System.currentTimeMillis();
			//kc.del(key);
			//after = System.currentTimeMillis();
			//System.out.println("Time required is : " + (after-before));
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
