package com.niket.DistributedSystem;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.niket.DistributedSystem.NetworkHandler;
import com.niket.DistributedSystem.ThreadPool;
import com.niket.DistributedSystem.KVServer;
import com.niket.DistributedSystem.KVMessage;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;


/**
 * This NetworkHandler will asynchronously handle the socket connections. 
 * It uses a threadpool to ensure that none of it's methods are blocking.
 *
 */
public class KVClientHandler implements NetworkHandler {
	private KVServer kvServer = null;
	private ThreadPool threadpool = null;
	private ConsistentHashingCoordinator consistentHashingCoordinator;
	
	public KVClientHandler(KVServer kvServer, ConsistentHashingCoordinator consistentHashingCoordinator) {
		this.kvServer = kvServer;
		this.consistentHashingCoordinator = consistentHashingCoordinator;
        this.threadpool = ExecutionThreadPool.getInstance();
	}

	public KVClientHandler(KVServer kvServer, ConsistentHashingCoordinator consistentHashingCoordinator, int poolSize) {
		this.kvServer = kvServer;
		this.consistentHashingCoordinator = consistentHashingCoordinator;
        this.threadpool = ExecutionThreadPool.getInstance(poolSize);
	}
	
	@Override
	public void handle(Socket client) throws IOException {
		Runnable task = new ClientHandler(kvServer,consistentHashingCoordinator, client);
		try {
			threadpool.addToQueue(task);
		} catch (InterruptedException e) {
			// Ignore this error
			return;
		}
	}


    private class ClientHandler implements Runnable {
		private KVServer kvServer = null;
		private Socket client = null;
		private ExecutionThreadPool executionThreadPool = ExecutionThreadPool.getInstance();
		private ConsistentHashingCoordinator consistentHashingCoordinator;
		@Override
		public void run() {
		     // TODO: Implement Me!
			 BufferedReader in = null;
			 PrintWriter out = null;
			 int replicaCount = kvServer.getReplicaCount();

			 try{
				 System.out.println("Inside KVCLientHandleer");
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);

				String json = in.readLine();
				KVMessage message = KVMessage.toKVMessage(json);
				Boolean status;
				final String key = message.getKey();
				final String value = message.getValue();
				switch(message.getMsgType()){

					case "GETR": 	String result = kvServer.get(message.getKey());
									System.out.println("KVServer handling request get with key " + message.getKey() );

									if(result != null){
										message.setValue(result);
										message.setStatus("1");
									} else{
										message.setStatus("0");
									}
									break;
					case "PUTR": 	status = kvServer.put(message.getKey(),message.getValue());
									System.out.println("KVServer handling request put with key " + message.getKey() + "and value "+message.getValue());
									message.setStatus( status?"1":"0");
									
									break;
					case "DELETER":	status = kvServer.delete(message.getKey());
									System.out.println("KVServer handling request delete with key " + message.getKey() );
									message.setStatus( status?"1":"0");
									break;
					
					case "GET": 	
									int readCount = kvServer.getReadCount();
									List<Node> readNodeList = consistentHashingCoordinator.getPreferenceList(message.getKey(),readCount);
									List<Callable<String>> readCallableTasks = new ArrayList<>();
									Callable<String> readCallableTask = null;
									for(int i=1;i<readCount;i++){
										final Node readNode = readNodeList.get(i);
										readCallableTask = ()->{
											KVClient client = new KVClient(readNode);	
											return client.get(key,true);
										};
										readCallableTasks.add(readCallableTask);
									}

									readCallableTask = ()->{
										System.out.println("KVServer handling request get with key " + key );
										return kvServer.get(key);
									};
									readCallableTasks.add(readCallableTask);

									List<Future<String>> readFutures = executionThreadPool.executor.invokeAll(readCallableTasks);
									
									List<String> readResultList= new ArrayList<>();

									for(Future<String> future : readFutures){
										readResultList.add(future.get());
									}
									if(readResultList.size() == readCount){
										message.setValue(readResultList.toString());
										message.setStatus("1");
									} else{
										message.setStatus("0");
									}
									break;

					case "PUT": 	
									int writeCount = kvServer.getWriteCount();
									List<Node> writeNodeList = consistentHashingCoordinator.getPreferenceList(message.getKey(),replicaCount);
									List<Callable<Boolean>> writeCallableTasks = new ArrayList<>();
									Callable<Boolean> writeCallableTask = null;
									for(int i=1;i<writeCount;i++){
										final Node writeNode = writeNodeList.get(i);
										writeCallableTask = ()->{
											KVClient client = new KVClient(writeNode);
											return client.put(key,value,true);
										};
										writeCallableTasks.add(writeCallableTask);
									}
								
									writeCallableTask = ()->{
										System.out.println("KVServer handling request put with key " + key + " value "+ value );
										return kvServer.put(key, value);
									};
									writeCallableTasks.add(writeCallableTask);
								
									List<Future<Boolean>> writeFutures = executionThreadPool.executor.invokeAll(writeCallableTasks);

									List<Boolean> writeResultList= new ArrayList<>();
								
									for(Future<Boolean> future : writeFutures){
										writeResultList.add(future.get());
									}
									message.setStatus(writeResultList.toString());
									
									writeCallableTasks = new ArrayList<>();
									writeCallableTask = null;
									for(int i=writeCount;i<replicaCount;i++){
										final Node np = writeNodeList.get(i);		
										writeCallableTask = ()->{
											KVClient client = new KVClient(np);
											return client.put(key,value,true);
										};
										writeCallableTasks.add(writeCallableTask);
									}
		
								
									executionThreadPool.executor.invokeAll(writeCallableTasks);

									break;
					case "DELETE":	
									int deleteCount = kvServer.getWriteCount();
									List<Node> deleteNodeList = consistentHashingCoordinator.getPreferenceList(message.getKey(),deleteCount);
									List<Callable<Boolean>> deleteCallableTasks = new ArrayList<>();
									Callable<Boolean> deleteCallableTask = null;
									for(int i=1;i<deleteCount;i++){
										final Node deleteNode = deleteNodeList.get(i);		
										deleteCallableTask = ()->{
											KVClient client = new KVClient(deleteNode);
											return client.put(key,value,true);
										};
										deleteCallableTasks.add(deleteCallableTask);
									}
								
									deleteCallableTask = ()->{
										System.out.println("KVServer handling request delete with key " + key + " value "+ value );
										return kvServer.delete(key);
									};
									deleteCallableTasks.add(deleteCallableTask);
								
									List<Future<Boolean>> deleteFutures = executionThreadPool.executor.invokeAll(deleteCallableTasks);

									List<Boolean> deleteResultList= new ArrayList<>();
								
									for(Future<Boolean> future : deleteFutures){
										deleteResultList.add(future.get());
									}
									message.setStatus(deleteResultList.toString());
							
									break;
				}
				json = message.toJson();
				out.println(json);

			} catch(Exception e){
				System.out.println("Exception while handling request "+e);
			} finally{
				try{
					in.close();
					out.close();
					client.close();
				} catch(Exception exp){
					System.out.println("Exception while closing streams and socket"+exp);
				}
				
			}
			 
		}
		
		public ClientHandler(KVServer kvServer, ConsistentHashingCoordinator consistentHashingCoordinator, Socket client) {
			this.kvServer = kvServer;
			this.client = client;
			this.consistentHashingCoordinator = consistentHashingCoordinator;
		}
	}
	
}
