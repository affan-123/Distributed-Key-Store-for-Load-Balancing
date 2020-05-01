package com.niket.DistributedSystem;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.niket.DistributedSystem.NetworkHandler;
import com.niket.DistributedSystem.ThreadPool;
import com.niket.DistributedSystem.KVMessage;
import com.niket.DistributedSystem.KVClient;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;


/**
 * This NetworkHandler will asynchronously handle the socket connections. 
 * It uses a threadpool to ensure that none of it's methods are blocking.
 *
 */
public class CoordinatorHandler implements NetworkHandler {
	private ThreadPool threadpool = null;
	private ConsistentHashingCoordinator consistentHashingCoordinator;
	private KVServer kvServer;
	
	
	public CoordinatorHandler(ConsistentHashingCoordinator consistentHashingCoordinator,KVServer kvServer) {
		this.consistentHashingCoordinator = consistentHashingCoordinator;
		this.kvServer = kvServer;
		this.threadpool = ExecutionThreadPool.getInstance();
	}

	public CoordinatorHandler(ConsistentHashingCoordinator consistentHashingCoordinator,KVServer kvServer, int poolSize) {
		this.consistentHashingCoordinator = consistentHashingCoordinator;
		this.kvServer = kvServer;
        this.threadpool = ExecutionThreadPool.getInstance(poolSize);
	}
	
	@Override
	public void handle(Socket client) throws IOException {
		Runnable task = new ClientHandler(consistentHashingCoordinator, client, kvServer);
		try {
			threadpool.addToQueue(task);
		} catch (InterruptedException e) {
			// Ignore this error
			return;
		}
	}


    private class ClientHandler implements Runnable {
		private KVClient client = null;
		private Socket clientSocket = null;
		private ConsistentHashingCoordinator consistentHashingCoordinator = null;
		private KVServer kvServer = null;
		private ExecutionThreadPool executionThreadPool = ExecutionThreadPool.getInstance();
		
		public ClientHandler(ConsistentHashingCoordinator consistentHashingCoordinator, Socket client, KVServer kvServer) {
			this.clientSocket = client;
			this.consistentHashingCoordinator = consistentHashingCoordinator;
			this.kvServer = kvServer;
		}

		private void readRepair(List<String> resultList, String key){
			List<Node> nodeList = new ArrayList<>();
			List<String> valuesList = new ArrayList<>();

			for(String e : resultList){
				String[] vals = e.split(";");
				nodeList.add(Node.getObject(vals[1]));
				valuesList.add(vals[0]);
			}

			String result = getLastestResult(valuesList);

			for(int i = 0; i < nodeList.size(); i++ ){
				if(!valuesList.get(i).split("#")[0].equals(result)){
					final int index = i;
					executionThreadPool.executor.submit(()->{
						try{
							System.out.println("Repairing node "+nodeList.get(index)+" for value "+valuesList.get(index)+" and real value "+result);
							KVClient client = new KVClient(nodeList.get(index));
							client.put(key, result);
						} catch(Exception e){
							System.out.println("Exception while repairing");
						}
					});
					
				}
			}
		}

		private String getLastestResult(List<String> resultList){
			int maxVersion = -1;
			String result = "";
			for(String res : resultList){
				String[] vals = res.split("#");
				if(Integer.parseInt(vals[1]) > maxVersion){
					maxVersion = Integer.parseInt(vals[1]);
					result = vals[0];
				}
			}
			return result;
		}

		@Override
		public void run() {
		     // TODO: Implement Me!
			 System.out.println("Starting wow");
			 BufferedReader in = null;
			 PrintWriter out = null;
			 int replicaCount = kvServer.getReplicaCount();
			 try{
				in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				out = new PrintWriter(clientSocket.getOutputStream(),true);

				String json = in.readLine();
				KVMessage message = KVMessage.toKVMessage(json);
				Boolean status = false;

				List<Node> listnode = consistentHashingCoordinator.getPreferenceList(message.getKey(),replicaCount);
				if(listnode == null){
					System.out.println("Did not find node");
					message.setStatus("0");
				}
				else{
					System.out.println("handling request of type "+message.getMsgType()+ " with "+message.toJson());
					System.out.println("Succ List "+listnode);
					Node localNode = consistentHashingCoordinator.getLocaNode();
					//check for local
					final String key = message.getKey();
					final String value = message.getValue();

					if(message.getMsgType().equals("ROUTE")){
						System.out.println("Sending routing info lol");
						message.setValue( consistentHashingCoordinator.getNodeList().toString());

					}
					else if(message.getMsgType().equals("DELETEINRANGE")){

						String[] prms = message.getKey().split(":");
						Map<String,String> map = kvServer.getKeysInRange(Integer.parseInt(prms[0]) ,Integer.parseInt(prms[1]) , Integer.parseInt(prms[2]));
						List<Future<Boolean>> resultList = new ArrayList<>();
						if(map != null && map.size()>0){
							for(Map.Entry<String,String> entry : map.entrySet()){
								final String lkey = entry.getKey();
								System.out.println("Deleting "+map.toString()+ " from node "+localNode);

								Future<Boolean> res = executionThreadPool.executor.submit(()->{
									try{
										return kvServer.delete(lkey);
									} catch(Exception e){
										System.out.println("Exception while migrating to node "+message.getValue());
										return false;
									}

								});
							}
							boolean flag = false;
							for(Future<Boolean> f : resultList){
								if(!f.get()){
									flag = true;
								}
							}
							if(!flag)
							message.setStatus("1");
							else
							message.setStatus("0");
						} else{
							System.out.println("Nothing to delete at "+ localNode+" for range "+prms[0]+" to "+prms[1]);
							message.setValue("NO TRANSFER");
							message.setStatus("1");
						}

					}
					else if(message.getMsgType().equals("BALANCEADD")){

						String[] prms = message.getKey().split(":");
						Map<String,String> map = kvServer.getKeysInRange(Integer.parseInt(prms[0]) ,Integer.parseInt(prms[1]) , Integer.parseInt(prms[2]));
						List<Future<Boolean>> resultList = new ArrayList<>();
						if(map != null && map.size()>0){
							System.out.println("Migrating "+map.toString()+ " from node "+localNode+" to "+message.getValue());
							for(Map.Entry<String,String> entry : map.entrySet()){
								final String lkey = entry.getKey();
								final String lvalue = entry.getValue();
								Future<Boolean> res =  executionThreadPool.executor.submit(()->{
									try{
										KVClient client = new KVClient(Node.getObject(message.getValue()));
										return client.put(lkey, lvalue, true);
									} catch(Exception e){
										System.out.println("Exception while migrating to node "+message.getValue());
										return false;
									}

								});
								resultList.add(res);

							}
							boolean flag = false;
							for(Future<Boolean> f : resultList){
								if(!f.get()){
									flag = true;
								}
							}
							if(!flag)
							message.setStatus("1");
							else
							message.setStatus("0");
						} else{
							System.out.println("Nothing to migrate at "+ localNode+" for range "+prms[0]+" to "+prms[1]);
							message.setValue("NO TRANSFER");
							message.setStatus("1");
						}

					}
					else if(message.getMsgType().equals("ADD")){
						
						Node newNode = Node.getObject(message.getKey());
						
						if(newNode!=null){
							if(!consistentHashingCoordinator.handleNodeAddition(newNode)){
								message.setStatus("0");
								System.out.println("Could not adjust nodes while adding new node");
							}
							consistentHashingCoordinator.addNode(newNode);

							List<Node> nodeList = consistentHashingCoordinator.getNodeList();
							for(Node inode : nodeList){
									if(inode.equals(localNode)){
										continue;
									}
									executionThreadPool.executor.submit(()->{
										try{
											KVClient client = new KVClient(inode);
											client.connectToCoordinator(newNode,true);
										} catch(Exception e){
											System.out.println("Exception while brocasting new node addition"+e);
										}
									});	
								
							}

							message.setStatus("1");

						} else{
							message.setValue("NULL");
							message.setStatus("0");
						}
					}
					else if(message.getMsgType().equals("ADDR")){
						
						Node newNode = Node.getObject(message.getKey());
						if(newNode!=null){
							
							consistentHashingCoordinator.addNode(newNode);
		
							message.setStatus("1");

						} else{
							message.setValue("NULL");
							message.setStatus("0");
						}
					}
					else if(message.getMsgType().equals("REMOVE")){
						
						Node newNode = Node.getObject(message.getKey());
						if(newNode!=null){
							List<Node> nodeList = consistentHashingCoordinator.getNodeList();
							for(Node inode : nodeList){
									if(inode.equals(localNode)){
										continue;
									}
									executionThreadPool.executor.submit(()->{
										try{
											KVClient client = new KVClient(inode);
											client.deleteNode(newNode,true);
										} catch(Exception e){
											System.out.println("Exception while brocasting new node addition"+e);
										}
									});	
								
							}

							//consistentHashingCoordinator.deleteNode(newNode);

							message.setStatus("1");

						} else{
							message.setValue("NULL");
							message.setStatus("0");
						}
					}
					else if(message.getMsgType().equals("REMOVER")){
						
						Node newNode = Node.getObject(message.getKey());
						if(newNode!=null){
							
							consistentHashingCoordinator.deleteNode(newNode);
		
							message.setStatus("1");

						} else{
							message.setValue("NULL");
							message.setStatus("0");
						}
					}
					

					else if( message.getMsgType().endsWith("R")  ||  ((listnode.get(0).getIp().equals(localNode.getIp()) && (listnode.get(0).getPort() == localNode.getPort()) ) )  ){
						System.out.println("Key present locally");

						switch(message.getMsgType()){

							case "GET": 	
											int readCount = kvServer.getReadCount();
											final List<Node> readNodeList = consistentHashingCoordinator.getPreferenceList(message.getKey(),replicaCount);
											System.out.println("Coordinating request get with key " + key + "to nodes: "+readNodeList.toString());

											List<Callable<String>> readCallableTasks = new ArrayList<>();
											Callable<String> readCallableTask = null;
											for(int i=1;i<replicaCount;i++){
												final Node n = readNodeList.get(i);
												readCallableTask = ()->{
													try{
														KVClient client = new KVClient(n);	

														return client.get(key,true)+";"+n.toString();
													
													} catch(Exception e){
														System.out.println("Exception while getting data key "+key+" value "+value);
														return null;
													}
												};
												readCallableTasks.add(readCallableTask);
											}

											readCallableTask = ()->{
												try{
													System.out.println("KVServer handling request get with key " + key );
													return kvServer.get(key)+";"+localNode.toString();
												} catch(Exception e){
													System.out.println("Exception while getting data key "+key+" value "+value);
													return null;
												}
												
											};
											readCallableTasks.add(readCallableTask);

											List<Future<String>> readFutures = executionThreadPool.executor.invokeAll(readCallableTasks);
											
											List<String> readResultList= new ArrayList<>();
											List<String> readResultTotalList= new ArrayList<>();

											int readCompletedThreadCount = 0;

											while(readCompletedThreadCount < replicaCount && readResultList.size() < readCount){
												for(Future<String> future : readFutures){
													if(future.isDone()){
														String res = future.get(); 
														if(res != null){
															if(readResultList.size() < readCount){
															
																readResultList.add(future.get().split(";")[0]);
															}
														}
														readCompletedThreadCount++;
														readResultTotalList.add(future.get());
													}
												}
											}
											
											if(readResultList.size() >= readCount){
												message.setValue(getLastestResult(readResultList));

												//message.setValue(readResultList.toString());
												message.setStatus("1");
											} else{
												message.setStatus("0");
											}
											this.readRepair(readResultTotalList, key);
											break;

							case "PUT": 	
											int writeCount = kvServer.getWriteCount();
											
											final List<Node> writeNodeList = consistentHashingCoordinator.getPreferenceList(message.getKey(),replicaCount);
											System.out.println("Coordinating request put with key " + key + " value "+ value+" to nodes: "+writeNodeList.toString() );
											



											List<Callable<Boolean>> writeCallableTasks = new ArrayList<>();
											Callable<Boolean> writeCallableTask = null;
											for(int i=1;i<replicaCount;i++){
												final Node np = writeNodeList.get(i);		
												writeCallableTask = ()->{
													try{
														KVClient client = new KVClient(np);
														return client.put(key,value,true);
													} catch(Exception e){
														System.out.println("Exception while putting data key "+key+" value "+value);
														return null;
													}
													
												};
												writeCallableTasks.add(writeCallableTask);
											}
										
											writeCallableTask = ()->{
												try{
													
														System.out.println("KVServer handling request put with key " + key + " value "+ value );
														return kvServer.put(key, value);

			
													
												} catch(Exception e){
													System.out.println("Exception while putting data key "+key+" value "+value);
													return null;
												}
												
											};
											writeCallableTasks.add(writeCallableTask);
										
											List<Future<Boolean>> writeFutures = executionThreadPool.executor.invokeAll(writeCallableTasks);

											

											

											List<Boolean> writeResultList= new ArrayList<>();
											int writeCompletedThreadCount = 0;

											while(writeCompletedThreadCount < replicaCount && writeResultList.size() < writeCount){
												for(Future<Boolean> future : writeFutures){
													if(future.isDone()){
														Boolean res = future.get(); 
														if(res != null){
															if(writeResultList.size() < writeCount){
																writeResultList.add(future.get());
															}
														}
														writeCompletedThreadCount++;
													}
												}
											}
											message.setValue(writeResultList.toString());
											if(writeResultList.size() >= writeCount){
												message.setStatus("1");
											} else{
												message.setStatus("0");

											}

											/*writeCallableTasks = new ArrayList<>();
											writeCallableTask = null;
											for(int i=writeCount;i<replicaCount;i++){
												final Node np = writeNodeList.get(i);		
												writeCallableTask = ()->{
													KVClient client = new KVClient(np);
													return client.put(key,value,true);
												};
												writeCallableTasks.add(writeCallableTask);
											}
				
										
											executionThreadPool.executor.invokeAll(writeCallableTasks);*/

											break;
							case "DELETE":	

											int deleteCount = kvServer.getWriteCount();
											final List<Node> deleteNodeList = consistentHashingCoordinator.getPreferenceList(message.getKey(),replicaCount);
											System.out.println("Coordinating request delete with key " + key + " value "+ value + " to nodes : "+deleteNodeList.toString());

											List<Callable<Boolean>> deleteCallableTasks = new ArrayList<>();
											Callable<Boolean> deleteCallableTask = null;
											for(int i=1;i<deleteCount;i++){
												final Node nd = deleteNodeList.get(i);
												deleteCallableTask = ()->{
													try{
														KVClient client = new KVClient(nd);	
														return client.put(key,value,true);
													} catch(Exception e){
														System.out.println("Exception while delete data key "+key+" value "+value);
														return false;
													}
													
												};
												deleteCallableTasks.add(deleteCallableTask);
											}
										
											deleteCallableTask = ()->{
												try{
													System.out.println("KVServer handling request delete with key " + key + " value "+ value );
													return kvServer.delete(key);
												} catch(Exception e){
													System.out.println("Exception while delete data key "+key+" value "+value);
													return false;
												}
												
											};
											deleteCallableTasks.add(deleteCallableTask);
										
											List<Future<Boolean>> deleteFutures = executionThreadPool.executor.invokeAll(deleteCallableTasks);

											List<Boolean> deleteResultList= new ArrayList<>();
											int deleteCompletedThreadCount = 0;

											while(deleteCompletedThreadCount < replicaCount && deleteResultList.size() < deleteCount){
												for(Future<Boolean> future : deleteFutures){
													if(future.isDone()){
														Boolean res = future.get(); 
														if(res != null){
															if(deleteResultList.size() < deleteCount){
																deleteResultList.add(future.get());
															}
														}
														deleteCompletedThreadCount++;
													}
												}
											}
											message.setValue(deleteResultList.toString());
											if(deleteResultList.size() >= deleteCount){
												message.setStatus("1");
											} else{
												message.setStatus("0");

											}
											break;

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
						}

					} else{

						System.out.println("Coordinator redirecting request to node" + listnode.get(0).toString());
						this.client = new KVClient(listnode.get(0));
						System.out.println("Client created");
						switch(message.getMsgType()){

							case "GET": 	
							System.out.println("Here ");

											String result = this.client.get(message.getKey());
											System.out.println("Coordinator handling request get with key " + message.getKey());
											if(result != null){
												message.setValue(result);
												message.setStatus("1");
											} else{
												message.setStatus("0");
											}
											break;
							case "PUT":
											System.out.println("Here "); 	
											status = client.put(message.getKey(),message.getValue());
											System.out.println("Coordinator handling request put with key " + message.getKey() + " and value "+message.getValue() );
											message.setStatus( status?"1":"0");
											/*if(status){
												message.setStatus("1");
											} else{
												message.setStatus("0");
											}*/
											break;
							case "DELETE":	status = client.del(message.getKey());
											System.out.println("Coordinator handling request delete with key " + message.getKey());
											message.setStatus( status?"1":"0");
											break;
							/*case "GETR": 	result = kvServer.get(message.getKey());
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
											break;*/

						}

					}

					
				}
				
				json = message.toJson();
				out.println(json);

			} catch(Exception e){
				System.out.println("Exception while handling request "+e);
			} finally{
				try{
					in.close();
					out.close();
					clientSocket.close();
				} catch(Exception exp){
					System.out.println("Exception while closing streams and socket"+exp);
				}
				
			}
			 
		}
		
		
	}
	
}
