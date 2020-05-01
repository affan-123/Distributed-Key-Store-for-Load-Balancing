/*

Author : Niket Doke
Date : 10 Feb 2020

This class stores Key Value pairs into database. Every Value is stored with 
a version which is incremented everytime the key is updated Provides get,put 
and delete method for KVStore.

*/
package com.niket.DistributedSystem;
import java.net.Socket;
import com.niket.DistributedSystem.KVException;
import com.niket.DistributedSystem.KVMessage;
import com.niket.DistributedSystem.Node;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.UnknownHostException;


public class KVClient  {

	private Node node;
	
	/**
	 * @param server is the DNS reference to the Key-Value server
	 * @param port is the port on which the Key-Value server is listening
	 */
	public KVClient(Node node) {
		this.node = node;
	}

	private Socket getSocket() {
		try{
			return new Socket(this.node.getIp(),this.node.getPort());
		} catch(Exception e){
			System.out.println("Exception while connecting to host: "+e);
			return null;
		} 
	    
	}
	
	private void closeHost(Socket sock) throws KVException {
	    // TODO: Implement Me!

		;
	}

	public boolean put(String key, String value) throws KVException {
		return this.put(key, value, false);
	}

	public boolean put(String key, String value, boolean replica) throws KVException {
	    // TODO: Implement Me!
		KVMessage message =null;
		if(!replica){
			message = new KVMessage(key,value,"PUT");
		} else{
			message = new KVMessage(key,value,"PUTR");
		}
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending request put with key " + message.getKey()+ " value "+ message.getValue() + " to "+ node.toString());

		if (client == null) return false;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response +" node :"+this.node.toString());
				message = KVMessage.toKVMessage(response);

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

	    return ((message.getStatus().contains("true") || (message.getStatus().contains("1")))?true:false);
	}

	public String get(String key) throws KVException{
		return this.get(key, false);
	}


	public String get(String key, boolean replica) throws KVException {
		// TODO: Implement Me!
		KVMessage message = null;
		if(!replica){
			message = new KVMessage(key,"GET");
		} else {
			message = new KVMessage(key,"GETR");
		}
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending request get with key " + message.getKey() + " to "+ node.toString());

		if (client == null) return null;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);

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
	    return message.getValue();
	}

	public boolean del(String key) throws KVException {
		return this.del(key, false);
	}
	
	public boolean del(String key, boolean replica) throws KVException {
	    // TODO: Implement Me!
		KVMessage message = null;

		if(!replica){
			message = new KVMessage(key,"DELETE");
		} else{
			message = new KVMessage(key,"DELETER");
		}
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending request delete with key " + message.getKey() + " to "+ node.toString());

		if (client == null) return false ;

		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);


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
		return ((message.getStatus().contains("true") || (message.getStatus().contains("1")))?true:false);

	}	
	public Boolean connectToCoordinator(Node newNode) throws KVException {
		return connectToCoordinator(newNode, false);
	}



	public Boolean connectToCoordinator(Node newNode, boolean replica) throws KVException {
		// TODO: Implement Me!
		KVMessage message = null;
		if(!replica){
			message = new KVMessage(newNode.toString(),"ADD");
		} else {
			message = new KVMessage(newNode.toString(),"ADDR");
		}
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending node to Coordinator " + newNode.toString());

		if (client == null) return null;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);

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
	    return ((message.getStatus().contains("true") || (message.getStatus().contains("1")))?true:false);
	}

	public Boolean deleteNode(Node newNode) throws KVException {
		return deleteNode(newNode, false);
	}

	public Boolean deleteNode(Node newNode, boolean replica) throws KVException {
		// TODO: Implement Me!
		KVMessage message = null;
		if(!replica){
			message = new KVMessage(newNode.toString(),"REMOVE");
		} else {
			message = new KVMessage(newNode.toString(),"REMOVER");
		}
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending node to Coordinator " + newNode.toString());

		if (client == null) return null;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);

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
	    return ((message.getStatus().contains("true") || (message.getStatus().contains("1")))?true:false);
	}


	public String getRoutingInfo() throws KVException {
		// TODO: Implement Me!
		KVMessage message = new KVMessage("PSUEDO","ROUTE");
		//message.setMsgType("ROUTE");
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending request To get routes " + message.getKey() + " to "+ node.toString());

		if (client == null) return null;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);

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
	    return message.getValue();
	}

	public boolean balanceadd(int low, int high,int size, Node destNode) throws KVException {
		// TODO: Implement Me!
		KVMessage message = new KVMessage(low+":"+high+":"+size,destNode.toString(),"BALANCEADD");
		//message.setMsgType("ROUTE");
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending  balance routes " + message.getKey() + " to "+ node.toString());

		if (client == null) return false;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);

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
	    return  ((message.getStatus().contains("true") || (message.getStatus().contains("1")))?true:false);
	}

	public boolean deleteInRange(int low, int high,int size) throws KVException {
		// TODO: Implement Me!
		KVMessage message = new KVMessage(low+":"+high+":"+size,"DELETEINRANGE");
		//message.setMsgType("ROUTE");
		String json = message.toJson();
		Socket client = getSocket();
		System.out.println("KVClient sending  delete routes " + message.getKey() + " to "+ node.toString());

		if (client == null) return false;
		BufferedReader in = null;
			 PrintWriter out = null;
			 try{
				in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream(),true);
				out.println(json);
				String response = in.readLine();
				System.out.println("Response is : "+response);
				message = KVMessage.toKVMessage(response);

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
	    return ((message.getStatus().contains("true") || (message.getStatus().contains("1")))?true:false);
	}

	
	
}
