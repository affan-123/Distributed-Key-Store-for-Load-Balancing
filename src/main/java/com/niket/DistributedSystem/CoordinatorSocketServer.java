package com.niket.DistributedSystem;

import java.net.ServerSocket;
import java.net.InetAddress;  
import java.io.IOException;
import java.net.UnknownHostException;
import com.niket.DistributedSystem.NetworkHandler;
import com.niket.DistributedSystem.Node;

class CoordinatorSocketServer{

    private Node node;
    private ServerSocket server;
    private NetworkHandler handler;

    public CoordinatorSocketServer(Node node,NetworkHandler handler) throws IOException,UnknownHostException{
        this.node = node;
        this.server = new ServerSocket(node.getPort(),Config.QUEUE_SIZE,InetAddress.getByName(this.node.getIp()));
        this.handler = handler;
    }

    public void listen() throws IOException{
        
        while(true){
            this.handler.handle(this.server.accept());
            System.out.println("Server got request Handling it!!");
        }
    }

    private void closeSocket() {
	    try {
            server.close();
        } catch (IOException e) {
            System.out.println("Exception while closing socket: " + e);
        }

	}
	
	protected void finalize(){
		closeSocket();
	}

    

    

}                                                                                                                                                                   