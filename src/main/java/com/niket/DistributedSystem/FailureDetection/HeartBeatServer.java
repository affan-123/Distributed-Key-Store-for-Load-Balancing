package com.niket.DistributedSystem.FailureDetection;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import java.net.SocketException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.niket.DistributedSystem.ExecutionThreadPool;
import com.niket.DistributedSystem.Node;


public class HeartBeatServer {
    DatagramSocket socket;
    Node lNode;
    int port;
    String ID = null;
    ExecutionThreadPool executionThreadPool;
    HeartBeatManager manager;
    

    public HeartBeatServer(Node lNode) throws SocketException{
        this.socket = new DatagramSocket(lNode.getPort()+1000);
        this.lNode = lNode;
        this.manager = HeartBeatManager.getInstance(lNode);
        executionThreadPool = ExecutionThreadPool.getInstance();
    }


    public void listen(){
        try{
            while(true){
                byte[] buffer = new byte[256];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                this.socket.receive(packet);
                executionThreadPool.addToQueue(new HandleReceive(socket, packet));
                

            }
           
        } catch(Exception e ){
            System.out.println("Exception while handling heartbeat packets"+e);
        }
    }

    class HandleReceive implements Runnable{

        DatagramPacket packet;
        DatagramSocket socket ;
        
        public HandleReceive(DatagramSocket socket, DatagramPacket packet){
            this.packet = packet;
            this.socket = socket;
        }

        public void run(){

            try{
    
                
               String received = new String(packet.getData(), 0, packet.getLength());
               // System.out.println("Server UDP received "+received);
                
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
                Node node = objectMapper.readValue(received, Node.class);
                //System.out.println("Server Node receieved is "+node.toString());
                //System.out.println("Status of node is "+manager.getStatus(node));
                //manager.setAvialable(node);
                manager.report(node);

                //manager.addNode(node, true);
                
    
            } catch(Exception e){
                System.out.println("Exception while receiving UDP packet"+e);
            }
    }

    
    }

}