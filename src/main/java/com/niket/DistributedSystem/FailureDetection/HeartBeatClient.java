package com.niket.DistributedSystem.FailureDetection;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.niket.DistributedSystem.Node;

public class HeartBeatClient {
    DatagramSocket socket;
    
    Node node;

    public HeartBeatClient(Node node) throws SocketException,UnknownHostException {
        this.socket = new DatagramSocket();
        this.node = node;
    }
    public boolean sendHeartBeat(Node source){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            String msg = objectMapper.writeValueAsString(source);
            byte[] bufferMsg = msg.getBytes();
            DatagramPacket packet  = new DatagramPacket(bufferMsg, bufferMsg.length, InetAddress.getByName(this.node.getIp()), this.node.getPort()+1000);
            this.socket.send(packet);
            return true;
        } catch(Exception e){
            System.out.println("Exception while sending UDP packet"+e);
            return false;
        }
    }

   

    

   

    
}

