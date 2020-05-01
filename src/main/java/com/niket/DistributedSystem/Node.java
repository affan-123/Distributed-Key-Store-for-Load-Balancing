package com.niket.DistributedSystem;

import java.net.Socket;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Node{
    private String ip;
    private int port;
    private String id;
    private boolean available;
    private Socket socket = null;

    public Node(String ip,int port,String ID){
        this.ip = ip;
        this.port = port;
        this.id = ID;
        this.available = false;
    }

    public Node(String ip,String ID){
        this.ip = ip;
        this.id = ID;
        this.port = 8080;
        this.available = false;
    }

    public Node(){

    }

    public String getIp(){
        return this.ip;
    }

    public int getPort(){
        return this.port;
    }

    public boolean isAvailable(){
        return this.available;
    }

    public String getID(){
        return this.id;
    }

    public void setStatus(boolean status){
        this.available = status;
    }

    /**
     * @param socket the socket to set
     */
    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public Socket getSocket(){
        return this.socket;
    }


    @Override
    public String toString(){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            String msg = objectMapper.writeValueAsString(this);
            return msg;
        } catch(Exception e){
            System.out.println("Exception while converting Node to String "+e);
            return null;
        }
        
    }

    @Override
    public boolean equals(Object obj) { 
          
    // if both the object references are  
    // referring to the same object. 
    if(this == obj) 
            return true; 
          
        // it checks if the argument is of the  
        // type Geek by comparing the classes  
        // of the passed argument and this object. 
        // if(!(obj instanceof Geek)) return false; ---> avoid. 
        if(obj == null || obj.getClass()!= this.getClass()) 
            return false; 
          
        // type casting of the argument.  
        Node node = (Node) obj; 
          
        // comparing the state of argument with  
        // the state of 'this' Object. 
        return (node.ip.equals(this.ip)  &&  node.id.equals(this.id)  && node.port == this.port); 
    } 

    @Override
    public int hashCode() 
    { 
        int result = 17;
        result = 31 * result + id.hashCode();
        result = 31 * result + port;
        result = 31 * result + ip.hashCode();
        return result; 
    }
    
    public static Node getObject(String json){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
            return objectMapper.readValue(json, Node.class);
            
        } catch(Exception e){
            System.out.println("Exception while converting json to Node"+e);
            return null;
        }
        
    }
}