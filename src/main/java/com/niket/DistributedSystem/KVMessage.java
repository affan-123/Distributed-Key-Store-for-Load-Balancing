package com.niket.DistributedSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.annotation.JsonInclude;


@JsonInclude(JsonInclude.Include.NON_NULL) 	//  ignore all null fields
public class KVMessage {
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String status = null;
	private String message = null;

    public KVMessage(){
        
    }
    public KVMessage(String key,String msgType){
        this.key = key;
        this.msgType = msgType;
    }

    public KVMessage(String key,String value,String msgType){
        this.value = value;
        this.key = key;
        this.msgType = msgType;
    }

    

    public static KVMessage toKVMessage(String jsonString){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
            KVMessage msg = objectMapper.readValue(jsonString, KVMessage.class);
            return msg;
        } catch(Exception e){
            System.out.println("Exception while serializing "+e);
        }
        return null;
        
    }
	
	public final String getKey() {
		return key;
	}

	public final void setKey(String key) {
		this.key = key;
	}

	public final String getValue() {
		return value;
	}

	public final void setValue(String value) {
		this.value = value;
	}

	public final String getStatus() {
		return status;
	}

	public final void setStatus(String status) {
		this.status = status;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
    }
    
    public final void setMsgType(String message) {
		this.msgType = message;
	}

	public String getMsgType() {
		return msgType;
	}

    public String toJson(){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(this);
            return json;
        } catch(Exception e){
            System.out.println("Exception while serializing "+e);
        }
        return null;
    }

}