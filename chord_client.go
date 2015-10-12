package main

import (
	"fmt"
	"os"
	"log"
	"encoding/json"
	"io/ioutil"
	"net/rpc/jsonrpc"
	"strconv"
	"bufio"
	"strings"
	"time"
)

// Structure to hold the Configuration parameters if each peer
type Server_Config struct {
		ServerID string
		Protocol string 
		IpAddress string 
		Port int  
		PeerIpAddress string
		PeerPort int
		StartMethod string
    	PersistentStorageContainer struct {
			File string
	}
		Methods [7]string
	}

 // Structure to parse the client JSON messages.
    type Client_Request struct{
    	Method string
		Params json.RawMessage
	}

// Structure to hold the value in the params of the record    
    type Value struct{
    	Content string
    	Size string
    	Created string
    	Modified string
    	Accessed string
    	Permission string
    }
    
    // Structure to hold the response JSON message received from the server.
    type Response struct{
    	Msg []byte
    }
    
    // Structure to hold the JSON message to be sent to server to make the remote procedure call.
    type Request struct{
    	Msg []byte
    }
    
    // Structure to parase the response from the server
     type Peer_Response struct{
   	Result *json.RawMessage
	Error error
}
	func main(){
	if len(os.Args) <= 2 {
		fmt.Println("Did not receive all Command Line Arguments")
		log.Fatal(1)
	}
	config_file_name := os.Args[1]
	config_file, err_read := ioutil.ReadFile(config_file_name)
	// Config file holds the configuration of the server in the form of a JSON message. 
	if err_read != nil{
		fmt.Println(err_read)
	}
	
  	var config_parameters Server_Config
   	// Unmarshalling the JSON message to get the server configuration parameters.	
   	err_unmarshal := json.Unmarshal(config_file, &config_parameters)
   	if err_unmarshal !=  nil{
		fmt.Println(err_unmarshal)	
	}
	client_connection, _ := jsonrpc.Dial(config_parameters.Protocol, config_parameters.IpAddress+":"+strconv.Itoa(config_parameters.Port))
	input_file_name := os.Args[2]
	
	file , err := os.OpenFile(input_file_name, os.O_RDONLY,0777)
	if err != nil{
	log.Fatal(1)
	}
	
	defer file.Close()
	reader := bufio.NewReader(file)
	// Reading the Input File line by line.
	line,err_eof := reader.ReadString('\n')
	for err_eof == nil{
	client_request := &Client_Request{}
		var client_request_msg []byte
		fmt.Println("\n\n" + "Client --> Server " + line)
		//There might be cariage return and line feed depending on the text editor used to create the input file.
		if(strings.Contains(line, "\r")){
			client_request_msg = []byte(line[:len(line)-2])
			err := json.Unmarshal(client_request_msg, &client_request)
			if err !=nil{
				fmt.Println(err)
			}
		}else{//If only line feed is present at the end of each line in the input file.
				client_request_msg = []byte(line[:len(line)-1])
				err := json.Unmarshal(client_request_msg, &client_request)
				if err !=nil{
					fmt.Println(err)
				}
		}
		
		if(client_request.Method == "insert" || client_request.Method == "insertOrUpdate"){
		param_str := string(client_request.Params)
		str_ctr := strings.SplitAfterN(param_str,",",3)
		key:=(str_ctr[0][2:len(str_ctr[0])-2])
		relationship:=(str_ctr[1][2:len(str_ctr[1])-2])
		value:=(str_ctr[2][1:len(str_ctr[2])-1])

		request_value := &Value{}
		err = json.Unmarshal([]byte(value), &request_value)
				if err !=nil{
					fmt.Println(err)
				}
		created_time := string(time.Now().Format("02/01/2006, 15:04:05"))
		value_msg, _ := json.Marshal(Value{Content: request_value.Content,Size: strconv.Itoa(len(request_value.Content)) + "B",Created: created_time, Permission: request_value.Permission})
		client_request_msg = []byte("{\"method\": \""+ client_request.Method+ "\", \"params\": [\""+key+"\", \""+relationship + "\", "+string(value_msg)+"]}")
		req := Request{client_request_msg}
    	res := Response{}
	    	if client_request.Method == "insert"{
				err = client_connection.Call("Response.Insert", req, &res)
			}else{
	    		err = client_connection.Call("Response.InsertORUpdate", req, &res)
	    	}
			if err !=nil{
				fmt.Println(err)
			}
			if res.Msg != nil{
				fmt.Println("Server --> Client " +string(res.Msg))
			}
		}else if(client_request.Method == "lookup" || client_request.Method == "delete"){
			req := Request{client_request_msg}
    		res := Response{}
    		if client_request.Method == "lookup"{
				err = client_connection.Call("Response.Lookup", req, &res)
			}else if client_request.Method == "delete"{
				err = client_connection.Call("Response.Delete", req, &res)
			}
			
			if err !=nil{
				fmt.Println(err)
			}
			if res.Msg != nil{
				fmt.Println("Server --> Client " +string(res.Msg))
			}
			
		}else{
			
			req := Request{}
    		res := Response{}
			
			if client_request.Method == "listKeys"{
				err = client_connection.Call("Response.ListKeys", &req, &res)
			}else if client_request.Method == "listIds"{
				err = client_connection.Call("Response.ListIds", &req, &res)
			}else if client_request.Method == "shutdown"{
				err = client_connection.Call("Response.Shutdown", &req, &res)
			}
			if err !=nil{
				fmt.Println(err)
			}
			if res.Msg != nil{
				fmt.Println("Server --> Client " +string(res.Msg))
			}
		}
//	
		fmt.Println()
		time.Sleep(time.Second * 1)
		line,err_eof = reader.ReadString('\n')
		}
		client_connection.Close()
	}
