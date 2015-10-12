package main

import (
    "fmt"
    "os"
	"io"
	"log"
	"io/ioutil"
	"net"
	"net/rpc"
	"encoding/json"
	"net/rpc/jsonrpc"
	"strconv"
	"time"
	"crypto/sha1"
	"encoding/hex"
	"math/big"
	"encoding/binary"
	"math"
	"math/rand"
	"strings"
	"bufio"
	"bytes"
)

// Structure to hold the Configuration parameters if each peer
type Node_Config struct {
		ServerID string
		Protocol string 
		IpAddress string 
		Port int  
		PeerIpAddress string
		PeerPort int
    	PersistentStorageContainer struct {
			File string
	}
		Methods [7]string
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

// Structure to hold the response JSON message to be sent from the server.
type Response struct{
  	Msg []byte
}
    
// Structure to hold the JSON message to be sent to server to make the remote procedure call.
type Request struct{
   	Msg []byte
}

//  Structure to represent the entry in the Finger Table
type Node_Entry struct {
	NodeId int
	IpAddress string
	Port int
}

// Structure to parse the client request
type Client_Request struct{
   	Method string
	Params json.RawMessage
}

// Structure to send message to a peer
 type Peer_Response struct{
   	Result *json.RawMessage
	Error error
}
 
 // Structure to hold the entries when giving to a successor
 type To_Successor struct{
 	Predecessor Node_Entry
 	Data bytes.Buffer
 }

// Structure to give ids and keys to the successor
 type To_Peer struct{
 	Result string	
 	Error error
 }
 
 // constants
 const NIL = -1
 const M = 5
 
 // Global variables
 var SIZE = math.Pow(float64(2),float64(M))
 var key_size int
 var rel_size int
 var node_parameters = &Node_Config{}
 var node_listener net.Listener
 var node_id int
 var predecessor Node_Entry
 var successor Node_Entry
 var finger [M] Node_Entry
 var next = 0
 var file1 *os.File
 
// Function to start the node 
func start_node(){
    	if len(os.Args) <= 1 {
		fmt.Println("Received No Command Line Arguments")
		log.Fatal(1)
	}
	config_file_name := os.Args[1]
	config_file, err := ioutil.ReadFile(config_file_name)
	// Config file holds the configuration of the server in the form of a JSON message.
	if err != nil{
		log.Fatal(1)
	}
   	// Unmarshalling the JSON message to get the server configuration parameters.	
   	err1 := json.Unmarshal(config_file, &node_parameters)   // storing in the structure the respective values from the json file.
   	if err1 !=  nil{
		fmt.Println(err1)	
	}
   	
	node_listener, err = net.Listen(node_parameters.Protocol, node_parameters.IpAddress+":"+strconv.Itoa(node_parameters.Port))
	if err1 !=  nil{
		fmt.Println(err1)	
	}
	
	for i := M; i > 0; i-- {
		finger[i-1].NodeId = -1
	}
	
	key_size = (int(M)/2)
	rel_size = (int(M)/2)
	if((int(M)%2) !=0){
		key_size++
		}	
	
	node_id = get_id((node_parameters.IpAddress+":"+strconv.Itoa(node_parameters.Port)),int(SIZE))
	successor.NodeId = -1	
	
	if node_parameters.PeerIpAddress == ""{
		create()
	}else{
		join()
	}

	file1, erro := os.Create(node_parameters.PersistentStorageContainer.File)
	file1.Close()
	fmt.Println(erro)
	go Stabilize()
	go Fix_Fingers()
	go Check_Predecessor()
	go Purge()
	fmt.Println("Node Started with id = " + strconv.Itoa(node_id))
	fmt.Println("Node Successor = " + strconv.Itoa(successor.NodeId))
	fmt.Println("Node Predecessor = " + strconv.Itoa(predecessor.NodeId))
	// Starting the server to accept connections continously.
		for {
			conn, err := node_listener.Accept()
			if err != nil {
				continue
			}
			jsonrpc.ServeConn(conn)
		}
    }


// Function to insert the record in the server database.
func (t *Response) Insert (request *Request, response *Response) error{
	var errorr error
	// Unmarshal the request message
	client_request_msg := &Client_Request{}
	err := json.Unmarshal(request.Msg, &client_request_msg)
	if err !=nil{
		fmt.Println(err)
		errorr = err
	}
	
	// Break down params into key, relationship and values
	param_str := string(client_request_msg.Params)
	str_ctr := strings.SplitAfterN(param_str,",",3)
	key:=(str_ctr[0][2:len(str_ctr[0])-2])
	relationship:=(str_ctr[1][2:len(str_ctr[1])-2])
	value:=(str_ctr[2][1:len(str_ctr[2])-1])
	
	key_hash := get_id(key,int(math.Pow(2,float64(key_size))))
	rel_hash := get_id(relationship,int(math.Pow(2,float64(rel_size))))
	key_id := getKey(key_hash,key_size,rel_hash,rel_size)
	
	if(key_id<=node_id && key_id>predecessor.NodeId) || (node_id<predecessor.NodeId && ((key_id>predecessor.NodeId && key_id<int(SIZE)) || (key_id>=0 && key_id<=node_id))){
		// key belongs to this node, do the lookup here
		res := json.RawMessage(`false`)
		file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
		var flag bool
		reader := bufio.NewReader(file)
		defer file.Close()
		line,err_eof := reader.ReadString('\n')
		// Check if the file already contains the key, relationship pair.
		for err_eof == nil{
			if(strings.Contains(line, (key + ", " + relationship +","))){
				flag = true
				break
			}
			line,err_eof = reader.ReadString('\n')
		}
		
		if(!flag){
		// Record nt already present, write it to the file.
		res = json.RawMessage(`true`)
		file , errorr = os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_APPEND|os.O_WRONLY,0777)
		if errorr != nil{
			fmt.Print(errorr)
		}
		defer file.Close()
		
		_, errorr = file.WriteString((key + ", " + relationship + ", " + value + "\n"))
		if errorr != nil { 
			fmt.Print(errorr)
		}
		}
		if errorr != nil{
			res = json.RawMessage(`false`)
		}
		
		// Send JSON response to the client.
		rsp_msg := Peer_Response{Result: &res, Error: errorr}
		response.Msg, errorr = json.Marshal(rsp_msg)
		if errorr != nil{
		fmt.Print(errorr)
		}
	}else{
		req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(key_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
    	res := Response{}
    	resp := &Response{}
    	resp.Find_Successor(&req,&res)
    	if(res.Msg!=nil){
   		dest_node := &Node_Entry{}
		err := json.Unmarshal(res.Msg, &dest_node)
		fmt.Println("Looked up for successor of: " + strconv.Itoa(key_id) + "  Got back: " + strconv.Itoa(dest_node.NodeId))
		if err !=nil{
			fmt.Println(err)
		}
		req = *request
		res = Response{}
		connection, _ := jsonrpc.Dial(node_parameters.Protocol, dest_node.IpAddress+":"+strconv.Itoa(dest_node.Port))
		connection.Call("Response.Insert",req,&res)
		connection.Close()
		if(res.Msg!=nil){
			response.Msg=res.Msg
			}
		}
	}
	
    fmt.Println(errorr)
    return nil
}

// Function to lookup records.
func (t *Response) Lookup (request *Request, response *Response) error{
	var errorr error
	var i int

	// Unmarshal the request message
	client_request_msg := &Client_Request{}
	errorr = json.Unmarshal(request.Msg, &client_request_msg)
	if errorr !=nil{
		fmt.Println(errorr)
	}
	
	// Break down params into key, relationship and values
	param_str := string(client_request_msg.Params)
	str_ctr := strings.SplitAfterN(param_str,",",3)
	key:=(str_ctr[0][2:len(str_ctr[0])-2])
	relationship:=(str_ctr[1][2:len(str_ctr[1])-2])
	
	if(key != "" && relationship != ""){// when key and relationship both are present
		key_hash := get_id(key,int(math.Pow(2,float64(key_size))))
		rel_hash := get_id(relationship,int(math.Pow(2,float64(rel_size))))
		key_id := getKey(key_hash,key_size,rel_hash,rel_size)
		
		req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(key_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
  	 	res := Response{}
    	resp := &Response{}
    	resp.Find_Successor(&req,&res)
    	if(res.Msg!=nil){
   		dest_node := &Node_Entry{}
		err := json.Unmarshal(res.Msg, &dest_node)
		fmt.Println("Looked up for successor of: " + strconv.Itoa(key_id) + "  Got back: " + strconv.Itoa(dest_node.NodeId))
		if err !=nil{
			fmt.Println(err)
		}
		req = *request
		res = Response{}
		if dest_node.NodeId == node_id{
			resp := &Response{}
    		resp.Lookup_Record(&req,&res)
		}else{
			connection, _ := jsonrpc.Dial(node_parameters.Protocol, dest_node.IpAddress+":"+strconv.Itoa(dest_node.Port))
			connection.Call("Response.Lookup_Record",req,&res)
			connection.Close()
		}
		response.Msg=res.Msg
		}
		
	}else if key == ""{ // When only relationship is present
		for i = 0; i < int(math.Pow(2,float64(key_size))); i++{
			rel_hash := get_id(relationship,int(math.Pow(2,float64(rel_size))))
			key_id := getKey(i,key_size,rel_hash,rel_size)
			
			req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(key_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
	  	 	res := Response{}
	    	resp := &Response{}
	    	resp.Find_Successor(&req,&res)
	    	if(res.Msg!=nil){
	   		dest_node := &Node_Entry{}
			err := json.Unmarshal(res.Msg, &dest_node)
			fmt.Println("Looked up for successor of: " + strconv.Itoa(key_id) + "  Got back: " + strconv.Itoa(dest_node.NodeId))
			if err !=nil{
				fmt.Println(err)
			}
			req = *request
			res = Response{}
			if dest_node.NodeId == node_id{
				resp := &Response{}
    			resp.Lookup_Record(&req,&res)
			}else{
				connection, _ := jsonrpc.Dial(node_parameters.Protocol, dest_node.IpAddress+":"+strconv.Itoa(dest_node.Port))
				connection.Call("Response.Lookup_Record",req,&res)
				connection.Close()
			}
			response.Msg=res.Msg
			if(string(res.Msg[10:15]))!="false"{
				break
			}
			}
    	}
	}else{// when ony key is present
		for i = 0; i < int(math.Pow(2,float64(rel_size))); i++{
			key_hash := get_id(key,int(math.Pow(2,float64(key_size))))
			key_id := getKey(key_hash,key_size,i,rel_size)
			
			req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(key_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
	  	 	res := Response{}
	    	resp := &Response{}
	    	resp.Find_Successor(&req,&res)
	    	if(res.Msg!=nil){
	   		dest_node := &Node_Entry{}
			err := json.Unmarshal(res.Msg, &dest_node)
			fmt.Println("Looked up for successor of: " + strconv.Itoa(key_id) + "  Got back: " + strconv.Itoa(dest_node.NodeId))
			if err !=nil{
				fmt.Println(err)
			}
			req = *request
			res = Response{}
			if dest_node.NodeId == node_id{
				resp := &Response{}
	    		resp.Lookup_Record(&req,&res)
			}else{
				connection, _ := jsonrpc.Dial(node_parameters.Protocol, dest_node.IpAddress+":"+strconv.Itoa(dest_node.Port))
				connection.Call("Response.Lookup_Record",req,&res)
				connection.Close()
			}
			response.Msg=res.Msg
			if(string(res.Msg[10:15]))!="false"{
				break
			}
			}
		}
	}
	
	return nil
}


// Function to lookup the record from the server database.
func (t *Response) Lookup_Record (request *Request, response *Response) error{
	var errorr error
	// Unmarshal the request message
	client_request_msg := &Client_Request{}
	err := json.Unmarshal(request.Msg, &client_request_msg)
	if errorr !=nil{
		fmt.Println(errorr)
	}
	
	// Break down params into key, relationship and values
	param_str := string(client_request_msg.Params)
	str_ctr := strings.SplitAfterN(param_str,",",3)
	key:=(str_ctr[0][2:len(str_ctr[0])-2])
	relationship:=(str_ctr[1][2:len(str_ctr[1])-2])
	match_string := (key + ", " + relationship + ",")
	if(key == ""){
		match_string = (", " + relationship + ",")
	}else if(relationship == ""){
		match_string = (key + ", ")
	}
	
	res := json.RawMessage(`false`)
	var line_buf bytes.Buffer
	file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
	if errorr != nil{
		fmt.Print(errorr)
	}
		
	reader := bufio.NewReader(file)
		
	line,err_eof := reader.ReadString('\n')
	for err_eof == nil{
		if(strings.Contains(line, match_string)){
			str_ctr := strings.SplitAfterN(line,",",3)
			res = json.RawMessage([]byte("[\"" + (str_ctr[0][:len(str_ctr[0])-1]) + "\", " +  "\"" + (str_ctr[1][1:len(str_ctr[1])-1]) + "\", " + (str_ctr[2][:len(str_ctr[2])-1]) + "]"))
			request_value := &Value{}
			err = json.Unmarshal([]byte((str_ctr[2][:len(str_ctr[2])-1])), &request_value)
			if err !=nil{
				fmt.Println(err)
			}//update the accessed time
			accessed_time := string(time.Now().Format("02/01/2006, 15:04:05"))
			value_msg, _ := json.Marshal(Value{Content: request_value.Content,Size: request_value.Size,Created: request_value.Created,Modified: request_value.Modified,Accessed: accessed_time, Permission: request_value.Permission})
			line_buf.WriteString((str_ctr[0][:len(str_ctr[0])-1]) + ", " + (str_ctr[1][1:len(str_ctr[1])-1]) + ", " + string(value_msg) + "\n")
			res = json.RawMessage([]byte("[\"" + (str_ctr[0][:len(str_ctr[0])-1]) + "\", " +  "\"" + (str_ctr[1][1:len(str_ctr[1])-1]) + "\", " + (string(value_msg)) + "]"))
			} else{
				line_buf.WriteString(line)
			}
			line,err_eof = reader.ReadString('\n')
			}
	file.Close()
		
	file , errorr = os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_WRONLY|os.O_APPEND,0777)
	file.Truncate(0)
	if errorr != nil{
		fmt.Print(errorr)
	}
	_,errorr = file.Write(line_buf.Bytes())
	if errorr != nil {
	    fmt.Println(errorr)
	}
			
	file.Close()
	// Send JSON response to the client.
	rsp_msg := Peer_Response{Result: &res, Error: errorr}
	response.Msg, errorr = json.Marshal(rsp_msg)
	if errorr != nil{
		fmt.Print(errorr)
	}
	
    fmt.Println(errorr)
    return nil
}

// Functio to update the record
func (t *Response) InsertORUpdate (request *Request, response *Response) error{
	var errorr error
	// Unmarshal the request message
	client_request_msg := &Client_Request{}
	err := json.Unmarshal(request.Msg, &client_request_msg)
	if err !=nil{
		fmt.Println(err)
		errorr = err
	}
	
	// Break down params into key, relationship and values
	param_str := string(client_request_msg.Params)
	str_ctr := strings.SplitAfterN(param_str,",",3)
	key:=(str_ctr[0][2:len(str_ctr[0])-2])
	relationship:=(str_ctr[1][2:len(str_ctr[1])-2])
	value:=(str_ctr[2][1:len(str_ctr[2])-1])
	
	key_hash := get_id(key,int(math.Pow(2,float64(key_size))))
	rel_hash := get_id(relationship,int(math.Pow(2,float64(rel_size))))
	key_id := getKey(key_hash,key_size,rel_hash,rel_size)
	
	if(key_id<=node_id && key_id>predecessor.NodeId) || (node_id<predecessor.NodeId && ((key_id>predecessor.NodeId && key_id<int(SIZE)) || (key_id>=0 && key_id<=node_id))){
		// key belongs to this node, do the lookup here
		
		res := json.RawMessage(`false`)
		var line_buf bytes.Buffer
		new_line := key + ", " + relationship + ", " + value + "\n"
		file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
			if errorr != nil{
				fmt.Print(errorr)
			}
		reader := bufio.NewReader(file)
		line,err_eof := reader.ReadString('\n')
		for err_eof == nil{
			if(strings.Contains(line, (key + ", " + relationship + ","))){
				str_ctr := strings.SplitAfterN(line,",",3)
				
				request_value := &Value{}
				err = json.Unmarshal([]byte(value), &request_value)
				
				file_value := &Value{}
				err = json.Unmarshal([]byte((str_ctr[2][:len(str_ctr[2])-1])), &file_value)
				
				if err !=nil{
					fmt.Println(err)
				}
				if request_value.Permission == "RW"{// Only update if the permission is Read/Write
				modified_time := string(time.Now().Format("02/01/2006, 15:04:05"))// Update the modified time
				value_msg, _ := json.Marshal(Value{Content: request_value.Content,Size: strconv.Itoa(len(request_value.Content)) + "B",Created: file_value.Created,Modified: modified_time,Accessed: file_value.Accessed, Permission: file_value.Permission})
				line_buf.WriteString(key + ", " + relationship + ", " + string(value_msg) + "\n")
				res = json.RawMessage([]byte("[\"" + (str_ctr[0][:len(str_ctr[0])-1]) + "\", " +  "\"" + (str_ctr[1][1:len(str_ctr[1])-1]) + "\", " + (string(value_msg)) + "]"))
				}else{
					line_buf.WriteString(line)
				}
				new_line = ""
				}else{
					line_buf.WriteString(line)
				}
			line,err_eof = reader.ReadString('\n')
			}
		
		file.Close()
		file , errorr = os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_WRONLY|os.O_APPEND,0777)
			file.Truncate(0)
			if errorr != nil{
				fmt.Print(errorr)
			}
			_,err := file.Write(line_buf.Bytes())
			if err != nil {
			    fmt.Println(err)
			}
			if new_line != ""{
				_, errorr = file.WriteString(new_line)
				if errorr != nil { 
					fmt.Print(errorr)
				}
				}
			file.Close()
		// Send JSON response to the client.
		rsp_msg := Peer_Response{Result: &res, Error: errorr}
		response.Msg, errorr = json.Marshal(rsp_msg)
		if errorr != nil{
		fmt.Print(errorr)
		}
	}else{//Find the peer to which the key should belong and call the lookup function of that peer remotely.
		req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(key_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
    	res := Response{}
    	resp := &Response{}
    	resp.Find_Successor(&req,&res)
    	if(res.Msg!=nil){
   		dest_node := &Node_Entry{}
		err := json.Unmarshal(res.Msg, &dest_node)
		fmt.Println("Looked up for successor of: " + strconv.Itoa(key_id) + "  Got back: " + strconv.Itoa(dest_node.NodeId))
		if err !=nil{
			fmt.Println(err)
		}
		req = *request
		res = Response{}
		connection, _ := jsonrpc.Dial(node_parameters.Protocol, dest_node.IpAddress+":"+strconv.Itoa(dest_node.Port))
		connection.Call("Response.InsertORUpdate",req,&res)
		connection.Close()
		if(res.Msg!=nil){
			response.Msg=res.Msg
			}
		}
	}
 fmt.Println(errorr)
    return nil
}

// Fucntion to Delete a record
func (t *Response) Delete (request *Request, response *Response) error{
	var errorr error
	// Unmarshal the request message
	client_request_msg := &Client_Request{}
	err := json.Unmarshal(request.Msg, &client_request_msg)
	if err !=nil{
		fmt.Println(err)
		errorr = err
	}
	
	// Break down params into key, relationship and values
	param_str := string(client_request_msg.Params)
	str_ctr := strings.SplitAfterN(param_str,",",3)
	key:=(str_ctr[0][2:len(str_ctr[0])-2])
	relationship:=(str_ctr[1][2:len(str_ctr[1])-2])
	
	key_hash := get_id(key,int(math.Pow(2,float64(key_size))))
	rel_hash := get_id(relationship,int(math.Pow(2,float64(rel_size))))
	key_id := getKey(key_hash,key_size,rel_hash,rel_size)
	
	if(key_id<=node_id && key_id>predecessor.NodeId) || (node_id<predecessor.NodeId && ((key_id>predecessor.NodeId && key_id<int(SIZE)) || (key_id>=0 && key_id<=node_id))){
		// key belongs to this node, do the lookup here
		var line_buf bytes.Buffer
		file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
		if errorr != nil{
			fmt.Print(errorr)
		}
		reader := bufio.NewReader(file)
		line,err_eof := reader.ReadString('\n')
		for err_eof == nil{
			if(strings.Contains(line, (key + ", " + relationship + ","))){
				str_ctr := strings.SplitAfterN(line,",",3)
				request_value := &Value{}
				err = json.Unmarshal([]byte((str_ctr[2][:len(str_ctr[2])-1])), &request_value)
				if err !=nil{
					fmt.Println(err)
				}
					if request_value.Permission != "RW"{
						line_buf.WriteString(line)
					}
				}else{
					line_buf.WriteString(line)
				}
			line,err_eof = reader.ReadString('\n')
			}
		
		file.Close()
		file , errorr = os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_WRONLY|os.O_APPEND,0777)
			file.Truncate(0)
			if errorr != nil{
				fmt.Print(errorr)
			}
			_,err := file.Write(line_buf.Bytes())
			if err != nil {
			    fmt.Println(err)
			}
			file.Close()
		// Send JSON response to the client.
		if errorr != nil{
			fmt.Print(errorr)
		}
	}else{//Find the peer to which the key should belong and call the lookup function of that peer remotely.
		req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(key_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
    	res := Response{}
    	resp := &Response{}
    	resp.Find_Successor(&req,&res)
    	if(res.Msg!=nil){
   		dest_node := &Node_Entry{}
		err := json.Unmarshal(res.Msg, &dest_node)
		fmt.Println("Looked up for successor of: " + strconv.Itoa(key_id) + "  Got back: " + strconv.Itoa(dest_node.NodeId))
		if err !=nil{
			fmt.Println(err)
		}
		req = *request
		res = Response{}
		connection, _ := jsonrpc.Dial(node_parameters.Protocol, dest_node.IpAddress+":"+strconv.Itoa(dest_node.Port))
		connection.Call("Response.Delete",req,&res)
		connection.Close()
		if(res.Msg!=nil){
			response.Msg=res.Msg
			}
		}
	}
 fmt.Println(errorr)
    return nil
}

// Return the successor of the node asked
func (t *Response) Return_Successor(request *Request, response *Response) error{
	if successor.NodeId != NIL {
			rsp_msg := Node_Entry{NodeId: successor.NodeId, IpAddress: successor.IpAddress, Port: successor.Port}
			response.Msg, _ = json.Marshal(rsp_msg)		
		}
	return nil
}

// Return the list of keys
func (t *Response) Return_Keys(request *Request, response *Response) error{
	var errorr error
	file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
	if errorr != nil{
		fmt.Print(errorr)
	}
	var keys string
	var res_str string
	defer file.Close()
	reader := bufio.NewReader(file)
	line,err_eof := reader.ReadString('\n')
	
	for i := 0; err_eof == nil; i++ {
		// Parse the argumets received line by line from the file.
		str_ctr := strings.Split(line[:len(line)-1]," ")
		keys = keys + str_ctr[1][:len(str_ctr[1])-1] + ", "
		line,err_eof = reader.ReadString('\n')
	}
	if keys!= "" {
		res_str = ("["+keys[:len(keys)-2]+"]")
	}
	// Send JSON response to the client.
	rsp_msg := To_Peer{Result: res_str, Error: errorr}
	response.Msg, errorr = json.Marshal(rsp_msg)
	if errorr != nil{
	fmt.Print(errorr)
	}
	return nil
}

// List all the keys in the system.
func (t *Response) ListKeys (request *Request, response *Response) error{
	var str string
	var errorr error
	succ_node := successor
	str = ""
	req := Request{}
    res := Response{}
    resp := &Response{}
    resp.Return_Keys(&req,&res)
	key_list := &To_Peer{}
	json.Unmarshal(res.Msg, key_list)
	
	str = str + key_list.Result
	
	
	for (succ_node.NodeId != node_id){
		connection, _ := jsonrpc.Dial(node_parameters.Protocol, succ_node.IpAddress+":"+strconv.Itoa(succ_node.Port))
		connection.Call("Response.Return_Keys",req,&res)
		connection.Close()
		key_list = &To_Peer{}
		json.Unmarshal(res.Msg, key_list)
		str = str + key_list.Result	
	
		connection, _ = jsonrpc.Dial(node_parameters.Protocol, succ_node.IpAddress+":"+strconv.Itoa(succ_node.Port))
		connection.Call("Response.Return_Successor",req,&res)
		connection.Close()
		dest_node := &Node_Entry{}
		errorr := json.Unmarshal(res.Msg, &dest_node)
		if errorr != nil{
		fmt.Print(errorr)
		}
		succ_node = *dest_node
	}
	
	// Send JSON response to the client.
	rsp_msg := To_Peer{Result: str, Error: errorr}
	response.Msg, errorr = json.Marshal(rsp_msg)
	if errorr != nil{
	fmt.Print(errorr)
	}
	return nil
}

// Return the list of ids from this node.
func (t *Response) Return_Ids(request *Request, response *Response) error{
	var errorr error
	file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
	if errorr != nil{
		fmt.Print(errorr)
	}
	var ids string
	var res_str string
	defer file.Close()
	reader := bufio.NewReader(file)
	line,err_eof := reader.ReadString('\n')
	
	for i := 0; err_eof == nil; i++ {
		// Parse the argumets received line by line from the file.
		str_ctr := strings.Split(line[:len(line)-1]," ")
		ids = ids + "[" + str_ctr[0][:len(str_ctr[0])-1] + ", " + str_ctr[1][:len(str_ctr[1])-1] + "], "
		line,err_eof = reader.ReadString('\n')
	}
	if ids!= "" {
		res_str = ("["+ids[:len(ids)-2]+"]")
	}
	// Send JSON response to the client.
	rsp_msg := To_Peer{Result: res_str, Error: errorr}
	response.Msg, errorr = json.Marshal(rsp_msg)
	if errorr != nil{
	fmt.Print(errorr)
	}
	return nil
}

// Return the list of ids in the system
func (t *Response) ListIds (request *Request, response *Response) error{
	var str string
	var errorr error
	succ_node := successor
	str = ""
	req := Request{}
    res := Response{}
    resp := &Response{}
    resp.Return_Ids(&req,&res)
	id_list := &To_Peer{}
	json.Unmarshal(res.Msg, id_list)
	
	str = str + id_list.Result
	
	
	for (succ_node.NodeId != node_id){
		connection, _ := jsonrpc.Dial(node_parameters.Protocol, succ_node.IpAddress+":"+strconv.Itoa(succ_node.Port))
		connection.Call("Response.Return_Ids",req,&res)
		connection.Close()
		id_list = &To_Peer{}
		json.Unmarshal(res.Msg, id_list)
		str = str + id_list.Result	
	
		connection, _ = jsonrpc.Dial(node_parameters.Protocol, succ_node.IpAddress+":"+strconv.Itoa(succ_node.Port))
		connection.Call("Response.Return_Successor",req,&res)
		connection.Close()
		dest_node := &Node_Entry{}
		errorr := json.Unmarshal(res.Msg, &dest_node)
		if errorr != nil{
		fmt.Print(errorr)
		}
		succ_node = *dest_node
	}
	
	// Send JSON response to the client.
	rsp_msg := To_Peer{Result: str, Error: errorr}
	response.Msg, errorr = json.Marshal(rsp_msg)
	if errorr != nil{
	fmt.Print(errorr)
	}
	return nil
}

func (t *Response) Update_Successor(request *Request, response *Response) error{
	new_successor := &Node_Entry{}
	err := json.Unmarshal(request.Msg, &new_successor)
	if err == nil{
		successor = *new_successor
	}
	return nil
}

func (t *Response) Update_Predecessor(request *Request, response *Response) error{
	new_predecessor := &Node_Entry{}
	err := json.Unmarshal(request.Msg, &new_predecessor)
	if err == nil{
		predecessor = *new_predecessor
	}
	return nil
}

// Insert entries got from the departing node
func (t *Response) Insert_Entries(request *Request, response *Response) error{
	var done bool
	var errorr error
	var file_handler *os.File
	done = false
	
	for done==false{
		file_handler , errorr = os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_WRONLY|os.O_APPEND,0777)
		if errorr != nil{
			amt := 2000 + time.Duration(rand.Intn(500))
			time.Sleep(time.Millisecond * amt)
		}else{
			done = true
		}
	}
	file_handler.WriteString(string(request.Msg))
	file_handler.Close()
	return nil
}

// Fucntion to shutdown the node.
func (t *Response) Shutdown(request *Request, response *Response) error{
	//Notify predecessor of the departure.
	if predecessor.NodeId != NIL{
		connection, _ := jsonrpc.Dial(node_parameters.Protocol, predecessor.IpAddress+":"+strconv.Itoa(predecessor.Port))
		req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(successor.NodeId) +  ", \"IpAddress\": " + "\"" + successor.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(successor.Port)  +"}")}
		res := Response{}
   	 	connection.Call("Response.Update_Successor",req,&res)
    	connection.Close()
	}
	
	//Notify successor of the departure
	connection, _ := jsonrpc.Dial(node_parameters.Protocol, successor.IpAddress+":"+strconv.Itoa(successor.Port))
	if successor.NodeId != NIL{
		req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(predecessor.NodeId) +  ", \"IpAddress\": " + "\"" + predecessor.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(predecessor.Port)  +"}")}
		res := Response{}
   	 	connection.Call("Response.Update_Predecessor",req,&res)
    	//connection.Close()
	}
	// Send the file line by line to the successor
		file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
		if errorr != nil{
			fmt.Print(errorr)
		}
		reader := bufio.NewReader(file)
		line,err_eof := reader.ReadString('\n')
		for err_eof == nil{
			req := Request{[]byte(line)}
			res := Response{}
   	 		connection.Call("Response.Insert_Entries",req,&res)
   	 		line,err_eof = reader.ReadString('\n')
		}
		file.Close()
		os.Remove(node_parameters.PersistentStorageContainer.File)
		connection.Close()	
	node_listener.Close()
	os.Exit(0)
	return nil
}

func integer_to_bytes(num int) (binnary []byte) {
    binnary = make([]byte, 4)
    binary.BigEndian.PutUint32(binnary, uint32(num))
    return
}

func get_id(Message string, num int) int {
   h := sha1.New()
   io.WriteString(h,Message)
   str := hex.EncodeToString(h.Sum(nil))
   h_int , _ := big.NewInt(0).SetString(str, 16)
   sze := new(big.Int)
   sze.SetBytes(integer_to_bytes(num))
   modd := new(big.Int)
   ID := modd.Mod(h_int, sze)
   return int(uint64(ID.Int64()))
}

// Get the hash key for the key relationship pair
func getKey(key_id int, key_size int, rel_id int, rel_size int) int{
   key_id_64 := int64(key_id)
   rel_id_64 := int64(rel_id)
   key_binary := strconv.FormatInt(key_id_64 , 2)
   key_binary = strings.Repeat("0",(key_size - len(key_binary))) + key_binary
   rel_binary := strconv.FormatInt(rel_id_64 , 2)
   rel_binary = strings.Repeat("0",(rel_size - len(rel_binary))) + rel_binary
   key_rel_sring := key_binary + rel_binary
   key_rel_id, err := strconv.ParseInt(key_rel_sring, 2, 64)
   if err != nil {
        fmt.Println(err)
    }
    return int(key_rel_id)
}

// To create a new chord ring
func create(){
	predecessor.NodeId = NIL
	
	//Referring to self
	successor.IpAddress = node_parameters.IpAddress
	successor.NodeId = node_id
	successor.Port = node_parameters.Port
	
}

// To join an existing chord ring
func join(){
	predecessor.NodeId = NIL
	connection, _ := jsonrpc.Dial(node_parameters.Protocol, node_parameters.PeerIpAddress+":"+strconv.Itoa(node_parameters.PeerPort))
	req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(node_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
    res := Response{}
    connection.Call("Response.Find_Successor",req,&res)
    if res.Msg != nil{
		responce_message := &Node_Entry{}
		json.Unmarshal(res.Msg, &responce_message)
		successor = *responce_message
		}
    connection.Close();
}

// Find the successor of the hash key
func (t *Response) Find_Successor(request *Request, response *Response) error{
	request_message := &Node_Entry{}
	err := json.Unmarshal(request.Msg, &request_message)
	if err !=nil{
		fmt.Println(err)
	}
	
		if (request_message.NodeId > node_id && request_message.NodeId <= successor.NodeId) || (node_id >= successor.NodeId && ((request_message.NodeId > node_id && request_message.NodeId < int(SIZE)) || (request_message.NodeId <= successor.NodeId && request_message.NodeId >= 0))){
		rsp_msg := Node_Entry{NodeId: successor.NodeId, IpAddress: successor.IpAddress, Port: successor.Port}
		response.Msg, _ = json.Marshal(rsp_msg)
		return nil
	}else{
		preceding_node := Closest_Preceding_Node(request_message.NodeId)
		if preceding_node.NodeId == node_id {
			rsp_msg := Node_Entry{NodeId: preceding_node.NodeId, IpAddress: preceding_node.IpAddress, Port: preceding_node.Port}
			response.Msg, _ = json.Marshal(rsp_msg)	
			return nil
		} else{
			connection, err := jsonrpc.Dial(node_parameters.Protocol, preceding_node.IpAddress+":"+strconv.Itoa(preceding_node.Port))
	   	 	if err==nil{
				req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(request_message.NodeId) +  ", \"IpAddress\": " + "\"" + request_message.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(request_message.Port)  +"}")}
		   	 	res := Response{}
	    		connection.Call("Response.Find_Successor",req,&res)
    			response.Msg = res.Msg
	    		connection.Close()
    		}else{
    			for i:=0;i<M;i++{
    				if finger[i].NodeId == preceding_node.NodeId{
    					finger[i].IpAddress = node_parameters.IpAddress
    					finger[i].NodeId = node_id
    					finger[i].Port = node_parameters.Port
    				}
    			}
    		}
	   	 	
    		return nil
    	}
	}
	return nil
	
}

// Look the finger table for the successor
func Closest_Preceding_Node(target_id int) Node_Entry{
	preceding_node := Node_Entry{NodeId: node_id, IpAddress: node_parameters.IpAddress, Port: node_parameters.Port}
	for i:=0;i<M;i++{
		j:=i+1
		if j == M{
			j=0
		}
		if finger[i].NodeId == -1 || finger[j].NodeId == -1{
			break
		}else{
if (finger[i].NodeId<target_id && finger[j].NodeId>=target_id) || (finger[i].NodeId > finger[j].NodeId  && ((target_id>finger[i].NodeId && target_id < int(SIZE)) || (target_id >= 0 && target_id <= finger[j].NodeId))){			
				preceding_node = finger[i]
				break
			}
		}
	}
	return preceding_node
}

// Get the predecessor of this node
func (t *Response) Return_Predecessor(request *Request, response *Response) error{
	if predecessor.NodeId != NIL {
			rsp_msg := Node_Entry{NodeId: predecessor.NodeId, IpAddress: predecessor.IpAddress, Port: predecessor.Port}
			response.Msg, _ = json.Marshal(rsp_msg)		
		}
	return nil
}

// To get to know about new nodes that a have joined.
func Stabilize (){
	for true {
		amt := 6000 + time.Duration(rand.Intn(500))
		time.Sleep(time.Millisecond * amt)
		connection, err := jsonrpc.Dial(node_parameters.Protocol, successor.IpAddress+":"+strconv.Itoa(successor.Port))
		if err == nil{
			req := Request{}
	   	 	res := Response{}
    		connection.Call("Response.Return_Predecessor",&req,&res)
    		connection.Close()
    		if(res.Msg!=nil){
   				preceding_node := &Node_Entry{}
				err := json.Unmarshal(res.Msg, &preceding_node)
				if err !=nil{
					fmt.Println(err)
				}
				if(preceding_node.NodeId > node_id && preceding_node.NodeId <= successor.NodeId) || (node_id >= successor.NodeId && (preceding_node.NodeId > node_id || preceding_node.NodeId < successor.NodeId)){
					successor = *preceding_node
				}
			}
    		connection, _ = jsonrpc.Dial(node_parameters.Protocol, successor.IpAddress+":"+strconv.Itoa(successor.Port))
			req = Request{[]byte("{\"NodeId\": " + strconv.Itoa(node_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
    		connection.Call("Response.Notify",req,&res)
    		connection.Close()
   		}
	}
}
	

func (t *Response) Notify (request *Request, response *Response) error{
	preceding_node := &Node_Entry{}
	err := json.Unmarshal(request.Msg, &preceding_node)
	if err !=nil{
		fmt.Println(err)
	}
	if(predecessor.NodeId == NIL) || ((preceding_node.NodeId > predecessor.NodeId && preceding_node.NodeId < node_id) || (node_id <= predecessor.NodeId && (preceding_node.NodeId > predecessor.NodeId || preceding_node.NodeId < node_id))){
		predecessor = *preceding_node
	}
	return nil
}

// To fix the finger table
func Fix_Fingers (){
	for true {
		amt := 7000 + time.Duration(rand.Intn(500))
	time.Sleep(time.Millisecond * amt)
	next++
	if next > M{
		next = 1
	}
	next_id := node_id + int(math.Pow(float64(2),float64(next-1)))
	if next_id >= int(SIZE){
		next_id = next_id - int(SIZE)
	}
	req := Request{[]byte("{\"NodeId\": " + strconv.Itoa(next_id) +  ", \"IpAddress\": " + "\"" + node_parameters.IpAddress + "\""  +  ", \"Port\": " + strconv.Itoa(node_parameters.Port)  +"}")}
    res := Response{}
    resp := &Response{}
    resp.Find_Successor(&req,&res)
    if(res.Msg!=nil){
   	response_message := &Node_Entry{}
	err := json.Unmarshal(res.Msg, &response_message)
	if err !=nil{
		fmt.Println(err)
	}
	fmt.Println("Asked for successor of: " + strconv.Itoa(next_id) + " for finger number " + strconv.Itoa(next) + "  Got back: " + strconv.Itoa(response_message.NodeId))
	finger[next-1] = *response_message
	
	}
    fmt.Println("Node Successor = " + strconv.Itoa(successor.NodeId))
    fmt.Println("Node Predecessor = " + strconv.Itoa(predecessor.NodeId))
    fmt.Println("Finger 1: " + strconv.Itoa(finger[0].NodeId))
    fmt.Println("Finger 2: " + strconv.Itoa(finger[1].NodeId))
    fmt.Println("Finger 3: " + strconv.Itoa(finger[2].NodeId))
    fmt.Println("Finger 4: " + strconv.Itoa(finger[3].NodeId))
    fmt.Println("Finger 5: " + strconv.Itoa(finger[4].NodeId))
	}
}

// Check if the predecessor is alive
func Check_Predecessor(){
	for true {
		time.Sleep(time.Second * 8)
		connection, error := jsonrpc.Dial(node_parameters.Protocol, predecessor.IpAddress+":"+strconv.Itoa(predecessor.Port))
		if error != nil {// The predecessor is Downn
			predecessor.NodeId = NIL
			predecessor.IpAddress = ""
			predecessor.Port = NIL
		}else{
			error = connection.Close()
		}
		//fmt.Println(error)
	}
}

// To remove the unused entriess from the database
func Purge(){
	for true {
	amt := 60000 + time.Duration(rand.Intn(500))
	time.Sleep(time.Millisecond * amt)
	var errorr error
		
		var line_buf bytes.Buffer
		file , errorr := os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_RDONLY,0777)
			if errorr != nil{
				fmt.Print(errorr)
			}
		reader := bufio.NewReader(file)
		line,err_eof := reader.ReadString('\n')
		for err_eof == nil{
			str_ctr := strings.SplitAfterN(line,",",3)
				
			file_value := &Value{}
			errorr = json.Unmarshal([]byte((str_ctr[2][:len(str_ctr[2])-1])), &file_value)
				
				if errorr !=nil{
					fmt.Println(errorr)
				}
			if file_value.Permission == "RW"{

				old_access_time_str := file_value.Accessed
				if old_access_time_str == ""{
					old_access_time_str = file_value.Modified
				}
				
				if old_access_time_str != ""{
					old_time,_ := time.Parse("02/01/2006, 15:04:05", old_access_time_str)
					delta := time.Since(old_time)
 					if (int(delta.Seconds()) - 14400) < 60{
 						line_buf.WriteString(line)
 					}
				}
			}else{
				line_buf.WriteString(line)
			}
			line,err_eof = reader.ReadString('\n')
			}
		
		file.Close()
		file , errorr = os.OpenFile(node_parameters.PersistentStorageContainer.File, os.O_WRONLY|os.O_APPEND,0777)
		file.Truncate(0)
		if errorr != nil{
			fmt.Print(errorr)
		}
		_,err := file.Write(line_buf.Bytes())
		if err != nil {
		    fmt.Println(err)
		}
		file.Close()
		if errorr != nil{
			fmt.Print(errorr)
		}
	}
}

// MAin function
func main(){
	res := new(Response)
	rpc.Register(res)
	start_node()
}