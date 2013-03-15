package chord;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.security.MessageDigest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Junaid
 * @date March 2013
 */
public class ChordNode {
    private DatagramSocket nodeSocket;
    
    
    // in ip:port format
    private String predecessor;
    private String successor;
    
    byte[] receiveData; 
    byte[] sendData ;
    
    int nodePort;
    private InetAddress nodeAddress; 
   
    static ChordNode chordNode;
        
    private InetAddress sendIPAddress;
    private int sendPort;
    
    // hash map to store key/value pair for current node
    private static Map<String,String> files = new HashMap<>();
    // hash map to store hash/ip of keys and thier owners
    private static Map<String,String> hashes = new HashMap<>();
    
    // constructor
    private ChordNode (int port ){
        
        nodePort = port;
        
      try{
         nodeSocket  = new DatagramSocket(port);
      }catch(Exception e){
         //e.print
      }
      
      try {
            // local ip address
         nodeAddress = InetAddress.getLocalHost();
         //nodeAddress = nodeAddress.getHostAddress();
      } catch (UnknownHostException ex) {
         Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
      }
      
      receiveData = new byte[1024];

      predecessor = null;
      successor = nodeAddress+":"+nodePort;

      
    }
    
    // get intance method 
    static public ChordNode getInstance(int port){
    
        if (chordNode == null){
            chordNode = new ChordNode(port);
        }
        return chordNode;
    }
    
    //getter methods
    public String predecessor(){
        return predecessor;
    }
    public String successor(){
        return successor;
    }
    
    /* join(address) -- address = ip:port
     * join request prototype = chord join ip port
     * 
     */
    public void join(String address) throws IOException{
        
        String tokenized[];
        
        tokenized = address.split(":");
        
        String joinCommand = "chord join"+" "+this.nodeAddress.getHostAddress()+" "+this.nodePort;
        
        System.out.println(tokenized[0]+":"+tokenized[1]);
        
        this.sendData = joinCommand.getBytes();
        
        DatagramPacket sendPacket = new DatagramPacket(this.sendData, this.sendData.length,
                                                                    InetAddress.getByName(tokenized[0]),Integer.parseInt(tokenized[1]));
             
        chordNode.nodeSocket.send(sendPacket);
        

    
    }
    
    /* notify(address of predecessor)
     * 
     */
    public void notify(String Address){
    
    
    
    }
    /*
     * add a key to DHT
     * 
     */
    public void addKey(String key, String value){
        
        this.files.put(key, value);
        
        this.addKeypart(key, this.nodeAddress+":"+this.nodePort);
        
        
        
    
    }
    
    private void addKeypart(String key, String address){
    
        String tokenized[] = this.successor.split(":"); 
        
        System.out.println("in add key part  -> "+tokenized[0].split("/")[1] +" "+tokenized[1]);
        
        String tokenized_y[] = address.split(":");
        
        String myHash = calculate_hash(this.nodeAddress.toString()+":"+this.nodePort);
        
        InetAddress successorAddress = null;
        try {
            successorAddress = InetAddress.getByName(tokenized[0].split("/")[1]); //receivePacket.getAddress();
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        }

        int successorPort = Integer.parseInt(tokenized[1]);//receivePacket.getPort();
        
        System.out.println(calculate_hash(key));
        System.out.println(myHash);
        
        String addKeyCommand = null;
        
        if (  ( (calculate_hash(key).compareTo(myHash) > 0)&&(calculate_hash(key).compareTo(calculate_hash(this.successor)) < 0 ) )|| myHash.compareTo(calculate_hash(this.successor)) >= 0){ //|| 
            
            System.out.println("key value pair has been put");
            //this.hashes.put(calculate_hash(key),address);
            
            //ask successor to keep the key 
            addKeyCommand = "keep key"+" "+key+" "+tokenized_y[0]+":"+tokenized_y[1];
            System.out.println("keep request sent to  "+ successorAddress +" port "+successorPort);
            // ask seccessor to add it to his table
            // this is yet to be done
            // also have to implement boundry condition
                
        }
        else{
            //initiate request to successor to find appropriate place for the key
            addKeyCommand = "add key"+" "+key+" "+tokenized_y[0]+":"+tokenized_y[1];
            System.out.println("add request sent to  "+ successorAddress +" port "+successorPort);
            
        }    
            
        
            this.sendData = addKeyCommand.getBytes();

            DatagramPacket sendPacket = new DatagramPacket(this.sendData, this.sendData.length,successorAddress,successorPort);

           

            try {
                this.nodeSocket.send(sendPacket);
            } catch (IOException ex) {
                Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
                ex.printStackTrace();
            }
        
            

    
    }
    
    // calculate hash function 
    public static String calculate_hash(String toBeHashed){

        MessageDigest md = null;
        try{
            md = MessageDigest.getInstance("SHA-1");
        }catch(Exception e){

            e.printStackTrace();
        }
        md.reset();
        md.update(toBeHashed.getBytes());
 
        byte byteData[] = md.digest();

        //md.ha
        //System.out.println(md.hashCode());
        //http://www.mkyong.com/java/java-sha-hashing-example/
        
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
         sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();

    }
    // printer method
    
    public void printKeyTable(){
    
        System.out.println("\nEntries in held by this node");
        System.out.println("Size : "+hashes.size());
        for(Iterator<String> it3 = hashes.keySet().iterator(); it3.hasNext();) {
            String key = it3.next();
            System.out.println(key +" : " + hashes.get(key));
        }
    
    
    }

    
    // listener class 
    private static class Listner implements Runnable {

    
    String[] tokenized;

      public void run (){

         chordNode = ChordNode.getInstance(0000);
         System.out.println("listening on port " + chordNode.nodePort);

         
         System.out.println("ip = " + chordNode.nodeAddress.getHostAddress());
         
         String nodeAddressHash = calculate_hash(chordNode.nodeAddress+":"+chordNode.nodePort);

         DatagramPacket receivePacket;
         String callerAddress;
         
         
         while(true){

            receivePacket = new DatagramPacket(chordNode.receiveData, chordNode.receiveData.length);
            try{
               chordNode.nodeSocket.receive(receivePacket);
            }catch(Exception e2){}
            
            String sentence = new String( receivePacket.getData(), 0, receivePacket.getLength());

            tokenized= sentence.split("\\s");
            
            System.out.println("@@Rawform received message  " + sentence);

            //# receiving join call
            if (tokenized[0].equals("chord") && tokenized[1].equals("join") ){
                    System.out.println("Join command received");

                   
                    InetAddress callerIPaddress = null;
                    try {
                        callerIPaddress = InetAddress.getByName(tokenized[2]); //receivePacket.getAddress();
                    } catch (UnknownHostException ex) {
                        ex.printStackTrace();
                    }
                    
                    int callerPort = Integer.parseInt(tokenized[3]);//receivePacket.getPort();

                    String callerNodeAddressHash = calculate_hash(callerIPaddress+":"+callerPort);
                    
                    System.out.println("caller ip in start of join" + callerIPaddress);

                       
                    if(( ( callerNodeAddressHash.compareTo(nodeAddressHash)>0 && callerNodeAddressHash.compareTo(calculate_hash(chordNode.successor())) < 0 ) )){
                       System.out.println("this is successor of caller node : call notify here 2nd cond");

                           // send address of successor to caller node
                           String notifyCaller = "make successor"+" "+(chordNode.successor().split(":"))[0] +" "+(chordNode.successor().split(":"))[1];
                           
                           //String notifyPredecessor = "change successor"+" "+callerIPaddress+":"+callerPort;
                           
                           //setting the predecessor -- caller node is successor of this node
                           chordNode.successor = callerIPaddress+":"+callerPort;
                           
                           chordNode.sendData = notifyCaller.getBytes();
                           
                           DatagramPacket sendPacket = new DatagramPacket(chordNode.sendData, chordNode.sendData.length,callerIPaddress,callerPort);
                            
                           System.out.println("callerIp "+ callerIPaddress +" port "+callerPort);
                           
                           try {
                                chordNode.nodeSocket.send(sendPacket);
                           } catch (IOException ex) {
                                Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
                                ex.printStackTrace();
                           }
                                                   
                                        
                   }
                    // me > my suc && caller > my.suc && 
                    else if (nodeAddressHash.compareTo(calculate_hash(chordNode.successor())) >= 0 && callerNodeAddressHash.compareTo(nodeAddressHash)>0 ) {

                        System.out.println("this is successor of caller node : call notify here 1st cond + this is last node");

                        // send address of successor to caller node
                        String notifyCaller = "make successor"+" "+(chordNode.successor().split(":"))[0] +" "+(chordNode.successor().split(":"))[1];

                        System.out.println("suc ip " + (chordNode.successor().split(":"))[0]);
                        //String notifyPredecessor = "change successor"+" "+callerIPaddress+":"+callerPort;

                        //setting the predecessor -- caller node is successor of this node
                        chordNode.successor = callerIPaddress+":"+callerPort;

                        chordNode.sendData = notifyCaller.getBytes();

                        DatagramPacket sendPacket = new DatagramPacket(chordNode.sendData, chordNode.sendData.length,callerIPaddress,callerPort);

                        System.out.println("callerIp "+ callerIPaddress +" port "+callerPort);

                        try {
                            chordNode.nodeSocket.send(sendPacket);
                        } catch (IOException ex) {
                            Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
                            ex.printStackTrace();
                        }



                    }
                    else if ( callerNodeAddressHash.compareTo(calculate_hash(chordNode.successor()))< 0  && nodeAddressHash.compareTo(calculate_hash(chordNode.successor()))>=0 ){

                        System.out.println("this is successor of caller node : call notify here 1st cond + this is last node");

                        // send address of successor to caller node
                        String notifyCaller = "make successor"+" "+(chordNode.successor().split(":"))[0] +" "+(chordNode.successor().split(":"))[1];

                        System.out.println("suc ip " + (chordNode.successor().split(":"))[0]);
                        //String notifyPredecessor = "change successor"+" "+callerIPaddress+":"+callerPort;

                        //setting the predecessor -- caller node is successor of this node
                        chordNode.successor = callerIPaddress+":"+callerPort;

                        chordNode.sendData = notifyCaller.getBytes();

                        DatagramPacket sendPacket = new DatagramPacket(chordNode.sendData, chordNode.sendData.length,callerIPaddress,callerPort);

                        System.out.println("callerIp "+ callerIPaddress +" port "+callerPort);

                        try {
                            chordNode.nodeSocket.send(sendPacket);
                        } catch (IOException ex) {
                            Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
                            ex.printStackTrace();
                        }


                    }
                   else {
                       
                       System.out.println("Forwarding to my successor");
                                           
                       //String forwardToSuccessor = "chord join"+" "+callerIPaddress+" "+callerPort;
                      
                       String forwardToSuccessor = "chord join"+" "+tokenized[2]+" "+callerPort;
                      
                       
                       chordNode.sendData = forwardToSuccessor.getBytes();
                       
                       System.out.println("caller ip  " + tokenized[2] );
                       
                       InetAddress sendIPAddress = null;
                       //((chordNode.successor()).split(":")[0]).substring(1)
                        try {
                            sendIPAddress = InetAddress.getByName(((chordNode.successor()).split(":")[0]));//.substring(1)
                        } catch (UnknownHostException ex) {
                            Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        int sendPort = Integer.parseInt((chordNode.successor()).split(":")[1]);
                       
                       
                       
                           
                        DatagramPacket sendPacket = new DatagramPacket(chordNode.sendData, chordNode.sendData.length,sendIPAddress,sendPort);

                        System.out.println("sendIp "+  sendIPAddress +" port "+ sendPort);

                        try {
                                chordNode.nodeSocket.send(sendPacket);
                        } catch (IOException ex) {
                                Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
                                ex.printStackTrace();
                        }

                       
                   }

            }
            
            // receiving makeMeSuccessor Command
            if (tokenized[0].equals("make") && tokenized[1].equals("successor")){
                System.out.println("make successor command received from " + receivePacket.getAddress() +":"+receivePacket.getPort());
                                             
                chordNode.successor =  tokenized[2]+":"+tokenized[3] ;//receivePacket.getAddress()+":"+receivePacket.getPort();            
            }
            // receiving make predecessor Command -- not used 
            if (tokenized[0].equals("make") && tokenized[1].equals("predecessor")){
                System.out.println("make predecessor command received from " + receivePacket.getAddress() +":"+receivePacket.getPort());
                
                chordNode.predecessor = receivePacket.getAddress()+":"+receivePacket.getPort();            
            }
            // changing successor -- not used 
            if (tokenized[0].equals("change") && tokenized[1].equals("successor")){
                System.out.println("change successor command received from " + receivePacket.getAddress() +":"+receivePacket.getPort());
                
                chordNode.successor = tokenized[2]+":"+tokenized[3];            
            }
            
            //receiving add key request
            if (tokenized[0].equals("add") && tokenized[1].equals("key")){
                
                System.out.println("Add key command received");
                chordNode.addKeypart(tokenized[2], tokenized[3]);
                              
            }
            
            // receiving command to put the key to my hashmap
            if (tokenized[0].equals("keep") && tokenized[1].equals("key")){
            
                chordNode.hashes.put(calculate_hash(tokenized[2]),tokenized[3]);
            
            }
            

         }

      }

   }
    
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        
        
        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        String input = null; 
        
        //creating node
        System.out.println("Enter a port for this node");
        
        input = inFromUser.readLine();
        chordNode = ChordNode.getInstance(Integer.parseInt(input)); 
        
        System.out.println("Node created");
        
        Thread listener1 = new Thread (new Listner());
        listener1.start();

        while (true){
        
            System.out.println("Enter ip:port to join network ");
            
            input = inFromUser.readLine();     
            
            if (input.equals("p")){
                System.out.println( chordNode.predecessor() );
            }
            else if (input.equals("s")){
                System.out.println(chordNode.successor());
            }
            else if (input.contains(":")) {
                chordNode.join(input);
            }
            else if (input.split("\\s")[0].equals("add")){
                
                chordNode.addKey(input.split("\\s")[1], input.split("\\s")[2]);
            
            }
            else if (input.equals("print")){
            
                chordNode.printKeyTable();
            }
      
        }
        
        
    }
}
