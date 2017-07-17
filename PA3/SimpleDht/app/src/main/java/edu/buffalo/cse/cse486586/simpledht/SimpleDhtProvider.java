package edu.buffalo.cse.cse486586.simpledht;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.FileNameMap;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static final String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static String myPort;
    static final Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    static final String sharedPrefPath = "/data/data/edu.buffalo.cse.cse486586.simpledht/shared_prefs";
    TreeMap<String,String> chordTreeMap = new TreeMap<String, String>();

    public class Node{
        String port;
        String hashvalue;
        public Node(String port, String hashvalue){
            this.port = port;
            this.hashvalue = hashvalue;
        }
    }

    class NodeComparator implements Comparator<Node> {
        public int compare(Node a, Node b) {
            return a.hashvalue.compareTo(b.hashvalue);
        }
    }

    NodeComparator nodeComparator = new NodeComparator();
    PriorityQueue<Node> chordQueue = new PriorityQueue<Node>(2,nodeComparator);

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        File fileList = new File(sharedPrefPath);
        if (fileList.exists()){
            File[] filenames = fileList.listFiles();
            if(filenames != null){
                for (File sharedprefFiles : filenames){
                    sharedprefFiles.delete();
                }
            }
        }
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.v("Contentvalues",values.toString());
        try {
            for(String key:chordTreeMap.keySet()){
                Log.v(TAG, "Printing all the nodes in the chord");
                Log.v(TAG, "Order:"+key+" - "+chordTreeMap.get(key));
            }
            new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,values.get("key").toString(),values.get("value").toString(), myPort).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        try {
            new LivenessCheckClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort).get();
        } catch (InterruptedException e) {
            Log.e(TAG, "Interrupted while executing the client task");
        } catch (ExecutionException e) {
            Log.e(TAG, "Exception while executing the client task");
        }
        return true;
    }

    @Override
    public MatrixCursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                              String sortOrder) {
        String[] columnNames = {"key","value"};
        MatrixCursor cursor =  new MatrixCursor(columnNames);
        if(selection.equals("@")) {
            File fileList = new File(sharedPrefPath);
            if (fileList.exists()){
                File[] filenames = fileList.listFiles();
                if(filenames != null){
                    for (File sharedprefFiles : filenames){
                        String shrdprfFile = fileNameWithoutExtension(sharedprefFiles.getName().toString());
                        SharedPreferences sp = getContext().getSharedPreferences(shrdprfFile,Context.MODE_PRIVATE);
                        String value = sp.getString("value","");
                        cursor.addRow(new String[] {shrdprfFile,value});
                        Log.v("query", selection);
                        Log.v("value ", value);
                    }
                }

            }
            cursor.moveToFirst();
        }
        else if(selection.equals("*")){
            try {
                cursor = new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort).get();
            } catch (InterruptedException e) {
                Log.e(TAG, "Interrupted while executing the QueryClient task");
            } catch (ExecutionException e) {
                Log.e(TAG, "Exception while executing the QueryClient task");
            }
        }
        else{
            try{
                int chordPos = 0;
                String headNode = "";
                for(String key:chordTreeMap.keySet()){
                    Log.v(TAG,"Looping inside query for key: "+ selection);
                    chordPos++;
                    if(chordPos == 1) headNode = chordTreeMap.get(key);
                    String hashkey = genHash(selection);
                    if (hashkey.compareTo(key) <= 0){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordTreeMap.get(key)));
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        String [] query = new String[2];
                        query[0] = "Key Query";
                        query[1] = selection;
                        oos.writeObject(query);
                        oos.flush();

                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        String[] queryResult = (String []) ois.readObject();
                        cursor.addRow(new String[] {queryResult[0],queryResult[1]});
                        Log.v(TAG,"Value obtained for key "+queryResult[0]+" is "+ queryResult[1]);
                        break;
                    }
                    else{
                        if (chordPos == chordTreeMap.size()) {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(headNode));
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            String [] query = new String[2];
                            query[0] = "Key Query";
                            query[1] = selection;
                            oos.writeObject(query);
                            oos.flush();

                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            String[] queryResult = (String []) ois.readObject();
                            cursor.addRow(new String[] {queryResult[0],queryResult[1]});
                            Log.v(TAG,"Value obtained for key "+queryResult[0]+" is "+ queryResult[1]);
                            break;
                        }

                    }

                }
            }catch (UnknownHostException e) {
                Log.e(TAG, "Message Sequence class not found");
            }
            catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String[] mssgToStore(String header, String key, String val, String port){
        String[] output = new String[4];
        output[0] = header;
        output[1] = key;
        output[2] = val;
        output[3] = port;
        return output;
    }

    private String fileNameWithoutExtension(String fileName){
        if (fileName.indexOf(".") > 0)
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
        return fileName;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            ContentResolver cr = getContext().getContentResolver();
            while (true) {
                try {
                    Socket sock = serverSocket.accept();
                    if (sock.isConnected()) {
                        Log.v(TAG, "Client connection established");
                        ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                        String [] msg = (String []) ois.readObject();
                        if(msg[0].equals("Alive")){
//                            boolean nodeInChord = false;
//                            for(Node node:chordQueue){
//                                if(node.port.equals(msg[1])){
//                                    nodeInChord = true;
//                                    break;
//                                }
//                            }
//                            if(!nodeInChord){
//                                chordQueue.add(new Node(msg[1],genHash(Integer.toString(Integer.parseInt(msg[1])/2))));
//                                Log.v(TAG, msg[1]+" is added to the chord");
//                            }
                            chordTreeMap.put(genHash(Integer.toString(Integer.parseInt(msg[1])/2)),msg[1]);
                            Log.v(TAG, msg[1]+" is added to the chord");
                            String liveNode = myPort;
                            ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                            oos.writeObject(liveNode);
                            oos.flush();
                        }
                        else if(msg[0].equals("Query")){
                            Cursor cursor = cr.query(providerUri,null,"@",null,null);
                            ArrayList<String> keys = new ArrayList<String>();
                            ArrayList<String> values = new ArrayList<String>();
                            if(cursor != null){
                                if(cursor.moveToFirst()){
                                    do{
                                        keys.add(cursor.getString(cursor.getColumnIndex("key")));
                                        values.add(cursor.getString(cursor.getColumnIndex("value")));
                                        Log.v(TAG, cursor.toString()+" is added to the cursor");
                                    }while(cursor.moveToNext());
                                }
                            }
                            ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                            oos.writeObject(keys);
                            oos.writeObject(values);
                            oos.flush();
                        }
                        else if(msg[0].equals("Insert")){
                            SharedPreferences sp =  getContext().getSharedPreferences(msg[1], Context.MODE_PRIVATE);
                            sp.edit().putString("value",msg[2]).commit();
                            Log.d(TAG, msg[1]+ " inserted into the node:"+myPort);
                            String ack = "Mssg Inserted";
                            ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                            oos.writeObject(ack);
                            oos.flush();
                        }
                        else if(msg[0].equals("Key Query")){
                            SharedPreferences sp = getContext().getSharedPreferences(msg[1],Context.MODE_PRIVATE);
                            String value = sp.getString("value","");

                            String[] queryResult = new String[2];
                            queryResult[0] = msg[1];
                            queryResult[1] = value;
                            ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                            oos.writeObject(queryResult);
                            oos.flush();
                            Log.v("query", msg[1]);
                            Log.v("value ", value);
                        }
                    } else {
                        Log.e(TAG, "Connection not established on Server");
                    }
                } catch (IOException e) {
                    Log.e(TAG, "Socket Connection closed on the server side");
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "Class not found");
                }
                catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class LivenessCheckClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            for (int i = 0; i < remotePorts.length; i++) {
                try{
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePorts[i]));
                    Log.d(TAG, "doInBackground: " + socket.isConnected());
                    String[] livenessCheck = new String[2];
                    livenessCheck[0] = "Alive";
                    livenessCheck[1] = myPort;
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(livenessCheck);
                    oos.flush();

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    String liveNode = (String) ois.readObject();
//                    boolean nodeInChord = false;
//                    for(Node node:chordQueue){
//                        if(node.port.equals(liveNode)){
//                            nodeInChord = true;
//                            break;
//                        }
//                    }
//                    if(!nodeInChord){
//                        chordQueue.add(new Node(liveNode,genHash(Integer.toString(Integer.parseInt(liveNode)/2))));
//                        Log.v(TAG, liveNode+" is added to the chord");
//                    }
                    chordTreeMap.put(genHash(Integer.toString(Integer.parseInt(liveNode)/2)),liveNode);
                    Log.v(TAG, liveNode+" is added to the chord");
                    socket.close();
                    Log.v(TAG, remotePorts[i]+" socket is closed");
//                    if(ack.equals("OK")){
//                        socket.close();
//                        Log.v(TAG, remotePorts[i]+" socket is closed");
//                    }
                }
                catch (IOException e) {
                    Log.e(TAG, "Port "+remotePorts[i]+" is down");
                }catch (NullPointerException e){
                    Log.e(TAG, "Port "+remotePorts[i]+" is down");
                }catch (ClassNotFoundException e) {
                    Log.e(TAG, "Class not found");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }


    private class InsertClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                int chordpos = 0;
//                Log.v(TAG,"Head Port "+chordQueue.peek().port);
                String headNode = "";
                Log.v(TAG,"Chord Size"+chordTreeMap.size());
//                for(Node node:chordQueue) {
                for(String key:chordTreeMap.keySet()) {
                    chordpos++;
                    if(chordpos == 1) headNode = chordTreeMap.get(key);
                    Log.v(TAG,"Chord Position "+chordpos);
                    String msgkey = msgs[0];
                    String msgValue = msgs[1];
                    String hashkey = genHash(msgkey);
                    if (hashkey.compareTo(key) <= 0) {
                        Log.v(TAG,"Inside If");
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordTreeMap.get(key)));
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        String[] msgtoStore = mssgToStore("Insert",msgkey, msgValue, chordTreeMap.get(key));
                        oos.writeObject(msgtoStore);

                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        String ack = (String) ois.readObject();
                        if (ack.equals("Mssg Inserted")) {
                            socket.close();
                            Log.v(TAG,chordTreeMap.get(key)+ " socket is closed");
                        }
                        break;
                    } else {
                        if (chordpos == chordTreeMap.size()) {
                            Log.v(TAG,"Inside Else");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(headNode));
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            String[] msgtoStore = mssgToStore("Insert",msgkey, msgValue,headNode);
                            oos.writeObject(msgtoStore);

                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            String ack = (String) ois.readObject();
                            if (ack.equals("Mssg Inserted")) {
                                socket.close();
                                Log.v(TAG,chordTreeMap.get(key)+ " socket is closed");
                            }
                            break;
                        }
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "Message Sequence class not found");
            }
            catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class QueryClientTask extends AsyncTask<String, Void, MatrixCursor> {
        @Override
        protected MatrixCursor doInBackground(String... msgs) {
            String[] columnNames = {"key","value"};
            MatrixCursor cursor =  new MatrixCursor(columnNames);
            try{
                for(String key:chordTreeMap.keySet()) {
//                for(Node node:chordQueue){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordTreeMap.get(key)));
                    String[] msg = new String[1];
                    msg[0] = "Query";
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    ArrayList<String > keys = (ArrayList<String>) ois.readObject();
                    ArrayList<String > values = (ArrayList<String>) ois.readObject();
                    Log.v(TAG,"* Query list:");
                    for(int i=0;i<keys.size();i++){
                        cursor.addRow(new String[] {keys.get(i),values.get(i)});
                        Log.v(TAG,keys.get(i)+":"+values.get(i));
                    }
                }
                cursor.moveToFirst();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return cursor;
        }
    }
}
