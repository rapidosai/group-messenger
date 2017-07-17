package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static final String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static int[] processSequencer = {0, 0, 0, 0, 0};
    static int[] processCounter = {0, 0, 0, 0, 0};
    static int deadProcId = -1;
    static AtomicInteger counter = new AtomicInteger(0);
    static final Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
    static String myPort;

    //Hold back message object tuple for the max sequence priority queue
    class HoldBackMessage {
        protected int mssgId;
        protected String mssg;
        protected int fromProcess;
        protected int propProcess;
        protected int propSeq;
        protected boolean deliverable;

        public HoldBackMessage(int mssgId, String mssg, int fromProcess, int propProcess, int propSeq, boolean deliverable) {
            this.mssg = mssg;
            this.mssgId = mssgId;
            this.fromProcess = fromProcess;
            this.propProcess = propProcess;
            this.propSeq = propSeq;
            this.deliverable = deliverable;
        }
    }

    //Hold back priority queue with comparator interface implementation to sort the queue
    static class HoldBackQueueComparator implements Comparator<HoldBackMessage> {
        public int compare(HoldBackMessage a, HoldBackMessage b) {
            //prioritizing messages with smaller sequence numbers
            if (a.propSeq < b.propSeq) return -1;
            else if (a.propSeq > b.propSeq) return 1;
                //In the event of tie
            else {
                //prioritizing undeliverable messages over deliverable
                if (!a.deliverable && b.deliverable) return -1;
                else if (a.deliverable && !b.deliverable) return 1;
                    //prioritizing messages with smaller process ID
                else {
                    if (a.propProcess < b.propProcess) return -1;
                    else if (a.propProcess > b.propProcess) return 1;
                    else return 0;
                }
            }
        }
    }

    HoldBackQueueComparator holdBackQueueComparator = new HoldBackQueueComparator();
    PriorityQueue<HoldBackMessage> mssgPriorityQueue = new PriorityQueue<HoldBackMessage>(5,holdBackQueueComparator);

    //Sequence object tuple with the process Id and its proposal for the max sequence priority queue
    static class ProposedSequenceWithProcessId {
        protected int proposedSeq;
        protected int procId;

        public ProposedSequenceWithProcessId(int proposedSeq, int procId) {
            this.proposedSeq = proposedSeq;
            this.procId = procId;
        }
    }

    //Max Sequence priority Queue with comparator interface implementation to sort the queue
    static class MaxSequenceComparator implements Comparator<ProposedSequenceWithProcessId> {
        public int compare(ProposedSequenceWithProcessId a, ProposedSequenceWithProcessId b) {
            //prioritizing messages with smaller sequence numbers
            if (a.proposedSeq > b.proposedSeq) return -1;
            else if (a.proposedSeq > b.proposedSeq) return 1;
                //In the event of tie
            else {
                //prioritizing undeliverable messages over deliverable
                if (a.procId < b.procId) return -1;
                else if (a.procId > b.procId) return 1;
                else return 0;
            }
        }
    }

    MaxSequenceComparator maxSequenceComparator = new MaxSequenceComparator();
    PriorityQueue<ProposedSequenceWithProcessId> maxseqPriorityQueue = new PriorityQueue<ProposedSequenceWithProcessId>(5,maxSequenceComparator);

    HashMap<Integer,ArrayList<ProposedSequenceWithProcessId>> msgMap = new HashMap<Integer, ArrayList<ProposedSequenceWithProcessId>>();

    static String[] multicastMssg(int mssgId, String mssg, int procId){
        String [] output = new String[3];
        output[0] = Integer.toString(mssgId);
        output[1] = mssg;
        output[2] = Integer.toString(procId);
        return output;
    }

    static int[] mssgSequence(int mssgId, int seq, int procId){
        int [] output = new int[3];
        output[0] = mssgId;
        output[1] = seq;
        output[2] = procId;
        return output;
    }

    static int[] maxSeqMssgMultiCast(int mssgId, int procId, int maxSeq, int maxProcId, int deadProc, int deadProcId){
        int [] output = new int[6];
        output[0] =  mssgId;
        output[1] = procId;
        output[2] = maxSeq;
        output[3] = maxProcId;
        output[4] = deadProc;
        output[5] = deadProcId;
        return output;
    }

    static ProposedSequenceWithProcessId mapMaxseq(ArrayList<ProposedSequenceWithProcessId> msgMapSeqList){
        int maxSeq = msgMapSeqList.get(0).proposedSeq;
        int maxSeqProcId = msgMapSeqList.get(0).procId;
        for(int i=1;i<msgMapSeqList.size();i++){
            if(msgMapSeqList.get(i).proposedSeq>msgMapSeqList.get(i-1).proposedSeq){
                maxSeq = msgMapSeqList.get(i).proposedSeq;
                maxSeqProcId = msgMapSeqList.get(i).procId;
            }
            else if(msgMapSeqList.get(i).proposedSeq<msgMapSeqList.get(i-1).proposedSeq){
                maxSeq = msgMapSeqList.get(i-1).proposedSeq;
                maxSeqProcId = msgMapSeqList.get(i-1).procId;
            }
            else{
                if(msgMapSeqList.get(i).procId<msgMapSeqList.get(i-1).procId){
                    maxSeq = msgMapSeqList.get(i).proposedSeq;
                    maxSeqProcId = msgMapSeqList.get(i).procId;
                }
                else{
                    maxSeq = msgMapSeqList.get(i-1).proposedSeq;
                    maxSeqProcId = msgMapSeqList.get(i-1).procId;
                }
            }
        }
        return new ProposedSequenceWithProcessId(maxSeq,maxSeqProcId);
    }

    static int [] maximumSequence(int propSeq, int procId, int currSeq, int currProcId) {
        int [] output = new int[2];
        if (propSeq > currSeq){
            output[0] = propSeq;
            output[1] = procId;
        }
        else {
            output[0] = currSeq;
            output[1] = currProcId;
        }
        return output;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        final EditText editText = (EditText) findViewById(R.id.editText1);

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            Socket sock = null;
            ContentResolver cr = getApplicationContext().getContentResolver();
            ContentValues keyValueToInsert = new ContentValues();
            int currProcId = 0;
            while (true) {
            try {
                sock = serverSocket.accept();
                if (sock.isConnected()) {
                    Log.e(TAG, "Client connection established");
                    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                    String [] multicastMssg = (String []) ois.readObject();
                    Log.d(TAG, "Multicast Message Received");
                    if (multicastMssg != null) {
                        for (int i = 0; i < 5; i++) {
                            if (remotePorts[i].equals(myPort)) {
                                processSequencer[i] = processSequencer[i]+1;
                                currProcId = i;
                                break;
                            }
                        }

                    int [] mssgSeq = mssgSequence(Integer.parseInt(multicastMssg[0]), processSequencer[currProcId], currProcId);
                    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                    Log.d(TAG, "Sending the proposed sequence to the client");
                    oos.writeObject(mssgSeq);
                    oos.flush();

                    for (HoldBackMessage hbm : mssgPriorityQueue) {
                        if (hbm.fromProcess == deadProcId) {
                            Log.d(TAG, "Deleting the deadprocid message in the PQ at step1");
                            mssgPriorityQueue.remove(hbm);
                        }
                    }

                    if(Integer.parseInt(multicastMssg[2]) != deadProcId) {
                        Log.d(TAG, "Putting the message in the PQ");
                        HoldBackMessage holdBackMessage = new HoldBackMessage(Integer.parseInt(multicastMssg[0]), multicastMssg[1], Integer.parseInt(multicastMssg[2]), currProcId, processSequencer[currProcId], false);
                        mssgPriorityQueue.add(holdBackMessage);
                    }

                    int[] maxSeqMssgMultiCast = (int[]) ois.readObject();
                    Log.d(TAG, "Received the max Seq from the client");
                    int[] finalSequence = maximumSequence(maxSeqMssgMultiCast[2], maxSeqMssgMultiCast[3], processSequencer[currProcId], currProcId);

                    String mssg = "";

                    for (HoldBackMessage hbm : mssgPriorityQueue) {
                        if (hbm.mssgId == maxSeqMssgMultiCast[0] && hbm.fromProcess == maxSeqMssgMultiCast[1]) {
                            mssg = hbm.mssg;
                            mssgPriorityQueue.remove(hbm);
                        }
                        if(maxSeqMssgMultiCast[4] == 1){
                            if (hbm.fromProcess == maxSeqMssgMultiCast[5]) {
                                Log.d(TAG, "Deleting the deadprocid message in the PQ at step2");
                                mssgPriorityQueue.remove(hbm);
                            }
                        }
                    }

                    HoldBackMessage finalMessage = new HoldBackMessage(maxSeqMssgMultiCast[0], mssg, maxSeqMssgMultiCast[1], finalSequence[1], finalSequence[0], true);
                    mssgPriorityQueue.add(finalMessage);

                    String finalmssg = mssgPriorityQueue.poll().mssg;


                    if(!finalmssg.isEmpty() && finalmssg != null){}
                    if(finalSequence != null){
                        keyValueToInsert.put("key", counter.toString());
                        keyValueToInsert.put("value", multicastMssg[1]);

                        counter.getAndIncrement();
                        Log.d(TAG, "Incremented Counter" + counter);

                        cr.insert(providerUri, keyValueToInsert);
                        Log.d(TAG, "Messages inserted into the Content Provider");
                        Log.d(TAG, finalmssg+" From:"+finalMessage.fromProcess+" Prop:"+finalMessage.propProcess);

                        publishProgress(multicastMssg[1]);
                    }

                    ois.close();
                    oos.close();
                    }
                    }
                    else {
                        Log.e(TAG, "Connection not established on Server");
                    }
                } catch (IOException e) {
                    Log.e(TAG, "Socket Connection closed on the server side");
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "Multicast Message class not found");
                }

            }
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                 /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                int currProcId = 0;
                ObjectOutputStream [] osList = new ObjectOutputStream[5];
                int [] mssgSeq = {0,0,0};
                Socket socket = null;
                ObjectInputStream ois = null;
                for (int j = 0; j < 5; j++) {
                    if (remotePorts[j].equals(myPort)) {
                        processCounter[j] = processCounter[j]+1;
                        currProcId = j;
                        break;
                    }
                }

                for (int i = 0; i < remotePorts.length; i++) {
                    if (deadProcId != i){
                        try{
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePorts[i]));
                            Log.d(TAG, "doInBackground: " + socket.isConnected());
                            String msgToSend = msgs[0];
                            String[] multicastMssg = multicastMssg(processCounter[currProcId], msgToSend, currProcId);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(multicastMssg);
                            Log.d(TAG, "Multicast Message Sent");
                            oos.flush();
                            osList[i] = oos;
                            ois = new ObjectInputStream(socket.getInputStream());
                            mssgSeq = (int []) ois.readObject();
                            Log.d(TAG, "Received the message sequence from the server");
                            ProposedSequenceWithProcessId proposedSequenceWithProcessId = new ProposedSequenceWithProcessId(mssgSeq[1], mssgSeq[2]);
                            maxseqPriorityQueue.add(proposedSequenceWithProcessId);

                            if(msgMap.containsKey(mssgSeq[0])){
                                ArrayList<ProposedSequenceWithProcessId> msgList = msgMap.get(mssgSeq[0]);
                                msgList.add(proposedSequenceWithProcessId);
                                msgMap.put(mssgSeq[0],msgList);
                            }
                            else{
                                ArrayList<ProposedSequenceWithProcessId> msgList = new ArrayList<ProposedSequenceWithProcessId>();
                                msgList.add(proposedSequenceWithProcessId);
                                msgMap.put(mssgSeq[0],msgList);
                            }
                        }
                        catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                            deadProcId = i;
                        }
                    }
                }

              Log.d(TAG, "Proposing the maximum sequence to the server");
                for (ObjectOutputStream os : osList) {
                    if (os != null) {
                        if(deadProcId != -1){
                            if (msgMap.get(mssgSeq[0]).size() > 3) {
                                ProposedSequenceWithProcessId maxPropSeq = mapMaxseq(msgMap.get(mssgSeq[0]));
                                int[] maxSeqMssgMultiCast = maxSeqMssgMultiCast(mssgSeq[0], currProcId, maxPropSeq.proposedSeq, maxPropSeq.procId, 1, deadProcId);
                                try {
                                    os.writeObject(maxSeqMssgMultiCast);
                                    os.flush();
                                    Log.d(TAG, "Sent Maximum sequence to the server");
                                }
                                catch (IOException e) {
                                    Log.e(TAG, "ClientTask socket IOException");
                                }
                            }
                        }
                        else{
                            if (msgMap.get(mssgSeq[0]).size() > 4) {
                                ProposedSequenceWithProcessId maxPropSeq = mapMaxseq(msgMap.get(mssgSeq[0]));
                                int[] maxSeqMssgMultiCast = maxSeqMssgMultiCast(mssgSeq[0], currProcId,  maxPropSeq.proposedSeq, maxPropSeq.procId, 0, deadProcId);
                                try {
                                    os.writeObject(maxSeqMssgMultiCast);
                                    os.flush();
                                    Log.d(TAG, "Sent Maximum sequence to the server");
                                }
                                catch (IOException e) {
                                    Log.e(TAG, "ClientTask socket IOException");
                                }
                            }
                        }

                    }
                }
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "Message Sequence class not found");
            }
            return null;
        }
    }
}