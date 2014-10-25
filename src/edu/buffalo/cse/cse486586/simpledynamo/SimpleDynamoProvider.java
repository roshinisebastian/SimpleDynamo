package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	// Static List containing details of all the nodes in the Ring
	ArrayList<DynamoNode> nodeList = new ArrayList<DynamoNode>();

	public static final Uri CONTENT_URI = Uri
			.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	// Cursor cursor;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static String myPort;
	static String myAvd;
	static final int MAX = 5;
	static boolean isRecoveryDone = true;

	// String of ports
	String[] port = { "11112", "11108", "11116", "11120", "11124" };
	// String of avds
	String[] avds = { "5556", "5554", "5558", "5560", "5562" };
	// Defines the SimpleDhtDatabaseHelper name
	private SimpleDynamoDatabaseHelper sHelper = null;
	private String nodesToReplicateFrom[], nodesToBeReplicated[];

	// Defines the db
	private SQLiteDatabase db;

	// Defines the node_id
	private static String NODE_ID;

	@Override
	public boolean onCreate() {
		Log.d(TAG, "in onCreate");
		// Creates a new helper object without actually opening or creating the
		// database
		// until SQLiteOpenHelper.getWritableDatabase is called

		sHelper = new SimpleDynamoDatabaseHelper(getContext()); // the application context
		db = sHelper.getWritableDatabase();
		Log.d("Created instance of DB", db.toString());

		// Call method to recover from nodes
		Log.d(TAG, "DB is created");
		// Code to obtain the AVD and Port of the emulator
		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		myAvd = portStr;
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			NODE_ID = genHash(myAvd);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, e.toString());
			e.printStackTrace();
		}
		//Log.d("In SimpleDhtProvide of avd: ", myAvd);
		//Log.d("In SimpleDhtProvide of avd with PortNumber: ", myPort);
		//Log.d("MyHash: ", NODE_ID);

		// Create a static list to hold all the nodes
		nodeList = createList();
		// Call to RecoverTask to send messages to other nodes for recovering
		// values
		if (!sHelper.getIsDBCreated()) {
			isRecoveryDone = false;
			Log.d(TAG, "DB is already created");
			nodesToBeReplicated = new String[2];
			nodesToReplicateFrom = new String[2];
			populateReplicationNodes(nodeList, nodesToReplicateFrom,
					nodesToBeReplicated);
			for (int j = 0; j < 2; j++) {
				Log.d("BEFORE CALL TO RECOVER", "nodesToReplicateFrom[" + j
						+ "]=" + nodesToReplicateFrom[j]);
				Log.d("BEFORE CALL TO RECOVER", "nodesToBeReplicated[" + j
						+ "]=" + nodesToBeReplicated[j]);
			}
			// recoverNode();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"RECOVER");
			Log.d("Enter Spinning","isRecoveryDone="+isRecoveryDone);
			/*while(!isRecoveryDone)
			{
				//Keep spinning till recovery is done
			}*/
			Log.d("Exit Spinning","isRecoveryDone="+isRecoveryDone);
		}
		// Creating a ServerTask to listen for incoming messages
		try {
			/*
			 * Create a server socket as well as a thread (AsyncTask) that
			 * listens on the server port.
			 */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,
					serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
		// Call to ClientTask
		// new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN",
		// myPort);

		return false;
	}


	// Method to create the static list with the details of all nodes
	// predecessor and successor
	public ArrayList<DynamoNode> createList() {
		ArrayList<DynamoNode> nList = new ArrayList<DynamoNode>();
		DynamoNode node = new DynamoNode();
		String nodeHash;
		int next, prev;
		try {
			for (int i = 0; i < MAX; i++) {
				node = new DynamoNode();
				next = (i + 1) % MAX;
				prev = ((MAX - 1) + i) % MAX;
				System.out.println("Value of i=" + i + ",next=" + next
						+ ",prev=" + prev);
				node.setNode_avd(avds[i]);
				node.setNode_port(port[i]);
				nodeHash = genHash(avds[i]);
				node.setNode_id(nodeHash);
				Log.d("My port=", node.getNode_port());
				Log.d("My avd=", node.getNode_avd());
				Log.d("My hashValue=", node.getNode_id());

				// Setting the next node
				node.setNext_node_avd(avds[next]);
				node.setNext_node_port(port[next]);
				nodeHash = genHash(avds[next]);
				node.setNext_node_id(nodeHash);
				Log.d("Next port=", node.getNext_node_port());
				Log.d("Next avd=", node.getNext_node_avd());
				Log.d("Next hashValue=", node.getNext_node_id());

				// Setting the prev node
				node.setPrevious_node_avd(avds[prev]);
				node.setPrevious_node_port(port[prev]);
				nodeHash = genHash(avds[prev]);
				node.setPrevious_node_id(nodeHash);
				Log.d("Prev port=", node.getPrevious_node_port());
				Log.d("Prev avd=", node.getPrevious_node_avd());
				Log.d("Prev hashValue=", node.getPrevious_node_id());
				nList.add(node);
			}
			Log.d("CREATELIST: ", nList.toString());
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			Log.e("CREATELIST","Exception!!"+e.toString());
		}

		return nList;
	}

	//
	public void populateReplicationNodes(ArrayList<DynamoNode> nodeList,
			String[] nodesToReplicateFrom, String[] nodesToBeReplicated) {
		int i = 0, myIndex = -1, k = 0;
		Log.d("POPULATEREPLICATIONNODES", "in here");
		// Fetching myIndex
		for (i = 0; i < MAX; i++) {
			if (nodeList.get(i).getNode_port().equals(myPort)) 
			{
				myIndex = i;
				Log.d("POPULATEREPLICATIONNODES", "I am found at index="+ myIndex);
				break;
			}
		}
		if (myIndex != -1) {
			i = myIndex + 1;
			k = myIndex - 1;
			for (int j = 0; j < 2; j++) {
				Log.d("POPULATEREPLICATIONNODES", "val of i =" + i);
				Log.d("POPULATEREPLICATIONNODES", "+val of k =" + k);
				nodesToReplicateFrom[j] = nodeList.get(i % MAX).getNode_port();
				if (k < 0) {
					k = MAX + k;
				}
				nodesToBeReplicated[j] = nodeList.get(k % MAX).getNode_port();
				i++;
				k--;
			}

		}
		for (int j = 0; j < 2; j++) {
			System.out.println();
			Log.d("POPULATEREPLICATIONNODES", "nodesToReplicateFrom[" + j
					+ "]=" + nodesToReplicateFrom[j]);
			System.out.println();
			Log.d("POPULATEREPLICATIONNODES", "nodesToBeReplicated[" + j + "]="
					+ nodesToBeReplicated[j]);
		}

	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// Code to delete the data from the
		int rowNum = 0;
		String msgToSend, keyHash;
		if (selection.equals("@")) {
			rowNum = db.delete(SimpleDynamoDatabase.TABLE, "1", null);
			Log.d("DELETE","Delete All Rows. Number of Rows deleted: "+Integer.toString(rowNum));
		} else if (selection.equals("*")) {
			deleteAllData();
		} else {
			try {
				keyHash = genHash(selection);
				msgToSend = "DELETEWITHKEY" + "~" + selection;
				int index = lookUp(keyHash);
				Log.d("DELETE","Delete With Key. key to be deleted: "+selection);
				sendMessage(nodeList.get(index).getNode_port(), msgToSend);
				index++;
				sendMessage(nodeList.get(index % MAX).getNode_port(), msgToSend);
				index++;
				sendMessage(nodeList.get(index % MAX).getNode_port(), msgToSend);
			} catch (NoSuchAlgorithmException e) {
				Log.e("DELETEWITHKEY","Exception!!"+e.toString());
			}
		}
		if (rowNum > 0) {
			getContext().getContentResolver().notifyChange(uri, null);
		}
		return rowNum;
	}

	public void deleteAllData() {

		String msgToSend = "DELETEALL";
		for (int i = 0; i < MAX; i++) {
			Log.d("DELETEALL","Sending to port="+nodeList.get(i).getNode_port());
			sendMessage(nodeList.get(i).getNode_port(), msgToSend);

		}
	}

	public void deleteAll() {
		int rowNum = db.delete(SimpleDynamoDatabase.TABLE, "1", null);
		Log.i("DELETEALL","Number of Rows deleted: "+Integer.toString(rowNum));
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// Code to insert key value pair
		Long timeStamp = 0L;
		// Standalone node
		String keyHash = null, key, value;
		String msgToSend;
		boolean insertInMe = false;
		key = (String) values.get("key");
		value = (String) values.get("value");
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e1) {
			Log.e("INSERT","Exception!!"+e1.toString());
		}
		Log.d("INSERT","Key: "+key);
		Log.d("INSERT","Key: "+key+" Value: "+value);
		//Log.d("INSERT","Key: "+key+" Key Hash Value: "+keyHash);
		int index = lookUp(keyHash);
		String mainPort = nodeList.get(index).getNode_port();
		if (index == -1) {
			Log.d("INSERT","Index returned is -1");
		} else
			Log.d("INSERT","Index returned is : "+Integer.toString(index));
		// send message to node and its 2 successors
		timeStamp = System.currentTimeMillis();
		msgToSend = "INSERT" + "~" + key + "~" + value + "~"+ nodeList.get(index).getNode_port() + "~"+ timeStamp.toString();
		for(int i=0;i<3;i++)
		{
			if(nodeList.get(index%MAX).getNode_port().equals(myPort))
			{
				insertInMe = true;
			}
			else
			{
				sendMessage(nodeList.get(index% MAX).getNode_port(), msgToSend);
				Log.d("INSERT","Key: "+key+" Index returned is : "+Integer.toString(index%MAX));
			}
			index++;
		}
		if(insertInMe==true)
		{
			ContentValues cv = new ContentValues();
			cv.put("key",key);
			cv.put("value", value);
			cv.put("port", mainPort);
			cv.put("version", timeStamp);
			Long rowId = db.insertWithOnConflict(SimpleDynamoDatabase.TABLE, null, cv,SQLiteDatabase.CONFLICT_REPLACE);
			Log.d("INSERTINME","Key: "+key+" Inserted in me rowId : "+Long.toString(rowId));
		}
		
		/*Log.d("INSERT","Key: "+key+" Index now is : "+Integer.toString(index));
		sendMessage(nodeList.get(index % MAX).getNode_port(), msgToSend);
		index++;
		Log.d("INSERT","Key: "+key+"Index again is : "+Integer.toString(index));
		sendMessage(nodeList.get(index % MAX).getNode_port(), msgToSend);*/
		return uri;
	}

	public int lookUp(String keyHash) {
		for (int i = 0; i < MAX; i++) {
			if (nodeList.get(i).getPrevious_node_id().compareTo(keyHash) < 0
					&& nodeList.get(i).getNode_id().compareTo(keyHash) >= 0
					&& nodeList.get(i).getPrevious_node_id()
					.compareTo(nodeList.get(i).getNode_id()) < 0) {
				//Log.d("I am in 1st if: value of i", Integer.toString(i));
				return i;
			} else {
				if (nodeList.get(i).getPrevious_node_id()
						.compareTo(nodeList.get(i).getNode_id()) > 0
						&& (keyHash.compareTo(nodeList.get(i).getNode_id()) < 0 || keyHash
								.compareTo(nodeList.get(i).getNode_id()) > 0)) {
				//	Log.d("I am in 1st else: value of i", Integer.toString(i));
					return i;
				}
			}
		}
		return (-1);
	}

	public void sendMessage(String port, String message) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2 }), Integer.parseInt(port));
			ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
			outputStream.writeObject(message);
			socket.close();
		} catch (NumberFormatException e) {
			Log.e("SENDMESSAGE","Exception!!"+e.toString());
		} catch (UnknownHostException e) {
			Log.e("SENDMESSAGE","Exception!!"+e.toString());
		} catch (IOException e) {
			Log.e("SENDMESSAGE","Exception!!"+e.toString());
		}

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// Code to query the nodes
		Integer m;
		Cursor cursor = null;
		String[] columnsToReturn = { "key", "value" };
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE);
		QueryResult qResult = new QueryResult();
		MatrixCursor mCursor = new MatrixCursor(new String[] {SimpleDynamoDatabase.COLUMN_KEY,SimpleDynamoDatabase.COLUMN_VALUE });
		if (selection.equals("@")) 
		{
			while(!isRecoveryDone)
			{
				//Spin loop
			}
			Log.d("query with @ ", selection);
			cursor = queryBuilder.query(db, columnsToReturn, null, null, null,null, sortOrder);
			cursor.setNotificationUri(getContext().getContentResolver(), uri);
			m = cursor.getCount();
			if (m == 0)
				Log.d("@:I am null", m.toString());
			else
				Log.d("@: Count returned from cursor: ", m.toString());
			cursor.moveToFirst();
			while (!cursor.isAfterLast()) {
				Log.d("QUERY@", "key=" + cursor.getString(0));
				Log.d("QUERY@", "value=" + cursor.getString(1));
				cursor.moveToNext();
			}
			return cursor;
		} else if (selection.equals("*")) {
			Log.d("query with * ", selection);
			cursor = queryAllNodes();
			Log.d("In query count of cursor returned from queryAllNodes",Integer.toString(cursor.getCount()));
		} else {
			Log.d("query with key ", selection);
			mCursor = queryWithKey(selection);
			//Log.d("query with key. Val returned for key =", qResult.toString());
			//Log.d("Before if!", qResult.getResults().toString());
			if (!(mCursor.getCount() == 0)) {
				return mCursor;
			} else {
				Log.d("I am in else..", qResult.toString());
			}

		}
		return cursor;
	}
	
	public QueryResult recoverValues(String port) {
		Log.v("RECOVERVALUES", "RECOVERVALUES");
		String[] columnsToReturn = { "key", "value", "version", "port"};
		QueryResult qResult = new QueryResult();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		Cursor cursor;
		Integer m;
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE);
		queryBuilder.appendWhere(SimpleDynamoDatabase.COLUMN_PORT + " = " + "'"+ port + "'");
		cursor = queryBuilder.query(db, columnsToReturn, null, null, null,null, null);
		cursor.setNotificationUri(getContext().getContentResolver(),CONTENT_URI);
		m = cursor.getCount();
		int count = 1;
		Log.d("RECOVERVALUES", "cursor count =" + m.toString());
		if (m == 0)
			Log.d("RECOVERVALUES", "cursor count =" + m.toString() + "I am null");
		else 
		{
			Log.d("RECOVERVALUES", "cursor count = " + m.toString());
			int keyIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_KEY);
			int valueIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
			int versionIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VERSION);
			int portIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_PORT);
			if (keyIndex == -1 || valueIndex == -1 || versionIndex == -1) {
				Log.e("RECOVERVALUES", "Wrong columns");
				cursor.close();
				try {
					throw new Exception();
				} catch (Exception e) {
					Log.d("RECOVERVALUES", "Exception!!" + e.toString());
				}
			}
			cursor.moveToFirst();
			for (int i = 0; i < m; i++) {
				String returnKey = cursor.getString(keyIndex);
				String returnValue = cursor.getString(valueIndex);
				String returnversionValue = cursor.getString(versionIndex);
				String returnPort = cursor.getString(portIndex);
				Log.d("RECOVERVALUES", "Value returned from cursor: "+ returnValue);
				Log.d("RECOVERVALUES","Key: "+returnKey+" Key returned from cursor: " + returnKey);
				Log.d("RECOVERVALUES","Key: "+returnKey+" Version returned from cursor: "+ returnversionValue);
				Log.d("RECOVERVALUES","Key: "+returnKey+" Port returned from cursor: "+ returnPort);
				qResult.getResults().put(returnKey,returnValue + "~" + returnversionValue + "~" +returnPort);
				//Log.d("RECOVERVALUES", "QResult returned from queryALL: "+ qResult.toString());
				cursor.moveToNext();
				count++;
			}
			//Log.d("RECOVERVALUES","QResult returned from queryALL: " + qResult.toString());
			Log.d("RECOVERVALUES","Count is=" + Integer.toString(count));
			Log.d("RECOVERVALUES","size of queryResult="+ Integer.toString(qResult.getResults().size()));
			cursor.close();
		}
		return qResult;
	}


	public MatrixCursor queryAllNodes() {
		MatrixCursor mCursor = new MatrixCursor(new String[] {
				SimpleDynamoDatabase.COLUMN_KEY,
				SimpleDynamoDatabase.COLUMN_VALUE });
		Log.d("QUERYALLNODES", "Selection = *");
		QueryResult qResult = new QueryResult();
		QueryResult finalResult = new QueryResult();
		String msgToSend = "QUERYALL";
		int i = 0, count = 0;
		String[] valueObtained;
		String time;
		long timeStampNew, timeStampOld;
		Log.d("QUERYALLNODES", "I am =" + myPort);
		Log.d("QUERYALLNODES", "My next port="+ nodeList.get(i).getNext_node_port());
		for (i = 0; i < MAX; i++) {
			Log.d("QUERYALLNODES", "Value of i:" + Integer.toString(i));
			Log.d("QUERYALLNODES", "I am =" + nodeList.get(i).getNode_port());
			Log.d("My next port=", nodeList.get(i).getNext_node_port());
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2 }), Integer.parseInt(nodeList.get(i).getNode_port()));
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				InputStream inStream = socket.getInputStream();
				ObjectInputStream oInStream = new ObjectInputStream(inStream);
				qResult = (QueryResult) oInStream.readObject();
				Log.d("QUERYALLNODES", "Retrieving Results from node: "+ nodeList.get(i).getNode_avd());
				Log.d("QUERYALLNODES","Size of the results="+ Integer.toString(qResult.getResults().size()));
				if (qResult != null && qResult.getResults() != null) {
					Iterator<Entry<String, String>> iterator = qResult.getResults().entrySet().iterator();
					while (iterator.hasNext()) {
						Log.d("QUERYALLNODES","Value returned from other nodes: "+ qResult.toString());
						Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator.next();
						//Log.d("QUERYALLNODES",
						//		"Key Retrieved in QueryAllNodes: "
						//				+ entry.getKey());
						Log.d("QUERYALLNODES","Value Retrieved in QueryAllNodes: "+ entry.getValue());
						if (finalResult.getResults().containsKey(entry.getKey())) {
							// Check for the timeStamp and put the latest one in
							// here
							// Obtain timeStamp for the new keyValue pair
							valueObtained = entry.getValue().split("~");
							time = valueObtained[1];
							timeStampNew = Long.valueOf(time);

							// Obtain timeStamp for the old keyValue pair in
							// finalResult
							valueObtained = finalResult.getResults().get(entry.getKey()).split("~");
							time = valueObtained[1];
							timeStampOld = Long.valueOf(time);

							// Compare the 2 timeStamps and add appropriate
							// value to Map
							if (timeStampNew > timeStampOld) {
								finalResult.getResults().put(entry.getKey(),
								entry.getValue());
							}
						} else 
						{
							finalResult.getResults().put(entry.getKey(),
							entry.getValue());
						}
						count++;
						iterator.remove();
					}
					Log.d("QUERYALLNODES","Value of Count=" + Integer.toString(count));
					Log.d("QUERYALLNODES","Size of finalResults"+ Integer.toString(finalResult.getResults().size()));
				}
				socket.close();
			} catch (NumberFormatException e) {
				Log.e("QUERYALLNODES","Exception!!"+e.toString());
			} catch (UnknownHostException e) {
				Log.e("QUERYALLNODES","Exception!!"+e.toString());
			} catch (ClassNotFoundException e) {
				Log.e("QUERYALLNODES","Exception!!"+e.toString());
			} catch (IOException e) {
				Log.e("QUERYALLNODES","Exception!!"+e.toString());
			}

		}
		// Put the values from the final result to the cursor
		Iterator<Entry<String, String>> iterator = finalResult.getResults()
				.entrySet().iterator();
		count = 0;
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator.next();
			Log.d("Key Retrieved from finalResult: ", entry.getKey());
			Log.d("Value Retrieved from finalResult: ", entry.getValue());
			valueObtained = entry.getValue().split("~");
			Log.d("QUERYWITHKEY", "Split Value =" + valueObtained[0]);
			mCursor.addRow(new String[] { entry.getKey(), valueObtained[0] });
			//Log.d("Key added to cursor in QueryAll: ", mCursor.toString());
			count++;
			iterator.remove();
		}
		Log.d("After adding to cursor: Value of Count=",Integer.toString(count));
		mCursor.moveToFirst();
		/*while (!mCursor.isLast()) {
			Log.d("key Added to cursor: ", mCursor.getString(0));
			mCursor.moveToNext();
		}*/
		return mCursor;
	}

	public QueryResult getAllValues() {
		Log.v("GETALLVALUES", "Selection =*");
		String[] columnsToReturn = { "key", "value", "version" };
		QueryResult qResult = new QueryResult();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		Cursor cursor;
		Integer m;
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE);
		cursor = queryBuilder.query(db, columnsToReturn, null, null, null,null, null);
		cursor.setNotificationUri(getContext().getContentResolver(),CONTENT_URI);
		m = cursor.getCount();
		int count = 1;
		Log.d("GETALLVALUES", "cursor count =" + m.toString());
		if (m == 0)
			Log.d("GETALLVALUES", "cursor count =" + m.toString() + "I am null");
		else {
			Log.d("GETALLVALUES", "cursor count = " + m.toString());
			int keyIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_KEY);
			int valueIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
			int versionIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VERSION);
			if (keyIndex == -1 || valueIndex == -1 || versionIndex == -1) {
				Log.e("GETALLVALUES", "Wrong columns");
				cursor.close();
				try {
					throw new Exception();
				} catch (Exception e) {
					Log.e("GETALLVALUES", "Exception!!" + e.toString());
				}
			}
			cursor.moveToFirst();
			for (int i = 0; i < m; i++) {
				String returnKey = cursor.getString(keyIndex);
				String returnValue = cursor.getString(valueIndex);
				String returnversionValue = cursor.getString(versionIndex);
				Log.d("GETALLVALUES", "Value returned from cursor: "+ returnValue);
				Log.d("GETALLVALUES", "Key returned from cursor: " + returnKey);
				Log.d("GETALLVALUES", "Version returned from cursor: "+ returnversionValue);
				qResult.getResults().put(returnKey,returnValue + "~" + returnversionValue);
				//Log.d("GETALLVALUES", "QResult returned from queryALL: "
				//		+ qResult.toString());
				cursor.moveToNext();
				count++;
			}
			//Log.d("GETALLVALUES","QResult returned from queryALL: " + qResult.toString());
			Log.d("GETALLVALUES","Count from:" + myPort + " is=" + Integer.toString(count));
			Log.d("GETALLVALUES","size of queryResult="+ Integer.toString(qResult.getResults().size()));
			cursor.close();
		}
		return qResult;
	}
	

	public MatrixCursor queryWithKey(String selection) {
		String msgToSend = null;
		String keyHash = null;
		MatrixCursor mCursor = new MatrixCursor(new String[] {
				SimpleDynamoDatabase.COLUMN_KEY,
				SimpleDynamoDatabase.COLUMN_VALUE });
		QueryResult qResult = new QueryResult();
		QueryResult finalResult = new QueryResult();
		String[] valueObtained;
		String time;
		long timeStampNew, timeStampOld;
		try {
			keyHash = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			Log.e("QUERYWITHKEY", "Exception in queryWithKey " + e.toString());
			e.printStackTrace();
		}
		Log.d("QUERYWITHKEY", "Key in queryWithKey: " + selection);
		Log.d("QUERYWITHKEY", "KeyHash in queryWithKey: " + keyHash);

		int index = lookUp(keyHash);
		// send message to node and its 2 successors
		msgToSend = "QUERYWITHKEY" + "~" + selection;
		// Write code to wait for values from other replication nodes
		for (int i = 0; i < 3; i++) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2 }), Integer.parseInt(nodeList.get(index % MAX).getNode_port()));
				ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
				outputStream.writeObject(msgToSend);
				InputStream inStream = socket.getInputStream();
				ObjectInputStream oInStream = new ObjectInputStream(inStream);
				qResult = (QueryResult) oInStream.readObject();
				if (qResult != null && qResult.getResults() != null) {
					Log.d("QUERYWITHKEY", "Value returned from other nodes: "+ qResult.toString());
					Iterator<Entry<String, String>> iterator = qResult.getResults().entrySet().iterator();
					while (iterator.hasNext()) {
						Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator.next();
						Log.d("QUERYWITHKEY","Key Retrieved in QueryAllNodes: "+ entry.getKey());
						Log.d("QUERYWITHKEY","Value Retrieved in QueryAllNodes: "+ entry.getValue());
						if (finalResult.getResults().containsKey(entry.getKey())) {
							// Check for the timeStamp and put the latest one in
							// here
							// Obtain timeStamp for the new keyValue pair
							valueObtained = entry.getValue().split("~");
							time = valueObtained[1];
							timeStampNew = Long.valueOf(time);

							// Obtain timeStamp for the old keyValue pair in
							// finalResult
							valueObtained = finalResult.getResults().get(entry.getKey()).split("~");
							time = valueObtained[1];
							timeStampOld = Long.valueOf(time);

							// Compare the 2 timeStamps and add appropriate
							// value to Map
							if (timeStampNew > timeStampOld)
							{
								finalResult.getResults().put(entry.getKey(),entry.getValue());
							}
						} else 
						{
							finalResult.getResults().put(entry.getKey(),entry.getValue());
						}
						iterator.remove();
					}
				}
				socket.close();
			}catch (EOFException e) {
				Log.e("QUERYWITHKEY", "Exception!!" + e.toString());
			}catch (NumberFormatException e) {
				Log.e("QUERYWITHKEY", "Exception!!" + e.toString());
			} catch (UnknownHostException e) {
				Log.e("QUERYWITHKEY", "Exception!!" + e.toString());
			} catch (ClassNotFoundException e) {
				Log.e("QUERYWITHKEY", "Exception!!" + e.toString());
			} catch (IOException e) {
				Log.e("QUERYWITHKEY", "Exception!!" + e.toString());
			}
			index++;
		}
		// Put the values from the final result to the cursor
		Iterator<Entry<String, String>> iterator = finalResult.getResults()
				.entrySet().iterator();
		mCursor.moveToFirst();
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator
					.next();
			Log.d("QUERYWITHKEY", "Key: " + entry.getKey());
			Log.d("QUERYWITHKEY", "Value: " + entry.getValue());
			valueObtained = entry.getValue().split("~");
			Log.d("QUERYWITHKEY", "Split Value =" + valueObtained[0]);
			mCursor.addRow(new String[] { entry.getKey(), valueObtained[0] });
			Log.d("QUERYWITHKEY","Key added to cursor= " + mCursor.getString(0));
			Log.d("QUERYWITHKEY","Value added to cursor= " + mCursor.getString(1));
			iterator.remove();
		}
		return mCursor;
	}

	public QueryResult getValues(String selection) {
		Log.v("GETVALUES", "selection =" + selection);
		QueryResult qResult = new QueryResult();
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		Cursor cursor;
		Integer m;
		String[] columnsToReturn = { "key", "value", "version" };
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE);
		queryBuilder.appendWhere(SimpleDynamoDatabase.COLUMN_KEY + " = " + "'"+ selection + "'");
		cursor = queryBuilder.query(db, columnsToReturn, null, null, null,null, null);
		cursor.setNotificationUri(getContext().getContentResolver(),CONTENT_URI);
		m = cursor.getCount();
		Log.d("GETVALUES", "cursor count=" + m.toString());
		if (m == 0)
			Log.d("GETVALUES", "Cursor count=" + m.toString() + " I am null!");
		else {
			Log.d("GETVALUES", "cursor count=" + m.toString());
			int keyIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_KEY);
			int valueIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
			int versionIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VERSION);
			if (keyIndex == -1 || valueIndex == -1) {
				Log.e(TAG, "Wrong columns");
				cursor.close();
				try {
					throw new Exception();
				} catch (Exception e) {
					Log.e("GETVALUES", "Exception!! " + e.toString());
					e.printStackTrace();
				}
			}
			cursor.moveToFirst();
			if (!(cursor.isFirst() && cursor.isLast())) {
				Log.e("GETVALUES", "More than one row returned");
				cursor.close();
				try {
					throw new Exception();
				} catch (Exception e) {
					Log.e("GETVALUES", "Exception!! " + e.toString());
					e.printStackTrace();
				}
			}
			String returnKey = cursor.getString(keyIndex);
			String returnValue = cursor.getString(valueIndex);
			String returnversionValue = cursor.getString(versionIndex);
			Log.d("GETVALUES", "Value returned from cursor: " + returnValue);
			Log.d("GETVALUES", "Key returned from cursor: " + returnKey);
			Log.d("GETVALUES", "Key returned from cursor: " + returnKey);
			qResult.getResults().put(returnKey,returnValue + "~" + returnversionValue);
			Log.d("GETVALUES","QResult returned from querywithkey: " + qResult.toString());
			cursor.close();
		}
		return qResult;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			// Using accept to bind the incoming connection to a new socket and
			// creating the BufferedReader stream to read incoming
			// messages
			while (true) {
				try {
					Socket client = serverSocket.accept();
					ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
					String recvMessage = (String) inputStream.readObject();
					if (recvMessage != null)
						Log.d("newRecvMsg in Server task: ", recvMessage);
					else
						Log.d("newRecvMsg is null: ", recvMessage);
					if (recvMessage != null && (recvMessage.contains("INSERT"))) {
						String[] msgObtained = new String[5];
						msgObtained = recvMessage.split("~");
						Log.d("INSERTSERVER","Message in INSERT: "+msgObtained[0]);
						Log.d("INSERTSERVER","Key: "+msgObtained[1]+" Key in INSERT: "+msgObtained[1]);
						Log.d("INSERTSERVER","Key: "+msgObtained[1]+"INSERTSERVER"+" key ="+msgObtained[1]+" Value in INSERT: "+msgObtained[2]);
						Log.d("INSERTSERVER","Key: "+msgObtained[1]+"INSERTSERVER"+" key ="+msgObtained[1]+" Port in INSERT:"+msgObtained[3]);
						Log.d("INSERTSERVER","Key: "+msgObtained[1]+"INSERTSERVER"+" key ="+msgObtained[1]+" Timestamp in INSERT:"+msgObtained[4]);
						ContentValues cv = new ContentValues();
						cv.put("key", msgObtained[1]);
						cv.put("value", msgObtained[2]);
						cv.put("port", msgObtained[3]);
						cv.put("version", msgObtained[4]);
						Long rowId = db.insertWithOnConflict(SimpleDynamoDatabase.TABLE, null, cv,SQLiteDatabase.CONFLICT_REPLACE);
						Log.d("INSERTSERVER","Key: "+msgObtained[1]+" row Id: "+rowId.toString());
					} else if (recvMessage != null && recvMessage.contains("QUERYWITHKEY")) {
						while(!isRecoveryDone)
						{
							//Spin loop
						}
						String[] msgObtained = new String[3];
						msgObtained = recvMessage.split("~");
						Log.d("Message in QueryWithKey: ", msgObtained[0]);
						Log.d("Key in QueryWithKey: ", msgObtained[1]);
						String select = msgObtained[1];
						QueryResult qResult = getValues(select);
						Log.d("QUERYWITHKEY: Result returned from cursor: ",qResult.toString());
						ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
						oos.writeObject(qResult);
						oos.flush();
						oos.close();
					} else if (recvMessage != null && recvMessage.contains("QUERYALL")) {
						while(!isRecoveryDone)
						{
							//Spin loop
						}
						String[] msgObtained = new String[3];
						msgObtained = recvMessage.split("~");
						Log.d("Message in QUERYALL: ", msgObtained[0]);
						QueryResult qResult = getAllValues();
						//Log.d("QUERYALL: Result returned from cursor: ",qResult.toString());
						Log.d("In server: QUERYALL ->",Integer.toString(qResult.getResults().size()));
						ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
						oos.writeObject(qResult);
						oos.flush();
						oos.close();
					} else if (recvMessage != null && recvMessage.contains("DELETEWITHKEY")) {
						String[] msgObtained = new String[3];
						msgObtained = recvMessage.split("~");
						Log.d("Message in DELETEWITHKEY: ", msgObtained[0]);
						Log.d("Key in DELETEWITHKEY: ", msgObtained[1]);
						String select = msgObtained[1];
						int rowNum = db.delete(SimpleDynamoDatabase.TABLE,"key ='" + select + "'", null);
						Log.d("Number of Rows deleted: ",Integer.toString(rowNum));
					} else if (recvMessage != null && recvMessage.contains("DELETEALL")) {
						String[] msgObtained = new String[3];
						msgObtained = recvMessage.split("~");
						Log.d("Message in DELETEALL: ", msgObtained[0]);
						deleteAll();
					}
					else if (recvMessage != null && recvMessage.contains("RECOVERGET")) {
						String[] msgObtained = new String[2];
						msgObtained = recvMessage.split("~");
						Log.d("Message in RECOVERGET",msgObtained[0]);
						Log.d("Port in RECOVERGET",msgObtained[1]);
						QueryResult qResult =recoverValues(msgObtained[1]);
						//Log.d("RECOVERGET: Result returned from cursor: ",qResult.toString());
						Log.d("RECOVERGET ->","In server count ="+Integer.toString(qResult.getResults().size()));
						ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
						oos.writeObject(qResult);
						oos.flush();
						oos.close();
					}
					else {
						Log.d("Error in message: ", recvMessage);
					}
					client.close();
				} catch (IOException ie) {
					Log.e("Exception!!!",ie.toString());
				} catch (ClassNotFoundException e1) {
					Log.e("Exception!!!",e1.toString());
				}
			}
		}
	}


	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String msgToSend = msgs[0];
			Log.d("in Client Task",msgToSend);
			if(msgToSend.equals("RECOVER"))
			{
				recoverNode();
			}
			else
				Log.e(TAG,"Message to send is null");
			return null;

		}

		public void recoverNode() {
			Log.d("RECOVERNODES", "In recover() after changing portToUse To String");
			Log.d("RECOVERNODES", "Start");
			QueryResult qResult = new QueryResult();
			QueryResult finalResult = new QueryResult();
			String msgToSend = "RECOVERGET" + "~" + myPort;
			int i = 0, count = 0;
			String[] valueObtained;
			String portToUse;
			String time;
			long timeStampNew, timeStampOld;
			portToUse = nodesToReplicateFrom[0];
			// Send message to successor nodes to get the key value pairs that
			// belong to you
			for (i = 0; i < 6; i++) {
				if(i <= 1)
				{
					portToUse = nodesToReplicateFrom[i%2];
					msgToSend = "RECOVERGET" + "~" + myPort;
					Log.d("RECOVERNODES","Sending message to port="+portToUse);
					Log.d("RECOVERNODES","Value of i="+i);
					Log.d("RECOVERNODES","Message Sent="+msgToSend);
					
				}
				if(i==4)
				{
					portToUse = nodesToReplicateFrom[0];
					msgToSend = "RECOVERGET" + "~" +nodesToBeReplicated[0];
					Log.d("RECOVERNODES","Sending message to port="+portToUse);
					Log.d("RECOVERNODES","Value of i="+i);
					Log.d("RECOVERNODES","Message Sent="+msgToSend);
				}
				else if(i==5)
				{
					portToUse = nodesToBeReplicated[0];
					msgToSend = "RECOVERGET" + "~" +nodesToBeReplicated[1];
					Log.d("RECOVERNODES","Sending message to port="+portToUse);
					Log.d("RECOVERNODES","Value of i="+i);
					Log.d("RECOVERNODES","Message Sent="+msgToSend);
				}
				else if (i >= 2) 
				{
					portToUse = nodesToBeReplicated[i%2];
					msgToSend = "RECOVERGET" + "~" + portToUse;
					Log.d("RECOVERNODES","Sending message to port="+portToUse);
					Log.d("RECOVERNODES","Value of i="+i);
					Log.d("RECOVERNODES","Message Sent="+msgToSend);
				}
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2 }), Integer.parseInt(portToUse));
					ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject(msgToSend);
					InputStream inStream = socket.getInputStream();
					ObjectInputStream oInStream = new ObjectInputStream(inStream);
					qResult = (QueryResult) oInStream.readObject();
					Log.d("RECOVERNODES","Retrieving Results from node: "+portToUse);
					Log.d("RECOVERNODES","Size of the results="+ Integer.toString(qResult.getResults().size()));
					if (qResult != null && qResult.getResults() != null) 
					{
						Iterator<Entry<String, String>> iterator = qResult.getResults().entrySet().iterator();
						while (iterator.hasNext()) 
						{
							Log.d("RECOVERNODES","in Iterator Loop");
							Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator.next();
							Log.d("RECOVERNODES","Key: "+entry.getKey()+" Key Retrieved in recoverNode: "+ entry.getKey());
							Log.d("RECOVERNODES","Key: "+entry.getKey()+"RECOVERNODES"+"Value Retrieved in recoverNode: "+ entry.getValue());
							if (finalResult.getResults().containsKey(entry.getKey())) {
								Log.d("Key: "+entry.getKey(),"RECOVERNODES"+"Key Present in finalResult");
								// Check for the timeStamp and put the latest one in here
								// Obtain timeStamp for the new keyValue pair
								valueObtained = entry.getValue().split("~");
								time = valueObtained[1];
								timeStampNew = Long.valueOf(time);

								// Obtain timeStamp for the old keyValue pair in finalResult
								valueObtained = finalResult.getResults().get(entry.getKey()).split("~");
								time = valueObtained[1];
								timeStampOld = Long.valueOf(time);
								Log.d("RECOVERNODES","Key: "+entry.getKey()+"New timestamp="+timeStampNew);
								Log.d("RECOVERNODES","Key: "+entry.getKey()+"Old timestamp="+timeStampOld);

								// Compare the 2 timeStamps and add appropriate
								// value to Map
								if (timeStampNew > timeStampOld) {
									Log.d("RECOVERNODES","Key: "+entry.getKey()+"New value has Latest timestamp");
									finalResult.getResults().put(entry.getKey(),
											entry.getValue());
								}
								else
								{
									Log.d("RECOVERNODES","Key: "+entry.getKey()+"key value pair in finalResult is latest");
								}
							} 
							else 
							{
								Log.d("RECOVERNODES","Key: "+entry.getKey()+"Key Added in finalResult");
								finalResult.getResults().put(entry.getKey(),entry.getValue());
							}
							count++;
							iterator.remove();
						}
						Log.d("RECOVERNODES","Value of Count=" + Integer.toString(count));
						Log.d("RECOVERNODES","Size of finalResults="+ Integer.toString(finalResult.getResults().size()));
					}
					socket.close();
				} catch (NumberFormatException e) {
					Log.e("RECOVERNODES","Exception!!"+e.toString());
				} catch (UnknownHostException e) {
					Log.e("RECOVERNODES","Exception!!"+e.toString());
				} catch (ClassNotFoundException e) {
					Log.e("RECOVERNODES","Exception!!"+e.toString());
				} catch (IOException e) {
					Log.e("RECOVERNODES","Exception!!"+e.toString());
				}

			}
			Iterator<Entry<String, String>> iterator = finalResult.getResults().entrySet().iterator();
			count = 0;
			while (iterator.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator.next();
				Log.d("RECOVERNODES","Key: "+entry.getKey()+" Key Retrieved from finalResult: " + entry.getKey());
				Log.d("RECOVERNODES","Key: "+entry.getKey()+"Value Retrieved from finalResult: " + entry.getValue());
				valueObtained = entry.getValue().split("~");
				Log.d("RECOVERNODES","Key: "+entry.getKey()+ "Value after split =" + valueObtained[0]);
				//Log.d("RECOVERNODES", "Port after split =" + valueObtained[2]);
				//Log.d("RECOVERNODES", "Version after split=" + valueObtained[1]);
				count++;
				Log.d("RECOVERNODES","After inserting Value of Count=" + Integer.toString(count));
				SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
				Cursor cursor;
				String[] columnsToReturn = { "key", "value", "version" };
				queryBuilder.setTables(SimpleDynamoDatabase.TABLE);
				queryBuilder.appendWhere(SimpleDynamoDatabase.COLUMN_KEY + " = " + "'"+ entry.getKey() + "'");
				cursor = queryBuilder.query(db, columnsToReturn, null, null, null,null, null);
				// Insert all the recovered values into DB
				ContentValues cv = new ContentValues();
				cv.put("key", entry.getKey());
				cv.put("value", valueObtained[0]);
				cv.put("port", valueObtained[2]);
				cv.put("version", valueObtained[1]);
				if(cursor.getCount()==0)
				{
					Long rowId = db.insertWithOnConflict(SimpleDynamoDatabase.TABLE,null, cv, SQLiteDatabase.CONFLICT_REPLACE);
					Log.d("RECOVERNODES","Key: "+entry.getKey()+" rowId=" + rowId.toString());
				}
				else
				{
					cursor.moveToFirst();
					int keyIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_KEY);
					int valueIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
					int versionIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VERSION);
					String returnKey = cursor.getString(keyIndex);
					String returnValue = cursor.getString(valueIndex);
					String returnversionValue = cursor.getString(versionIndex);
					timeStampOld = Long.valueOf(returnversionValue);
					time = valueObtained[1];
					timeStampNew = Long.valueOf(time);
					Log.d("RECOVERNODES","Key: "+entry.getKey()+"New timestamp="+timeStampNew);
					Log.d("RECOVERNODES","Key: "+entry.getKey()+"Old timestamp="+timeStampOld);
					// Compare the 2 timeStamps and add appropriate
					// value to Map
					if (timeStampNew > timeStampOld) {
						Log.d("RECOVERNODES","Key: "+entry.getKey()+"New value has Latest timestamp");
						Long rowId = db.insertWithOnConflict(SimpleDynamoDatabase.TABLE,null, cv, SQLiteDatabase.CONFLICT_REPLACE);
						Log.d("RECOVERNODES","Updated DB for Key: "+entry.getKey()+" rowId=" + rowId.toString());
					}
					else
					{
						Log.d("RECOVERNODES","Key value pair in DB is latest for Key: "+entry.getKey());
					}
					
				}
				iterator.remove();
			}
			isRecoveryDone = true;

		}

	}

}