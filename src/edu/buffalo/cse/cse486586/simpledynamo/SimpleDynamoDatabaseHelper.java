package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class SimpleDynamoDatabaseHelper extends SQLiteOpenHelper
{
	// Defines the table name
	private static final String DBNAME = "SimpleDynamo.db";
	private static boolean isDBCreated = false;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	
	public SimpleDynamoDatabaseHelper(Context context) {
		super(context, DBNAME, null, 1);
	}

	//This method will call the onCreate method in the GroupMessengerDatabase
	//to create a database
	@Override
	public void onCreate(SQLiteDatabase db) {
		Log.d(TAG,"in onCreate of SimpleDynamoDatabaseHelper");
		isDBCreated = true;
		SimpleDynamoDatabase.onCreate(db);
	}

	//This method will call the onUpgrade method in the GroupMessengerDatabase
	//to upGrade the database to a new version
	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		SimpleDynamoDatabase.onUpgrade(db, oldVersion, newVersion);
		
	}
	
	public boolean getIsDBCreated()
	{
		return isDBCreated;
	}
	
	
}