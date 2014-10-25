/*
	@author - Roshini Sebastian
	Person # - 5009-8161
*/
package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

//Class definition for defining the GroupMessengerDatabase
public class SimpleDynamoDatabase
{
// Defining the schema of the table 
  public static final String TABLE = "myDHTable";
  public static final String COLUMN_KEY = "key";
  public static final String COLUMN_VALUE = "value";
  public static final String COLUMN_VERSION = "version";
  public static final String COLUMN_PORT = "port";
  static final String TAG = SimpleDynamoProvider.class.getSimpleName();
  //The create table definition
  private static final String SQL_CREATE_MAIN = "CREATE TABLE " +
		  TABLE +                       
		    "(" +                           
		    COLUMN_KEY +" TEXT PRIMARY KEY , " +
		    COLUMN_VALUE + " TEXT NOT NULL ," +
		    COLUMN_VERSION + " TEXT ," +
		    COLUMN_PORT + " TEXT " +
		    ")";
  
  //This method will be called in the onCreate method of the
  //GroupMessengerDatabaseHelper class to create a new SQLite DB
  public static void onCreate(SQLiteDatabase database) {
	  	Log.d(TAG,"In create of SimpleDynamoDatabase");
	    database.execSQL(SQL_CREATE_MAIN);
  }

  //This method is called when there is a need to update the version of the database
  //The call is invoked from the onUpgrade method of the GroupMessengerDatabaseHelper class
  public static void onUpgrade(SQLiteDatabase database, int oldVersion,int newVersion) {
	    Log.w(SimpleDynamoDatabase.class.getName(), "Upgrading database from "+ oldVersion+ "to" +newVersion);
	    database.execSQL("DROP TABLE IF EXISTS " + TABLE);
	    onCreate(database);
  }
  
}