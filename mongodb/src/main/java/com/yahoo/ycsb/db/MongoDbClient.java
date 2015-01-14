/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Arrays;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.AggregationOptions;
import com.mongodb.Cursor;

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=acknowledged
 *
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton Mongo instance. */
    private static Mongo[] mongos;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    private static Random random = new Random();

    private static String[] clients = null;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongos != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String urls = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017");

            clients = urls.split(",");
            mongos = new Mongo[clients.length];

            database = props.getProperty("mongodb.database", "ycsb");

            // Set connectionpool to size of ycsb thread pool
            final String maxConnections = props.getProperty("threadcount", "100");

            // write concern
            String writeConcernType = props.getProperty("mongodb.writeConcern", "acknowledged").toLowerCase();
            if ("errors_ignored".equals(writeConcernType)) {
                writeConcern = WriteConcern.ERRORS_IGNORED;
            }
            else if ("unacknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.UNACKNOWLEDGED;
            }
            else if ("acknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.ACKNOWLEDGED;
            }
            else if ("journaled".equals(writeConcernType)) {
                writeConcern = WriteConcern.JOURNALED;
            }
            else if ("replica_acknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
            }
            else {
                System.err.println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ errors_ignored | unacknowledged | acknowledged | journaled | replica_acknowledged ]");
                System.exit(1);
            }

            // readPreference
            String readPreferenceType = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
            if ("primary".equals(readPreferenceType)) {
                readPreference = ReadPreference.primary();
            }
            else if ("primary_preferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.primaryPreferred();
            }
            else if ("secondary".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondary();
            }
            else if ("secondary_preferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondaryPreferred();
            }
            else if ("nearest".equals(readPreferenceType)) {
                readPreference = ReadPreference.nearest();
            }
            else {
                System.err.println("ERROR: Invalid readPreference: '"
                                + readPreferenceType
                                + "'. Must be [ primary | primary_preferred | secondary | secondary_preferred | nearest ]");
                System.exit(1);
            }

        	for( int i=0; i< mongos.length; i++) {
                try {
                    // strip out prefix since Java driver doesn't currently support
                    // standard connection format URL yet
                    // http://www.mongodb.org/display/DOCS/Connections

	            	String url = clients[i];

	                if (url.startsWith("mongodb://")) {
	                    url = url.substring(10);
	                }

	                // need to append db to url.
	                url += "/" + database;
	                MongoOptions options = new MongoOptions();
                    options.setCursorFinalizerEnabled(false);
	                options.connectionsPerHost = Integer.parseInt(maxConnections);
	                mongos[i] = new Mongo(new DBAddress(url), options);

	                System.out.println("mongo connection created with " + url);
	        	} catch (Exception e1) {
                    System.err
                            .println("Could not initialize MongoDB connection pool for Loader: "
                                    + e1.toString());
                    e1.printStackTrace();
                    return;
                }
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
        	for(int i=0; i<mongos.length; i++) {
            	try {
                    mongos[i].close();
                }
                catch (Exception e1) {
                    System.err.println("Could not close MongoDB connection pool: "
                            + e1.toString());
                    e1.printStackTrace();
                    return;
                }
        	}
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
        	db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
               	r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r, writeConcern);
            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     * Extended YCSB secondary lookups
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int complexinsert(String table, String key,
            HashMap<String, Object> values) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k));
            }
            WriteResult res = collection.insert(r, writeConcern);
            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }


    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, Object> result) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn, readPreference);
            }
            else {
                queryResult = collection.findOne(q, null, readPreference);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     * Extended YCSB secondary lookups
     *
     * @param table The name of the table
     * @param fieldname The secondary read field of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String fieldname, Object key, Set<String> fields,
            HashMap<String, Object> result) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);

            HashMap<String, Object> fieldKey = new HashMap<String, Object>();
            fieldKey.put(fieldname, key );

            DBObject q = new BasicDBObject();

            for (String k : fieldKey.keySet()) {
                q.put(k, fieldKey.get(k));
            }

            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn, readPreference);
            }
            else {
                queryResult = collection.findOne(q, null, readPreference);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     * Extended YCSB complex lookups
     *
     * @param table The name of the table
     * @param fieldname The secondary read field of the table
     * @param key The record key of the record to read.
     * @param fieldname2 The compound read field of the table
     * @param lbdate The lower bound date key of the record to read.
     * @param ubdate The upper bound date key of the record to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String fieldname, Object key, String fieldname2, Object lbdate, Object ubdate, 
		    Set<String> fields, HashMap<String, Object> result){
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);

            HashMap<String, Object> dateKey = new HashMap<String, Object>();
            dateKey.put("$gte", lbdate );
            dateKey.put("$lte", ubdate );

            DBObject scanRange2 = new BasicDBObject();

            for (String k : dateKey.keySet()) {
                scanRange2.put(k, dateKey.get(k));
            }

            DBObject q = new BasicDBObject().append(fieldname, key )
                                   .append(fieldname2, scanRange2);

            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn, readPreference);
            }
            else {
                queryResult = collection.findOne(q, null, readPreference);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, Object>> result) {
        com.mongodb.DB db = null;
        DBCursor cursor = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, Object> resultMap = new HashMap<String, Object>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                if( cursor != null ) {
                    cursor.close();
                }
                db.requestDone();
            }
        }

    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     * Extended YCSB secondary scans
     *
     * @param table The name of the table
     * @param fieldname The secondary read field of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String fieldname, Object startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, Object>> result) {
        com.mongodb.DB db = null;
        DBCursor cursor = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);

            HashMap<String, Object> fieldKey = new HashMap<String, Object>();
            fieldKey.put("$gte", startkey);

            DBObject scanRange = new BasicDBObject();

            for (String k : fieldKey.keySet()) {
                scanRange.put(k, fieldKey.get(k));
            }

            DBObject q = new BasicDBObject().append(fieldname, scanRange);
            cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, Object> resultMap = new HashMap<String, Object>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                if( cursor != null ) {
                    cursor.close();
                }
                db.requestDone();
            }
        }

    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     * Extended YCSB complex scans
     *
     * @param table The name of the table
     * @param fieldname The secondary read field of the table
     * @param key The record key of the record to read.
     * @param fieldname2 The compound read field of the table
     * @param lbdate The lower bound date key of the record to read.
     * @param ubdate The upper bound date key of the record to read
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int scan(String table, String fieldname, Object startkey, String fieldname2, Object lbdate, Object ubdate, 
                    int recordcount, Set<String> fields, Vector<HashMap<String, Object>> result) {
        com.mongodb.DB db = null;
        DBCursor cursor = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);

            HashMap<String, Object> dateKey = new HashMap<String, Object>();
            dateKey.put("$gte", lbdate );
            dateKey.put("$lte", ubdate );

            DBObject scanRange2 = new BasicDBObject();

            for (String k : dateKey.keySet()) {
                scanRange2.put(k, dateKey.get(k));
            }

            DBObject q = new BasicDBObject().append(fieldname, startkey )
                                   .append(fieldname2, scanRange2);

            cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, Object> resultMap = new HashMap<String, Object>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                if( cursor != null ) {
                    cursor.close();
                }
                db.requestDone();
            }
        }

    }

     /**
      * Perform an aggregate for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
      * Extended YCSB aggregates
      *
      * @param table The name of the table
      * @param fieldnameMatch The field of the table used for matching records
      * @param startkeyMatch The start record key to be matched for aggregate
      * @param endkeyMatch The end record key to be matched for aggregate
      * @param aggregaterecordcount The number of records to be filtered for aggregate
      * @param fieldnameGroup The field of the table used for grouping records
      * @param groupfunction The function name used for grouping records: valid values are "sum", "avg", "count"
      * @param topNresults The number of results from aggregate output to return
      * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
      * @return Zero on success, a non-zero error code on error or "not found".
      */
     public int aggregate(String table,String fieldNameMatch, Object startkeyMatch, Object endkeyMatch, int aggregaterecordcount,
                                      String fieldNameGroup, String groupfunction, int topNresults, Vector<HashMap<String,Object>> result)
     {
        com.mongodb.DB db = null;
        Cursor cursor = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);

            // create our pipeline operations, first with the $match
            DBObject match = new BasicDBObject("$match", new BasicDBObject(fieldNameMatch, new BasicDBObject("$gte",startkeyMatch).append("$lte",endkeyMatch)));

            // Now the $group operation
	    String fieldNameGrouped="intkey";
            DBObject groupFields = new BasicDBObject( "_id", "$"+fieldNameGroup );

            switch (groupfunction) {
            	case "count":
            		groupFields.put( groupfunction+fieldNameGrouped , new BasicDBObject( "$sum", 1 ));
               	 	break;
            	case "sum":
            	case "avg":
            	case "first":
            	case "last":
            	case "min":
            	case "max":
            		groupFields.put( groupfunction+fieldNameGrouped , new BasicDBObject( "$"+groupfunction, "$"+fieldNameGrouped ));
               	 	break;
            	default:
               	 	throw new IllegalArgumentException("Invalid accumulator: " + groupfunction);
            }

            DBObject group = new BasicDBObject("$group", groupFields);

            // Finally the $sort and $limit operations
            DBObject sort = new BasicDBObject("$sort", new BasicDBObject( groupfunction+fieldNameGrouped , -1 ));
	    // filter to limit number of records to be aggregated
            DBObject limit1 = new BasicDBObject("$limit", aggregaterecordcount );
	    // limit to topNresults aggregate output results
            DBObject limit2 = new BasicDBObject("$limit", topNresults );

            // run aggregation
            List<DBObject> pipeline = Arrays.asList(match, limit1, group, sort, limit2);

            AggregationOptions aggregationOptions = AggregationOptions.builder()
                .batchSize(100)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .allowDiskUse(true)
                .build();

            cursor = collection.aggregate(pipeline, aggregationOptions);

            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, Object> resultMap = new HashMap<String, Object>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                if( cursor != null ) {
                    cursor.close();
                }
                db.requestDone();
            }
        }

    }

    /**
     * Perform an aggregate for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     * Extended YCSB simple aggregates
     *
     * @param table The name of the table
     * @param fieldnameGroup The field of the table used for grouping records
     * @param len The number of records to be filtered for aggregate
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int aggregate(String table, String fieldNameGroup, int len, Vector<HashMap<String,Object>> result)
    {
       com.mongodb.DB db = null;
       Cursor cursor = null;
       try {
	   db = mongos[random.nextInt(mongos.length)].getDB(database);
	   db.requestStart();
	   DBCollection collection = db.getCollection(table);

	   // Now the $group operation
	   String fieldNameGrouped="intkey";
	   DBObject groupFields = new BasicDBObject( "_id", "$"+fieldNameGroup );

	   DBObject group = new BasicDBObject("$group", groupFields);

	   // Finally the $sort and $limit operations
	   DBObject sort = new BasicDBObject("$sort", new BasicDBObject( "_id", -1 ));
	   // filter to limit number of records to be aggregated
	   DBObject limit = new BasicDBObject("$limit", len );

	   // run aggregation
	   List<DBObject> pipeline = Arrays.asList(limit, group, sort);

	   AggregationOptions aggregationOptions = AggregationOptions.builder()
	       .batchSize(100)
	       .outputMode(AggregationOptions.OutputMode.CURSOR)
	       .allowDiskUse(true)
	       .build();

	   cursor = collection.aggregate(pipeline, aggregationOptions);

	   while (cursor.hasNext()) {
	       // toMap() returns a Map, but result.add() expects a
	       // Map<String,String>. Hence, the suppress warnings.
	       HashMap<String, Object> resultMap = new HashMap<String, Object>();

	       DBObject obj = cursor.next();
	       fillMap(resultMap, obj);

	       result.add(resultMap);
	   }

	   return 0;
       }
       catch (Exception e) {
	   System.err.println(e.toString());
	   return 1;
       }
       finally {
	   if (db != null) {
	       if( cursor != null ) {
		   cursor.close();
	       }
	       db.requestDone();
	   }
       }

    }

    /**
     * TODO - Finish
     *
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, Object> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
                resultMap.put(entry.getKey(), entry.getValue() );
        }
    }
}
