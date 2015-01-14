/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import java.util.Properties;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.generator.RandomDataGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.Properties;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative 
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * 
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>secondaryreadproportion</b>: what proportion of operations should be secondary reads (default: 0)
 * <LI><b>complexreadproportion</b>: what proportion of operations should be complex reads (default: 0)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>aggregateproportion</b>: what proportion of operations should be aggregates (default: 0)
 * <LI><b>secondaryscanproportion</b>: what proportion of operations should be secondary scans (default: 0)
 * <LI><b>complexscanproportion</b>: what proportion of operations should be complex scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * <LI><b>complexreads</b>: should complex lookups be performed for read/scan operations: true or false (default: true)
 * <LI><b>secondaryreadfield</b>: Which field to be used for secondary read/scan key lookups: intkey, stringkey, or datekey (default: intkey)
 * <LI><b>aggregatetype</b>: Which type of aggregate to be used: simple or complex (default: simple)
 * <LI><b>aggregaterecordcount</b>: number of records to filter for aggregates (default: 10000)
 * <LI><b>groupfunction</b>: Which function to be used when grouping for aggregates: sum, avg, first, last, min, max, or count (default: sum)
 * <LI><b>secondarykeydistribution</b>: what distribution should be used to select the secondary key values - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>numdistinctintkeys</b>: the number of distinct values to generate for the intkey field (default: 300)
 * <LI><b>numdistinctstringkeys</b>: the number of distinct values to generate for the string field (default: 500)
 * <LI><b>numdistinctdatekeys</b>: the number of distinct values to generate for the datekey field (default: 240)
 * </ul> 
 */
public class ComplexWorkload extends Workload
{

	/**
	 * The name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY="table";

	/**
	 * The default name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY_DEFAULT="complextable";

	public static String table;


	/**
	 * The name of the property for the number of fields in a record.
	 */
	public static final String FIELD_COUNT_PROPERTY="fieldcount";
	
	/**
	 * Default number of fields in a record.
	 */
	public static final String FIELD_COUNT_PROPERTY_DEFAULT="10";

	int fieldcount;

	/**
	 * The name of the property for the field length distribution. Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
	 * 
	 * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
	 * histogram will be read from the filename specified in the "fieldlengthhistogram" property.
	 */
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY="fieldlengthdistribution";
	/**
	 * The default field length distribution.
	 */
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

	/**
	 * The name of the property for the length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY="fieldlength";
	/**
	 * The default maximum length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY_DEFAULT="100";

	/**
	 * The name of a property that specifies the filename containing the field length histogram (only used if fieldlengthdistribution is "histogram").
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
	/**
	 * The default filename containing a field length histogram.
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

	/**
	 * Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
	 */
	IntegerGenerator fieldlengthgenerator;
	
	/**
	 * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY="readallfields";
	
	/**
	 * The default value for the readallfields property.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT="true";

	boolean readallfields;

	/**
	 * The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY="writeallfields";
	
	/**
	 * The default value for the writeallfields property.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT="false";

	boolean writeallfields;


	/**
	 * The name of the property for the proportion of transactions that are reads.
	 */
	public static final String READ_PROPORTION_PROPERTY="readproportion";
	
	/**
	 * The default proportion of transactions that are reads.	
	 */
	public static final String READ_PROPORTION_PROPERTY_DEFAULT="0.95";

	/**
	 * The name of the property for the proportion of transactions that are secondary reads.
	 */
	public static final String SECONDARY_READ_PROPORTION_PROPERTY="secondaryreadproportion";
	
	/**
	 * The default proportion of transactions that are secondary reads.	
	 */
	public static final String SECONDARY_READ_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the proportion of transactions that are complex reads.
	 */
	public static final String COMPLEX_READ_PROPORTION_PROPERTY="complexreadproportion";
	
	/**
	 * The default proportion of transactions that are complex reads.	
	 */
	public static final String COMPLEX_READ_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY="updateproportion";
	
	/**
	 * The default proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT="0.05";

	/**
	 * The name of the property for the proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY="insertproportion";
	
	/**
	 * The default proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY="scanproportion";
	
	/**
	 * The default proportion of transactions that are aggregates.
	 */
	public static final String AGGREGATE_PROPORTION_PROPERTY_DEFAULT="0.0";
	
        /**
         * The name of the property for the proportion of transactions that are aggregates.
         */
        public static final String AGGREGATE_PROPORTION_PROPERTY="aggregateproportion";

        /**
         * The default proportion of transactions that are scans.
         */
        public static final String SCAN_PROPORTION_PROPERTY_DEFAULT="0.0";

	/**
	 * The name of the property for the proportion of transactions that are secondary scans.
	 */
	public static final String SECONDARY_SCAN_PROPORTION_PROPERTY="secondaryscanproportion";
	
	/**
	 * The default proportion of transactions that are secondary scans.
	 */
	public static final String SECONDARY_SCAN_PROPORTION_PROPERTY_DEFAULT="0.0";
	
	/**
	 * The name of the property for the proportion of transactions that are complex scans.
	 */
	public static final String COMPLEX_SCAN_PROPORTION_PROPERTY="complexscanproportion";
	
	/**
	 * The default proportion of transactions that are complex scans.
	 */
	public static final String COMPLEX_SCAN_PROPORTION_PROPERTY_DEFAULT="0.0";
	
	/**
	 * The name of the property for the proportion of transactions that are read-modify-write.
	 */
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY="readmodifywriteproportion";
	
	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT="0.0";
	
	/**
	 * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
	
	/**
	 * The default distribution of requests across the keyspace
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

	/**
	 * The name of the property for the max scan length (number of records)
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY="maxscanlength";
	
	/**
	 * The default max scan length.
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT="1000";
	
	/**
	 * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY="scanlengthdistribution";
	
	/**
	 * The default max scan length.
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT="uniform";
	
	/**
	 * The name of the property for the order to insert records. Options are "ordered" or "hashed"
	 */
	public static final String INSERT_ORDER_PROPERTY="insertorder";
	
	/**
	 * Default insert order.
	 */
	public static final String INSERT_ORDER_PROPERTY_DEFAULT="hashed";
	
	/**
	 * The name of the property for deciding whether to perform complex lookups for read/scan operations. Options are "true" or "false"
	 */
	public static final String COMPLEX_READS_PROPERTY="complexreads";
	
	/**
	 * The default value for the complex reads property.
	 */
	public static final String COMPLEX_READS_PROPERTY_DEFAULT="true";

	/**
	 * The name of the property for deciding which field to be used for secondary lookups. Options are "intkey", "stringkey", or "datekey"
	 */
	public static final String SECONDARY_READ_FIELD_PROPERTY="secondaryreadfield";
	
	/**
	 * Default secondary lookup field
	 */
	public static final String SECONDARY_READ_FIELD_PROPERTY_DEFAULT="intkey";
	
	/**
	 * The type of aggregate to be used. Options are "simple" or "complex"
	 */
	public static final String AGGREGATE_TYPE_PROPERTY="aggregatetype";
	
	/**
	 * Default aggregate type
	 */
	public static final String AGGREGATE_TYPE_PROPERTY_DEFAULT="simple";

	/**
	 * The name of the property for the max records to filter for aggregate (number of records)
	 */
	public static final String AGGREGATE_RECORD_COUNT_PROPERTY="aggregaterecordcount";
	
	/**
	 * The default max records to filter for aggregate
	 */
	public static final String AGGREGATE_RECORD_COUNT_PROPERTY_DEFAULT="1000";
        /**
         * The group operation function name
         */
        public static final String GROUP_OP_NAME_FIELD_PROPERTY="groupfunction";

        /**
         * Default group operation function name
         */
        public static final String GROUP_OP_NAME_FIELD_PROPERTY_DEFAULT="sum";

	/**
	 * The name of the property for the the distribution of secondary key values. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String SECONDARY_KEY_DISTRIBUTION_PROPERTY="secondarykeydistribution";
	
	/**
	 * The default distribution of secondary key values
	 */
	public static final String SECONDARY_KEY_DISTRIBUTION_PROPERTY_DEFAULT="uniform";

	/**
	 * The number of distinct values to generate for the intkey field
	 */
	public static final String NUM_DISTINCT_INT_KEYS_PROPERTY="numdistinctintkeys";
	/**
	 * The default maximum distinct values for intkeys
	 */
	public static final String NUM_DISTINCT_INT_KEYS_PROPERTY_DEFAULT="500";

	/**
	 * The number of distinct values to generate for the stringkey field
	 */
	public static final String NUM_DISTINCT_STRING_KEYS_PROPERTY="numdistinctstringkeys";
	/**
	 * The default maximum distinct values for stringkeys
	 */
	public static final String NUM_DISTINCT_STRING_KEYS_PROPERTY_DEFAULT="500";

	/**
	 * The number of distinct values to generate for the datekey field
	 */
	public static final String NUM_DISTINCT_DATE_KEYS_PROPERTY="numdistinctdatekeys";
	/**
	 * The default maximum distinct values for datekeys
	 */
	public static final String NUM_DISTINCT_DATE_KEYS_PROPERTY_DEFAULT="500";

	/**
        * Percentage data items that constitute the hot set.
        */
        public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
  
        /**
         * Default value of the size of the hot set.
         */
        public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";
  
        /**
         * Percentage operations that access the hot set.
         */
        public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
  
        /**
         * Default value of the percentage operations accessing the hot set.
         */
        public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";
	
	/**
	 * Generator object that produces secondary key values.  The value of this depends on the properties that start with "NUM_DISTINCT_"
	 */
        IntegerGenerator intkeygenerator;
        IntegerGenerator stringkeygenerator;
        IntegerGenerator daysoffsetgenerator;
	/*
	 * Generator objects to generate offsets for lower and upper bound date ranges
	 */
        IntegerGenerator lbdaysoffsetgenerator;
        IntegerGenerator ubdaysoffsetgenerator;
	
	IntegerGenerator keysequence;

	DiscreteGenerator operationchooser;

	IntegerGenerator keychooser;

	Generator fieldchooser;

	CounterGenerator transactioninsertkeysequence;
	
	IntegerGenerator scanlength;
	
	boolean orderedinserts;

	int recordcount;
	
	private static int numdistinctdatekeys;

        /** 
	 * variables to generate random keys for secondary lookups/range scans 
 	 */
        boolean complexreads;
        String secondaryreadfield;
        String aggregatetype;

        /** 
	 * variables used with aggregate workload
 	 */
        String groupfunction;
        int aggregaterecordcount;

	
	protected static IntegerGenerator getFieldLengthGenerator(Properties p) throws WorkloadException{
		IntegerGenerator fieldlengthgenerator;
		String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		int fieldlength=Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY,FIELD_LENGTH_PROPERTY_DEFAULT));
		String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
		if(fieldlengthdistribution.compareTo("constant") == 0) {
			fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
		} else if(fieldlengthdistribution.compareTo("uniform") == 0) {
			fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
		} else if(fieldlengthdistribution.compareTo("zipfian") == 0) {
			fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
		} else if(fieldlengthdistribution.compareTo("histogram") == 0) {
			try {
				fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
			} catch(IOException e) {
				throw new WorkloadException("Couldn't read field length histogram file: "+fieldlengthhistogram, e);
			}
		} else {
			throw new WorkloadException("Unknown field length distribution \""+fieldlengthdistribution+"\"");
		}
		return fieldlengthgenerator;
	}
	
	// generate random integers between 1 and numdistinctintkeys (or default 500), following specified secondary key distribution
	protected static IntegerGenerator getIntKeysGenerator(Properties p) throws WorkloadException{
		IntegerGenerator numdistinctintkeysgenerator;
		String secondarykeydistribution = p.getProperty(SECONDARY_KEY_DISTRIBUTION_PROPERTY, SECONDARY_KEY_DISTRIBUTION_PROPERTY_DEFAULT);
		int numdistinctintkeys=Integer.parseInt(p.getProperty(NUM_DISTINCT_INT_KEYS_PROPERTY, NUM_DISTINCT_INT_KEYS_PROPERTY_DEFAULT));
		if(secondarykeydistribution.compareTo("uniform") == 0) {
			numdistinctintkeysgenerator = new UniformIntegerGenerator(1, numdistinctintkeys);
		} else if(secondarykeydistribution.compareTo("zipfian") == 0) {
			numdistinctintkeysgenerator = new ZipfianGenerator(1, numdistinctintkeys);
		} else {
			throw new WorkloadException("Distribution \""+secondarykeydistribution+"\" not allowed for secondary intkey");
		}
		return numdistinctintkeysgenerator;
	}
	
        // generate random strings of names from an array between 1 and numdistinctstringkeys (or default 500), following specified secondary key distribution
        protected static IntegerGenerator getStringKeysGenerator(Properties p) throws WorkloadException{
                IntegerGenerator numdistinctstringkeysgenerator;
                String secondarykeydistribution = p.getProperty(SECONDARY_KEY_DISTRIBUTION_PROPERTY, SECONDARY_KEY_DISTRIBUTION_PROPERTY_DEFAULT);
                int numdistinctstringkeys=Integer.parseInt(p.getProperty(NUM_DISTINCT_STRING_KEYS_PROPERTY, NUM_DISTINCT_STRING_KEYS_PROPERTY_DEFAULT));
                if(secondarykeydistribution.compareTo("uniform") == 0) {
                        numdistinctstringkeysgenerator = new UniformIntegerGenerator(1, numdistinctstringkeys);
                } else if(secondarykeydistribution.compareTo("zipfian") == 0) {
                        numdistinctstringkeysgenerator = new ZipfianGenerator(1, numdistinctstringkeys);
                } else {
                        throw new WorkloadException("Distribution \""+secondarykeydistribution+"\" not allowed for secondary stringkey");
                }
                return numdistinctstringkeysgenerator;
        }

        // generate random dates from a basedate and numdistinctdatekeys (or default 500), following specified secondary key distribution
	protected static Date getRandomDate(int daysFromBase)
        {
		// base date randomly chosen for test data purposes
                Date bDate = new Date(112,01,01);
                Calendar cal = Calendar.getInstance();
                cal.setTime(bDate);
		TimeZone tz = TimeZone.getTimeZone("America/Los_Angeles");
        	cal.setTimeZone(tz);

                if ( daysFromBase < numdistinctdatekeys/2 ){
                        daysFromBase = daysFromBase - numdistinctdatekeys;
                        cal.add(Calendar.DATE, daysFromBase);
                } else {
                        cal.add(Calendar.DATE, daysFromBase);
                }

                return cal.getTime();

        }

	/**
	 * Initialize the scenario. 
	 * Called once, in the main client thread, before any operations are started.
	 */
	public void init(Properties p) throws WorkloadException
	{
		table = p.getProperty(TABLENAME_PROPERTY,TABLENAME_PROPERTY_DEFAULT);
		
		fieldcount=Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY,FIELD_COUNT_PROPERTY_DEFAULT));
		fieldlengthgenerator = ComplexWorkload.getFieldLengthGenerator(p);

		double readproportion=Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,READ_PROPORTION_PROPERTY_DEFAULT));
		double secondaryreadproportion=Double.parseDouble(p.getProperty(SECONDARY_READ_PROPORTION_PROPERTY,SECONDARY_READ_PROPORTION_PROPERTY_DEFAULT));
		double complexreadproportion=Double.parseDouble(p.getProperty(COMPLEX_READ_PROPORTION_PROPERTY,COMPLEX_READ_PROPORTION_PROPERTY_DEFAULT));
		double updateproportion=Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,UPDATE_PROPORTION_PROPERTY_DEFAULT));
		double insertproportion=Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,INSERT_PROPORTION_PROPERTY_DEFAULT));
		double scanproportion=Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY,SCAN_PROPORTION_PROPERTY_DEFAULT));
		double aggregateproportion=Double.parseDouble(p.getProperty(AGGREGATE_PROPORTION_PROPERTY,AGGREGATE_PROPORTION_PROPERTY_DEFAULT));
		double secondaryscanproportion=Double.parseDouble(p.getProperty(SECONDARY_SCAN_PROPORTION_PROPERTY,SECONDARY_SCAN_PROPORTION_PROPERTY_DEFAULT));
		double complexscanproportion=Double.parseDouble(p.getProperty(COMPLEX_SCAN_PROPORTION_PROPERTY,COMPLEX_SCAN_PROPORTION_PROPERTY_DEFAULT));
		double readmodifywriteproportion=Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY,READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
		recordcount=Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
		String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		int maxscanlength=Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY,MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
		String scanlengthdistrib=p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		
		int insertstart=Integer.parseInt(p.getProperty(INSERT_START_PROPERTY,INSERT_START_PROPERTY_DEFAULT));
		complexreads=Boolean.parseBoolean(p.getProperty(COMPLEX_READS_PROPERTY,COMPLEX_READS_PROPERTY_DEFAULT));
		secondaryreadfield=p.getProperty(SECONDARY_READ_FIELD_PROPERTY,SECONDARY_READ_FIELD_PROPERTY_DEFAULT);
		aggregatetype=p.getProperty(AGGREGATE_TYPE_PROPERTY,AGGREGATE_TYPE_PROPERTY_DEFAULT);
		groupfunction=p.getProperty(GROUP_OP_NAME_FIELD_PROPERTY,GROUP_OP_NAME_FIELD_PROPERTY_DEFAULT);
		aggregaterecordcount=Integer.parseInt(p.getProperty(AGGREGATE_RECORD_COUNT_PROPERTY,AGGREGATE_RECORD_COUNT_PROPERTY_DEFAULT));
                numdistinctdatekeys=Integer.parseInt(p.getProperty(NUM_DISTINCT_DATE_KEYS_PROPERTY, NUM_DISTINCT_DATE_KEYS_PROPERTY_DEFAULT));

		if (complexreads)
		{
			intkeygenerator = ComplexWorkload.getIntKeysGenerator(p);
			stringkeygenerator = ComplexWorkload.getStringKeysGenerator(p);
		
        		// generate days interval to apply to base date
        		daysoffsetgenerator = new UniformIntegerGenerator( 1, numdistinctdatekeys );
			// used for date range queries
        		lbdaysoffsetgenerator = new UniformIntegerGenerator( 1, (int) (numdistinctdatekeys * 0.40) );
        		ubdaysoffsetgenerator = new UniformIntegerGenerator( (int) ( 1 + numdistinctdatekeys * 0.60 ) , numdistinctdatekeys );
		}
		
		readallfields=Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY,READ_ALL_FIELDS_PROPERTY_DEFAULT));
		writeallfields=Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY,WRITE_ALL_FIELDS_PROPERTY_DEFAULT));
		
		if (p.getProperty(INSERT_ORDER_PROPERTY,INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed")==0)
		{
			orderedinserts=false;
		}
		else if (requestdistrib.compareTo("exponential")==0)
		{
                    double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                                                                         ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
                    double frac       = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                                                                         ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
                    keychooser = new ExponentialGenerator(percentile, recordcount*frac);
		}
		else
		{
			orderedinserts=true;
		}

		keysequence=new CounterGenerator(insertstart);
		operationchooser=new DiscreteGenerator();
		if (readproportion>0)
		{
			operationchooser.addValue(readproportion,"READ");
		}

		if (secondaryreadproportion>0)
		{
			operationchooser.addValue(secondaryreadproportion,"SECONDARYREAD");
		}

		if (complexreadproportion>0)
		{
			operationchooser.addValue(complexreadproportion,"COMPLEXREAD");
		}

		if (updateproportion>0)
		{
			operationchooser.addValue(updateproportion,"UPDATE");
		}

		if (insertproportion>0)
		{
			operationchooser.addValue(insertproportion,"INSERT");
		}
		
		if (scanproportion>0)
		{
			operationchooser.addValue(scanproportion,"SCAN");
		}
		
		if (secondaryscanproportion>0)
		{
			operationchooser.addValue(secondaryscanproportion,"SECONDARYSCAN");
		}
		
		if (aggregateproportion>0)
		{
			operationchooser.addValue(aggregateproportion,"AGGREGATE");
		}
		
		if (complexscanproportion>0)
		{
			operationchooser.addValue(complexscanproportion,"COMPLEXSCAN");
		}
		
		if (readmodifywriteproportion>0)
		{
			operationchooser.addValue(readmodifywriteproportion,"READMODIFYWRITE");
		}

		transactioninsertkeysequence=new CounterGenerator(recordcount);
		if (requestdistrib.compareTo("uniform")==0)
		{
			keychooser=new UniformIntegerGenerator(0,recordcount-1);
		}
		else if (requestdistrib.compareTo("zipfian")==0)
		{
			//it does this by generating a random "next key" in part by taking the modulus over the number of keys
			//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
			//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
			//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
			//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
			//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
			
			int opcount=Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
			int expectednewkeys=(int)(((double)opcount)*insertproportion*2.0); //2 is fudge factor
			
			keychooser=new ScrambledZipfianGenerator(recordcount+expectednewkeys);
		}
		else if (requestdistrib.compareTo("latest")==0)
		{
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
		}
		else if (requestdistrib.equals("hotspot")) 
		{
      			double hotsetfraction = Double.parseDouble(p.getProperty(
          			HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      			double hotopnfraction = Double.parseDouble(p.getProperty(
          			HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      			keychooser = new HotspotIntegerGenerator(0, recordcount - 1, 
          			hotsetfraction, hotopnfraction);
    		}
		else
		{
			throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
		}

		fieldchooser=new UniformIntegerGenerator(0,fieldcount-1);
		
		if (scanlengthdistrib.compareTo("uniform")==0)
		{
			scanlength=new UniformIntegerGenerator(1,maxscanlength);
		}
		else if (scanlengthdistrib.compareTo("zipfian")==0)
		{
			scanlength=new ZipfianGenerator(1,maxscanlength);
		}
		else
		{
			throw new WorkloadException("Distribution \""+scanlengthdistrib+"\" not allowed for scan length");
		}

	}

	public String buildKeyName(long keynum) {
 		if (!orderedinserts)
 		{
 			keynum=Utils.hash(keynum);
 		}
		return "user"+keynum;
	}

        String getSecondaryKeyValue(String fieldname) {

                String secondaryKey = new String();
                RandomDataGenerator rdata = new RandomDataGenerator();
                int rInt, daysFromBase;

                switch ( fieldname ) {
			case "intkey":
                        	rInt = intkeygenerator.nextInt();
                        	secondaryKey = String.valueOf( rInt );
				break;
                	case "stringkey":
                        	rInt = stringkeygenerator.nextInt();
                        	secondaryKey = rdata.getRandomName( rInt );
				break;
                	case "datekey":
                        	daysFromBase = daysoffsetgenerator.nextInt();
                        	secondaryKey = getRandomDate(daysFromBase).toString();
				break;
                	case "lbdatekey":
				daysFromBase = lbdaysoffsetgenerator.nextInt();
				// generate lower bound date using random offset
                        	secondaryKey = getRandomDate(daysFromBase).toString();
				break;
                	case "ubdatekey":
				daysFromBase = ubdaysoffsetgenerator.nextInt();
				// generate upper bound date using random offset
                        	secondaryKey = getRandomDate(daysFromBase).toString();
				break;
			default:
             			throw new IllegalArgumentException("Invalid secondary read field: " + fieldname);
                }

                return secondaryKey;
        }

        Object getComplexKeyValue(String fieldname) {

                Object complexKey = new String("");
                RandomDataGenerator rdata = new RandomDataGenerator();
                int rInt, daysFromBase;

                switch ( fieldname ) {
                        case "intkey":
                                rInt = intkeygenerator.nextInt();
                        	complexKey = rInt;
                                break;
                        case "stringkey":
                                rInt = stringkeygenerator.nextInt();
                        	complexKey = rdata.getRandomName( rInt );
                                break;
                        case "datekey":
                                daysFromBase = daysoffsetgenerator.nextInt();
                                complexKey = getRandomDate(daysFromBase);
                                break;
                        case "lbdatekey":
                                daysFromBase = lbdaysoffsetgenerator.nextInt();
                                // generate lower bound date using random offset
                                complexKey = getRandomDate(daysFromBase);
                                break;
                        case "ubdatekey":
                                daysFromBase = ubdaysoffsetgenerator.nextInt();
                                // generate upper bound date using random offset
                                complexKey = getRandomDate(daysFromBase);
                                break;
                        default:
                                throw new IllegalArgumentException("Invalid complex field name: " + fieldname);
                }


                return complexKey;
        }

        HashMap<String, Object> buildComplexValues() {
                HashMap<String,Object> values=new HashMap<String,Object>();

                String fieldkey = new String();
                StringByteIterator sbdata = new StringByteIterator("0");
                Object sdata = new String();
                ByteIterator data;

                // Three additional fields added to base YCSB and used by complex lookups
                if (complexreads)
                {
                        // integer data field - bounded by range specified in the generator
                        fieldkey = "intkey";
                        sdata = getComplexKeyValue (fieldkey );
                        values.put(fieldkey,sdata);

                        // string data field - bounded by range specified in the generator
                        fieldkey = "stringkey";
                        sdata = getComplexKeyValue (fieldkey );
                        values.put(fieldkey,sdata);

                        // date data field - bounded by range specified in the generator
                        fieldkey = "datekey";
                        sdata = getComplexKeyValue (fieldkey );
                        values.put(fieldkey,sdata);
                }

                // original fields created by YCSB
                for (int i=0; i<fieldcount; i++)
                {
                        fieldkey = "field" + i;
                        data= new RandomByteIterator(fieldlengthgenerator.nextInt());
                        values.put(fieldkey,data.toArray() );
                }

                return values;
        }


	HashMap<String, ByteIterator> buildValues() {
 		HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

                String fieldkey = new String();
                String sdata = new String();
                ByteIterator data;

		// Three additional fields added to base YCSB and used by complex lookups
		if (complexreads)
		{
                        // integer data field - bounded by range specified in the generator
			fieldkey = "intkey";
                        sdata = getSecondaryKeyValue (fieldkey );
                        data = new StringByteIterator( sdata );
                        values.put(fieldkey,data);

                        // string data field - bounded by range specified in the generator
                        fieldkey = "stringkey";
                        sdata = getSecondaryKeyValue (fieldkey );
                        data = new StringByteIterator( sdata );
                        values.put(fieldkey,data);

                        // date data field - bounded by range specified in the generator
                        fieldkey = "datekey";
                        sdata = getSecondaryKeyValue (fieldkey );
                        data = new StringByteIterator( sdata );
                        values.put(fieldkey,data);
		}

		// original 10 fields created by YCSB
                for (int i=0; i<fieldcount; i++)
                {
                        fieldkey = "field" + i;
                        data= new RandomByteIterator(fieldlengthgenerator.nextInt());
                        values.put(fieldkey,data);
                }

		return values;
	}

	HashMap<String, ByteIterator> buildUpdate() {
		//update a random field
		HashMap<String, ByteIterator> values=new HashMap<String,ByteIterator>();
		String fieldname="field"+fieldchooser.nextString();
		ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextInt());
		values.put(fieldname,data);
		return values;
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
	public boolean doInsert(DB db, Object threadstate)
	{
		int keynum=keysequence.nextInt();
		String dbkey = buildKeyName(keynum);
		if ( ! complexreads) {
			HashMap<String, ByteIterator> values = buildValues();
			if (db.insert(table,dbkey,values) == 0)
				return true;
			else
				return false;
		} else {
			HashMap<String, Object> cvalues = buildComplexValues();
			if (db.complexinsert(table,dbkey,cvalues) == 0)
				return true;
			else
				return false;
		}
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations.
	 */
	public boolean doTransaction(DB db, Object threadstate)
	{
		String op=operationchooser.nextString();

		if (op.compareTo("READ")==0)
		{
			doTransactionRead(db);
		}
		else if (op.compareTo("SECONDARYREAD")==0)
		{
			doTransactionSecondaryRead(db);
		}
		else if (op.compareTo("COMPLEXREAD")==0)
		{
			doTransactionComplexRead(db);
		}
		else if (op.compareTo("UPDATE")==0)
		{
			doTransactionUpdate(db);
		}
		else if (op.compareTo("INSERT")==0)
		{
			doTransactionInsert(db);
		}
		else if (op.compareTo("SCAN")==0)
		{
			doTransactionScan(db);
		}
		else if (op.compareTo("SECONDARYSCAN")==0)
		{
			doTransactionSecondaryScan(db);
		}
		else if (op.compareTo("COMPLEXSCAN")==0)
		{
			doTransactionComplexScan(db);
		}
		else if (op.compareTo("AGGREGATE")==0)
		{
			doTransactionAggregate(db);
		}
		else
		{
			doTransactionReadModifyWrite(db);
		}
		
		return true;
	}

    int nextKeynum() {
        int keynum;
        if(keychooser instanceof ExponentialGenerator) {
            do
                {
                    keynum=transactioninsertkeysequence.lastInt() - keychooser.nextInt();
                }
            while(keynum < 0);
        } else {
            do
                {
                    keynum=keychooser.nextInt();
                }
            while (keynum > transactioninsertkeysequence.lastInt());
        }
        return keynum;
    }

	public void doTransactionRead(DB db)
	{
		//choose a random key
		int keynum = nextKeynum();
		
		String keyname = buildKeyName(keynum);
		
		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.read(table,keyname,fields,new HashMap<String,Object>());

	}
	
	public void doTransactionSecondaryRead(DB db)
	{
		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		// extended lookup by specified field: one of intkey, stringkey, datekey

		Object keyvalue = getComplexKeyValue ( secondaryreadfield ); 

		db.read(table,secondaryreadfield,keyvalue,fields,new HashMap<String,Object>());
	}
	
	public void doTransactionComplexRead(DB db)
	{
		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		// extended lookup on intkey and date range

		String fieldName1 = "intkey";
		String fieldName2 = "datekey";
		Object keyvalue = getComplexKeyValue ( fieldName1 ); 
		Object lbdatekey = getComplexKeyValue ( "lbdatekey" ); 
		Object ubdatekey = getComplexKeyValue ( "ubdatekey" ); 

		db.read(table,fieldName1,keyvalue,fieldName2, lbdatekey, ubdatekey, fields,new HashMap<String,Object>());
	}
	
	public void doTransactionReadModifyWrite(DB db)
	{
		//choose a random key
		int keynum = nextKeynum();

		String keyname = buildKeyName(keynum);

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}
		
		HashMap<String,ByteIterator> values;

		if (writeallfields)
		{
		   //new data for all the fields
		   values = buildValues();
		}
		else
		{
		   //update a random field
		   values = buildUpdate();
		}

		//do the transaction
		
		long st=System.nanoTime();

		db.read(table,keyname,fields,new HashMap<String,Object>());
		
		db.update(table,keyname,values);

		long en=System.nanoTime();
		
		Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int)((en-st)/1000));
	}
	
	public void doTransactionScan(DB db)
	{
		//choose a random key
		int keynum = nextKeynum();

		String startkeyname = buildKeyName(keynum);
		
		//choose a random scan length
		int len=scanlength.nextInt();

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.scan(table,startkeyname,len,fields,new Vector<HashMap<String,Object>>());
	}

	public void doTransactionSecondaryScan(DB db)
	{
		//choose a random scan length
		int len=scanlength.nextInt();

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		// extended lookup by specified field: one of intkey, stringkey, datekey

		Object startkeyvalue = getComplexKeyValue ( secondaryreadfield ); 

		db.scan(table,secondaryreadfield,startkeyvalue,len,fields,new Vector<HashMap<String,Object>>());
	}

	public void doTransactionComplexScan(DB db)
	{
		//choose a random scan length
		int len=scanlength.nextInt();

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		// extended lookup by specified field: one of intkey, stringkey, datekey

		String fieldName1 = "intkey";
		String fieldName2 = "datekey";
		Object keyvalue = getComplexKeyValue ( fieldName1 ); 
		Object lbdatekey = getComplexKeyValue ( "lbdatekey" ); 
		Object ubdatekey = getComplexKeyValue ( "ubdatekey" ); 

		db.scan(table,fieldName1,keyvalue,fieldName2, lbdatekey, ubdatekey, len,fields,new Vector<HashMap<String,Object>>());
	}

        public void doTransactionAggregate(DB db)
        {
		//choose a random filter for simple aggregates
		int len=scanlength.nextInt();

		//filter for complex aggregates
                int topNresults=20;
		String fieldNameGroup = "intkey";
		
		switch (aggregatetype){
			case "complex":
                		fieldNameGroup = "stringkey";
                		String fieldNameMatch = "datekey";
                		Object startkeyMatch = getComplexKeyValue ( "lbdatekey" );
                		Object endkeyMatch = getComplexKeyValue ( "ubdatekey" );

                		// complex aggregate computes customer sales average, count, or sum over a random date range
				// intkey represents sales amount and stringkey represents customer name

                		db.aggregate(table,fieldNameMatch,startkeyMatch,endkeyMatch, aggregaterecordcount, 
				     fieldNameGroup, groupfunction, topNresults, new Vector<HashMap<String,Object>>());

				break;

			case "simple":
                		// simple aggregate computes all distinct intkeys over a randomly filtered record range
				db.aggregate(table,fieldNameGroup,len,new Vector<HashMap<String,Object>>());

				break;

			default:
                                throw new IllegalArgumentException("Invalid aggregate type " + aggregatetype);
				
		}
        }

	public void doTransactionUpdate(DB db)
	{
		//choose a random key
		int keynum = nextKeynum();

		String keyname=buildKeyName(keynum);

		HashMap<String,ByteIterator> values;

		if (writeallfields)
		{
		   //new data for all the fields
		   values = buildValues();
		}
		else
		{
		   //update a random field
		   values = buildUpdate();
		}

		db.update(table,keyname,values);
	}

	public void doTransactionInsert(DB db)
	{
		//choose the next key
		int keynum=transactioninsertkeysequence.nextInt();

		String dbkey = buildKeyName(keynum);

                if ( ! complexreads) {
                        HashMap<String, ByteIterator> values = buildValues();
                        db.insert(table,dbkey,values);
                } else {
                        HashMap<String, Object> cvalues = buildComplexValues();
                        db.complexinsert(table,dbkey,cvalues);
                }

	}
}
