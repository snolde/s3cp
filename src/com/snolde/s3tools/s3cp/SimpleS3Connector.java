/**
 * 
 */
package com.snolde.s3tools.s3cp;

/**
 * Main working class managing authentication and selection of the needed copy method
 * 
 * @author snolde <stefan.nolde@lazos.cl>
 *
 */

import org.apache.log4j.Logger;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import java.util.ArrayList;
import java.io.*;
import java.security.NoSuchAlgorithmException;

public class SimpleS3Connector {
	
	protected static Logger log = Logger.getLogger(SimpleS3Connector.class);

	public String awsPubKey;
	
	public String awsSecretKey;
	
	public AWSCredentials awsCredentials;
	
	public RestS3Service s3Service;
	
	/**
	 * @param awsPubKey
	 * @param awsSecretKey
	 */
	public SimpleS3Connector(String awsPubKey, String awsSecretKey) throws S3ServiceException{
		super();
		this.awsPubKey = awsPubKey;
		this.awsSecretKey = awsSecretKey;
		this.awsCredentials = new AWSCredentials(awsPubKey, awsSecretKey);
		this.s3Service = new RestS3Service(awsCredentials);
		
	}

	/**
	 * @return the s3Service
	 */
	public RestS3Service getService() {
		return s3Service;
	}

	/**
	 * returns null if bucket not owned
	 * @return the s3Service
	 */
	public S3Bucket getBucket(String bucketName) throws S3ServiceException{
		S3Bucket s3Bucket = this.s3Service.getBucket(bucketName);
		return s3Bucket;
	}
	
	/**
	 * lists available buckets
	 * @return bucket list
	 */
	public ArrayList<String> s3ll() throws S3ServiceException{
		final S3Bucket[] buckets = this.s3Service.listAllBuckets();
		ArrayList<String> bucketList = new ArrayList<String>();
		try{
			for (int i = 0;; i++) {
				bucketList.add(buckets[i].getName());
			}			
		} catch (java.lang.ArrayIndexOutOfBoundsException iobe) {
			// expected behaviour
		}
		return bucketList;
	}

	/**
	 * lists Objects in a bucket
	 * @return the s3Service
	 */
	public ArrayList<String> s3ls(String bucketName) throws S3ServiceException{
		final S3Object[] objects = this.s3Service.listObjects(this.s3Service.getBucket(bucketName));
		ArrayList<String> objectList = new ArrayList<String>();
		try{
			for (int i = 0;; i++) {
				objectList.add(objects[i].getKey());
			}			
		} catch (java.lang.ArrayIndexOutOfBoundsException iobe) {
			// expected behaviour
		}
		return objectList;
	}
		
	/**
	 * @param bucket
	 * @param objectKey
	 * @param outFile
	 * @return
	 * @throws IOException
	 * @throws S3ServiceException
	 */
	public S3Object download(S3Bucket bucket,String objectKey, File outFile) throws IOException,S3ServiceException{
		S3Object object = getService().getObject(bucket, objectKey);
		log.debug("Object key: " + object.getKey());
		InputStream in = object.getDataInputStream();
		FileOutputStream out = new FileOutputStream(outFile);
		long stime = System.currentTimeMillis();
		log.debug("start: " + String.valueOf(stime));
		byte[] buf = new byte[1024];
		int len;
		while ((len = in.read(buf)) > 0){
			out.write(buf, 0, len);
		}
		in.close();
		out.close();
		long stop = System.currentTimeMillis();
		log.debug("stop:" +  String.valueOf(stop) + " dT: " + String.valueOf(stop-stime));
		return object;
	}

	public S3Object upload(S3Bucket bucket,String objectKey, File inFile)
			throws IOException,S3ServiceException,NoSuchAlgorithmException{
		S3Object object = new S3Object(bucket,inFile);
		object.setKey(objectKey);
		object.setContentLength(inFile.length());
		log.debug("Object key: " + object.getKey());
		long stime = System.currentTimeMillis();
		log.debug("start: " + String.valueOf(stime));
		this.getService().putObject(bucket, object);
		long stop = System.currentTimeMillis();
		log.debug("stop:" +  String.valueOf(stop) + " dT: " + String.valueOf(stop-stime));
		return object;
	}

}
