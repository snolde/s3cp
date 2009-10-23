/**
 * 
 */
package com.snolde.s3tools.s3cp;

import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import java.util.ArrayList;
import java.io.*;


 /**
 * s3cp <i>source</i> <i>destination</i> <i>pubkey</i> <i>secretkey</i>  
 * @author snolde <stefan.nolde@lazos.cl>
 *
 */
public class S3cp {
	

	protected static Logger log = Logger.getLogger(S3cp.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String source = null;
		String target = null;
		String bucket = null;
		String pubkey = null;
		String secret = null;
		
		try {
			for (int i = 0;; i++) {
				if (args[i].startsWith("-")) {
					if (args[i].toLowerCase().equals("-p")){
						pubkey = args[++i];
						log.debug(pubkey + " " + String.valueOf(i));
					} else if (args[i].toLowerCase().equals("-s")){
						secret = args[++i];
						log.debug(secret + " " + String.valueOf(i));
					} else if (args[i].toLowerCase().equals("-b")){
						bucket = args[++i];
						log.debug(bucket + " " + String.valueOf(i));
					} else throw new RuntimeException("Unknown option[".concat(args[i]));
				} else {
					if (source==null){
						source = args[i];
						log.debug("source:" + source);
					} else if (target==null){
						target = args[i];
						log.debug("target:" + target);
					} else if (bucket==null){
						bucket = args[i];
						log.debug("bucket:" + bucket);
					}
				}
			}

		} catch (java.lang.ArrayIndexOutOfBoundsException iobe) {
			//expected behaviour
		} catch (Throwable t) {
			System.err.println("Unexpected termination. ".concat(t.getClass().getName()));
			t.printStackTrace();

			System.exit(10);

		} finally {
			try {
				if ((pubkey!=null) && (secret!=null) && (bucket!=null)){
					SimpleS3Connector simpleS3Connector = new SimpleS3Connector(pubkey,secret);
					S3Object object = null;
					
					if ((bucket==null) && ((source==null) || (target==null))){
	
						ArrayList<String> bucketList = simpleS3Connector.s3ll();
						System.out.println("Available Buckets: " + bucketList.toString());
	
					} else if ((bucket!=null) && ((source==null) || (target==null))){
	
						ArrayList<String> objectList = simpleS3Connector.s3ls(bucket);
						System.out.println("Object keys: " + objectList.toString());
	
					} else if (source!=null && target!=null && bucket!=null){
						
						if (source.startsWith("s3:") && !target.startsWith("s3:")){
							log.debug("get from s3: " + source.substring(3));
							object = simpleS3Connector.download(simpleS3Connector.getBucket(bucket),
									source.substring(3),
									new File(target));
							log.debug(object.getMetadataMap());
						} else if (!source.startsWith("s3:") && target.startsWith("s3:")){
							object = simpleS3Connector.upload(simpleS3Connector.getBucket(bucket),
									target.substring(3),
									new File(source));
							log.debug(object.getMetadataMap());
						}
					}
				}
			} catch (IOException ioe){
				log.error("Unexpected termination. ".concat(ioe.getClass().getName()));
				ioe.printStackTrace();
				System.exit(10);
			} catch (S3ServiceException s3se){
				log.error("Unexpected termination. ".concat(s3se.getClass().getName()));
				s3se.printStackTrace();

				System.exit(10);

			} catch (Exception e){
				log.error("Unexpected termination. ".concat(e.getClass().getName()));
				e.printStackTrace();

				System.exit(10);

			}
			
		}

		System.exit(0);

	}
	
	


}
