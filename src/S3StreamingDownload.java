/*
Copyright (c) 2010 baldrickv

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/


package baldrickv.s3streamingtool;

import com.amazonaws.auth.AWSCredentials;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.util.Random;
import java.io.DataInputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;

import java.util.concurrent.Semaphore;
import java.util.concurrent.LinkedBlockingQueue;

import javax.crypto.Cipher;
import java.security.Key;
import javax.crypto.spec.IvParameterSpec;
import java.nio.ByteBuffer;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.LinkedList;

public class S3StreamingDownload
{
	protected static Logger log = Logger.getLogger(S3StreamingDownload.class.getName());

    public static void main(String args[])
		throws Exception
    {
		if (args.length != 5)
		{
			System.out.println("Args: S3StreamingDownload bucket s3_file_name block_size key_path aws_creds_path");
			System.exit(-1);
		}

		String bucket = args[0];
		String file = args[1];
		int block_size = Integer.parseInt(args[2]);
		String key_path = args[3];
		String aws_creds_path = args[4];

        Logger.getLogger("").setLevel(Level.WARNING);
 		log.setLevel(Level.FINE);

        Key secret_key = Utils.loadSecretKey(key_path);
        AWSCredentials creds = Utils.loadAWSCredentails(aws_creds_path);

		download(System.out, bucket, file, block_size, secret_key, creds);

    }

	public static void download(OutputStream out, String bucket, String file, int block_size, Key secret_key, AWSCredentials creds)
		throws Exception
	{
		S3StreamConfig config = new S3StreamConfig();
		config.setOutputStream(out);
		config.setS3Bucket(bucket);
		config.setS3File(file);
		config.setBlockSize(block_size);
		config.setSecretKey(secret_key);
		config.setEncryption(true);
		config.setS3Client(new AmazonS3Client(creds));

		download(config);
	}
	public static void download(S3StreamConfig config)
		throws Exception
	{

   		long start_time = System.currentTimeMillis();

		Cipher cipher = null;
		Key secret_key = null;
		int block_size = config.getBlockSize();
		OutputStream out = config.getOutputStream();

		if (config.getEncryption())
		{

			cipher = Cipher.getInstance(config.getEncryptionMode());

			secret_key = config.getSecretKey();

			int key_size = secret_key.getEncoded().length;
		
			//There would be some trifling code of the block size wasn't at least the key size
			block_size = Math.max(block_size, key_size);
		}


		long readOffset=0;
		long totalLen;

		ObjectMetadata omd = config.getS3Client().getObjectMetadata(config.getS3Bucket(), config.getS3File());

		totalLen = omd.getContentLength();
		
		LinkedBlockingQueue<DownloadRequest> queue = new LinkedBlockingQueue<DownloadRequest>();
		
		int block_count = 0;
		while(readOffset < totalLen)
		{
			long start = readOffset;
			long end = Math.min(totalLen, start + block_size);

			DownloadRequest dr = new DownloadRequest();
			dr.block_no = block_count;
			dr.start = start;
			dr.end = end;
			queue.put(dr);

			block_count++;

			readOffset += (end - start);
		}

		LinkedList<S3StreamingDownloadThread> threads = new LinkedList<S3StreamingDownloadThread>();
		Semaphore read_allow = new Semaphore(config.getIOThreads());
		BlockWeaver<DataBlock> weaver = new BlockWeaver<DataBlock>();

		for(int i=0; i<config.getIOThreads(); i++)
		{
			S3StreamingDownloadThread t = new S3StreamingDownloadThread(config, read_allow, weaver, queue);
			t.start();
			threads.add(t);
		}

		long total_size = 0;

		boolean first_block=true;

		for(int block = 0; block < block_count; block++)
		{

			byte[] s3_data = weaver.getNextBlock().getData();

			byte[] plain;

			if (cipher==null)
			{
				plain = s3_data;
			}
			else if (first_block)
			{

				IvParameterSpec iv_spec = new IvParameterSpec(s3_data,0,16);
				cipher.init(Cipher.DECRYPT_MODE, secret_key, iv_spec);
				plain = cipher.update(s3_data, 16, s3_data.length - 16);
				
				first_block=false;
			}
			else
			{
				plain = cipher.update(s3_data);
			}

			if (plain != null)
			{
				total_size += plain.length;
				out.write(plain);
				out.flush();
			}

			read_allow.release();

		}

		if (cipher != null)
		{
			byte[] plain = cipher.doFinal();

			if (plain != null)
			{
				total_size += plain.length;
				out.write(plain);
				out.flush();
			}
		}

		for(S3StreamingDownloadThread t : threads)
		{
			t.interrupt();
			t.join();
		}

   		long end_time = System.currentTimeMillis();

		double seconds = (double)(end_time - start_time)/1000.0;
		double rate = (double)total_size / seconds;
		double rate_kb = rate / 1024.0;
		DecimalFormat df = new DecimalFormat("0.00");

		log.info("Downloaded " + total_size + " at rate of " + df.format(rate_kb) + " kB/s");
	
	}


	/**
	 * S3 uses an inclusive range (on both ends).
	 * This call however uses a more standard, inclusive on the start and exclusive on the end.
	 * so returns all the bytes X such that (start <= X < end)
	 */
	protected static byte[] get(AmazonS3Client s3, String bucket, String file, long start, long end) 
	{
        long t1 = System.nanoTime();
		while(true)
		{
			try
			{
				log.log(Level.FINE, "Started " + file + " " + start + " " + end);

				GetObjectRequest req = new GetObjectRequest(bucket, file);

				req.setRange(start, end - 1);
				S3Object obj = s3.getObject(req); 
				
				int len = (int)obj.getObjectMetadata().getContentLength();
				byte[] b=new byte[len];
				DataInputStream din = new DataInputStream(obj.getObjectContent());

				din.readFully(b);
				din.close();


		        long t2 = System.nanoTime();

		        double seconds = (double)(t2 - t1) / 1000000.0 / 1000.0;
        		double rate = (double)len / seconds / 1024.0;

		        DecimalFormat df = new DecimalFormat("0.00");

        		log.log(Level.FINE,file + " " + start + " " + end  + " size: " + len + " in " + df.format(seconds) + " sec, " + df.format(rate) + " kB/s");

				return b;



			}
			catch(Throwable t)
			{
				log.log(Level.WARNING, "Error in download", t);
				try{Thread.sleep(5000);}catch(Exception e){}
			}
		}

	}

}
