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

import javax.crypto.Cipher;
import java.security.Key;
import javax.crypto.spec.IvParameterSpec;
import java.nio.ByteBuffer;

import java.util.logging.Logger;
import java.util.logging.Level;

public class S3StreamingDownload
{

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

        Key secret_key = Utils.loadSecretKey(key_path);
        AWSCredentials creds = Utils.loadAWSCredentails(aws_creds_path);

		download(System.out, bucket, file, block_size, secret_key, creds);



    }

	public static void download(OutputStream out, String bucket, String file, int block_size, Key secret_key, AWSCredentials creds)
		throws Exception
	{

        AmazonS3Client s3 = new AmazonS3Client(creds);

		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");

		int key_size = secret_key.getEncoded().length;
		
		//There would be some trifling code of the block size wasn't at least the key size
		block_size = Math.max(block_size, key_size);


		long readOffset=0;
		long totalLen;

		ObjectMetadata omd = s3.getObjectMetadata(bucket, file);

		totalLen = omd.getContentLength();
		
		boolean first_block=true;


		while(readOffset < totalLen)
		{
			long start = readOffset;
			long end = Math.min(totalLen, start + block_size);

			byte[] s3_data = get(s3, bucket, file, start, end);

			byte[] plain;

			if (first_block)
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
				out.write(plain);
				out.flush();
			}

			readOffset += s3_data.length;

		}

		byte[] plain = cipher.doFinal();

		if (plain != null)
		{
			out.write(plain);
			out.flush();
		}


	}


	

	/**
	 * S3 uses an inclusive range (on both ends).
	 * This call however uses a more standard, inclusive on the start and exclusive on the end.
	 * so returns all the bytes X such that (start <= X < end)
	 */
	private static byte[] get(AmazonS3Client s3, String bucket, String file, long start, long end) 
	{
		while(true)
		{
			try
			{

				GetObjectRequest req = new GetObjectRequest(bucket, file);

				req.setRange(start, end - 1);
				S3Object obj = s3.getObject(req); 
				
				int len = (int)obj.getObjectMetadata().getContentLength();
				byte[] b=new byte[len];
				DataInputStream din = new DataInputStream(obj.getObjectContent());

				din.readFully(b);
				din.close();

				return b;
			}
			catch(Throwable t)
			{
				t.printStackTrace();
				try{Thread.sleep(5000);}catch(Exception e){}
			}
		}

	}

}
