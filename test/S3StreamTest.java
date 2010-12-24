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



import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.Properties;

import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.security.Key;

import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import baldrickv.s3streamingtool.S3StreamingDownload;
import baldrickv.s3streamingtool.S3StreamingUpload;
import baldrickv.s3streamingtool.Hash;

public class S3StreamTest
{
	Random rnd;
	Key secret_key;
	AWSCredentials creds;
	String bucket;

	public S3StreamTest()
		throws Exception
	{

		rnd = new Random();

		byte key_data[]=new byte[16];
		rnd.nextBytes(key_data);

		secret_key = new SecretKeySpec(key_data, "AES");

		Properties test_config = new Properties();

		test_config.load(new FileInputStream("s3streamtest.txt"));

		Assert.assertTrue(test_config.containsKey("aws_id"));
		Assert.assertTrue(test_config.containsKey("aws_secret"));
		Assert.assertTrue(test_config.containsKey("test_bucket"));

		creds=new BasicAWSCredentials(
			test_config.getProperty("aws_id").trim(), 
			test_config.getProperty("aws_secret").trim());

		bucket = test_config.getProperty("test_bucket").trim();


	}


	@Test
	public void testStreamSmall()
		throws Exception
	{
		testRandomSize("s3-stream-test-small", 371, 10485760, 10485760);

	}

	@Test
	public void testStreamVerySmall()
		throws Exception
	{
		testRandomSize("s3-stream-test-very-small", 5, 10485760, 10485760);
	}

	@Test
	public void testStreamLarge()
		throws Exception
	{
		testRandomSize("s3-stream-test-large", 30*1048576, 10485760, 10485760);

	}
	@Test
	public void testStreamLargeMinusOne()
		throws Exception
	{
		testRandomSize("s3-stream-test-large-1", 30*1048576-16, 10485760, 10485760);

	}

	@Test
	public void testStreamSmallReadSize()
		throws Exception
	{
		testRandomSize("s3-stream-test-small-read", 2348, 10485760, 1);

	}

	



	private void testRandomSize(String name, int data_size, int write_size, int read_size)
		throws Exception
	{
		System.out.println(name);
		System.out.flush();
		byte input[] = new byte[data_size];

		rnd.nextBytes(input);

		ByteArrayInputStream in = new ByteArrayInputStream(input);

		S3StreamingUpload.upload(in, bucket, "test/" + name, write_size, secret_key, creds);

		ByteArrayOutputStream out = new ByteArrayOutputStream();

		S3StreamingDownload.download(out, bucket, "test/" + name, read_size, secret_key, creds);

		byte[] output = out.toByteArray();

		Assert.assertEquals(input.length, output.length);
		Assert.assertEquals(Hash.hash(input), Hash.hash(output));
		
	}




}
