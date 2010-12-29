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

import java.io.InputStream;
import java.io.OutputStream;

import java.security.Key;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3StreamConfig
{
	private AmazonS3Client s3_client;
	private String s3_bucket;
	private String s3_file;

	private boolean encryption=false;
	private String encryption_mode="AES/CBC/PKCS5PADDING";
	private Key secret_key;

	private int block_size=104857600; //100mb
	private int io_threads=2;

	private InputStream source; //Only applies to upload
	private OutputStream destination; //Only applies to download

	public void setS3Client(AmazonS3Client c){s3_client = c;}
	public void setS3Bucket(String s){s3_bucket = s;}
	public void setS3File(String s){s3_file = s;}
	public void setEncryption(boolean e){encryption = e;}
	public void setEncrypotionMode(String m){encryption_mode = m;}
	public void setSecretKey(Key k){secret_key = k;}
	public void setBlockSize(int z){block_size = z;}
	public void setIOThreads(int n){io_threads = n;}

	public void setInputStream(InputStream in){source = in;}
	public void setOutputStream(OutputStream out){destination = out;}


	public AmazonS3Client getS3Client(){return s3_client;}
	public String getS3Bucket(){return s3_bucket;}
	public String getS3File(){return s3_file;}
	public boolean getEncryption(){return encryption;}
	public String getEncryptionMode(){return encryption_mode;}
	public Key getSecretKey(){return secret_key;}
	public int getBlockSize(){return block_size;}
	public int getIOThreads(){return io_threads;}

	public InputStream getInputStream(){return source;}
	public OutputStream getOutputStream(){return destination;}



}
