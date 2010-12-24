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

import java.math.BigInteger;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Base64;

public class Hash
{
	public static String hash(byte b[])
	{
		return hash(b, 0, b.length);
	}

	public static String hash(byte b[], int offset, int size)
	{
		return hash("SHA-256", 256, b, offset, size);
	}

	public static String hash(String algo, int output_bits, byte b[], int offset, int size)
	{
		try
		{
			int output_bytes = output_bits / 4; //hex = 4 bits per byte
			MessageDigest sig=MessageDigest.getInstance(algo);
		 	sig.update(b, offset, size);
			byte d[]=sig.digest();

		 	StringBuffer s=new StringBuffer(output_bytes);
		 	BigInteger bi=new BigInteger(1,d);
		 	s.append(bi.toString(16));
		 	while (s.length() < output_bytes)
		 	{
	     		s.insert(0,'0');
	     	}
		 	return s.toString();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
												             
	}

	/**
	 * Cause apparently a hex encoded MD5 isn't good enough for S3.
	 * They want a base64 encode of the raw binary hash.  blech.
	 */
	public static String getMd5ForS3(byte b[])
	{
		try
		{
		    MessageDigest sig=MessageDigest.getInstance("MD5");
			sig.update(b, 0, b.length);
			byte d[] = sig.digest();

			byte encoded[] = Base64.encodeBase64(d, false);

			return new String(encoded);

		}
		catch(Exception e)
		{

			e.printStackTrace();
			System.exit(-1);
			return null;
		}


	}

	




}

