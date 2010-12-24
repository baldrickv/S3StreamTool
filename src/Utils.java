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

import java.security.Key;

import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.Scanner;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentials;

public class Utils
{
	public static Key loadSecretKey(String file_path)
		throws java.io.IOException
	{
		
		DataInputStream fis = new DataInputStream(new FileInputStream(file_path));

		int len = (int)new File(file_path).length();

		byte[] block = new byte[len];

		fis.readFully(block);
		fis.close();

		return new SecretKeySpec(block, "AES");

	}


	public static AWSCredentials loadAWSCredentails(String file_path)
		throws java.io.IOException
	{
		FileInputStream fin = new FileInputStream(file_path);

		Scanner scan = new Scanner(fin);

		String id = scan.next();
		String key = scan.next();

		fin.close();

		return new BasicAWSCredentials(id, key);

	}
}
