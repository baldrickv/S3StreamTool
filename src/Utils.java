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
