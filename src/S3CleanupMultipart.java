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
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.List;
import java.util.Scanner;

public class S3CleanupMultipart
{

    public static void main(String args[])
		throws Exception
    {
		if (args.length != 2)
		{
			System.out.println("Args: S3CleanupMultipart bucket aws_creds_path");
			System.exit(-1);
		}

		String bucket = args[0];
		String aws_creds_path = args[1];

        Logger.getLogger("").setLevel(Level.WARNING);

        AWSCredentials creds = Utils.loadAWSCredentails(aws_creds_path);

		cleanup(bucket, creds);



    }

	public static void cleanup(String bucket, AWSCredentials creds)
		throws Exception
	{

        AmazonS3Client s3 = new AmazonS3Client(creds);

		ListMultipartUploadsRequest list_req = new ListMultipartUploadsRequest(bucket);

		List<MultipartUpload> list = s3.listMultipartUploads(list_req).getMultipartUploads();

		Scanner scan = new Scanner(System.in);

		for(MultipartUpload mu : list)
		{
			System.out.println("-----------------------");
			System.out.println("  bucket: " + bucket);
			System.out.println("  key: " + mu.getKey());
			System.out.println("  uploadId: " + mu.getUploadId());
			System.out.println("  initiated at: " + mu.getInitiated());
			System.out.println("  initiated by: " + mu.getInitiator());
			System.out.println("-----------------------");

			System.out.print("Abort this upload [y|N]? ");
			String result = scan.nextLine().trim().toLowerCase();
			if (result.equals("y"))
			{
				AbortMultipartUploadRequest abort = new AbortMultipartUploadRequest(bucket, mu.getKey(), mu.getUploadId());

				s3.abortMultipartUpload(abort);
				System.out.println("Aborted upload");
			}
			else
			{
				System.out.println("Leaving this one alone");

			}



		}






	}


	

}
