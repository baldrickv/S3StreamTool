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

import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentials;


import java.util.logging.Logger;
import java.util.logging.Level;


import java.util.Scanner;

import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Anyone seeking understanding of this code, please don't look at this file.
 * This file is entirely command line processing nonsense.
 *
 * The real action is in S3StreamingUpload and S3StreamingDownload which are fed
 * a S3StreamConfig file with all the configuration parameters set.
 */
public class S3StreamingTool
{
	public static void main(String args[])
		throws Exception
	{
		BasicParser p = new BasicParser();

		Options o = getOptions();

		CommandLine cl = p.parse(o, args);

		if (cl.hasOption('h'))
		{
			HelpFormatter hf = new HelpFormatter();
			hf.setWidth(80);

			StringBuilder sb = new StringBuilder();

			sb.append("\n");
			sb.append("Upload:\n");
			sb.append("    -u -r creds -s 50M -b my_bucket -f hda1.dump -t 10\n");
			sb.append("Download:\n");
			sb.append("    -d -r creds -s 50M -b my_bucket -f hda1.dump -t 10\n");
			sb.append("Upload encrypted:\n");
			sb.append("    -u -r creds -z -k secret_key -s 50M -b my_bucket -f hda1.dump -t 10\n");
			sb.append("Download encrypted:\n");
			sb.append("    -d -r creds -z -k secret_key -s 50M -b my_bucket -f hda1.dump -t 10\n");
			sb.append("Cleanup in-progress multipart uploads\n");
			sb.append("    -c -r creds -b my_bucket\n");
			System.out.println(sb.toString());


			hf.printHelp("See above",o);

			return;
		}

		int n = 0;
		if (cl.hasOption('d')) n++;
		if (cl.hasOption('u')) n++;
		if (cl.hasOption('c')) n++;

		if (n != 1)
		{
			System.err.println("Must specify at exactly one of -d, -u or -c");
			System.exit(-1);
		}

		require(cl, 'b');
		
		if (cl.hasOption('d') || cl.hasOption('u'))
		{
			require(cl, 'f');
		}
		if (cl.hasOption('z'))
		{
			require(cl, 'k');
		}

		AWSCredentials creds = null;

		if (cl.hasOption('r'))
		{
			creds = Utils.loadAWSCredentails(cl.getOptionValue('r'));
		}
		else
		{
			if (cl.hasOption('i') && cl.hasOption('e'))
			{
				creds = new BasicAWSCredentials(cl.getOptionValue('i'), cl.getOptionValue('e'));
			}
			else
			{

				System.out.println("Must specify either credential file (-r) or AWS key ID and secret (-i and -e)");
				System.exit(-1);
			}
		}

		S3StreamConfig config = new S3StreamConfig();
		config.setEncryption(false);
		if (cl.hasOption('z'))
		{
			config.setEncryption(true);
			config.setSecretKey(Utils.loadSecretKey(cl.getOptionValue("e")));
		}

		if (cl.hasOption("encryption-mode"))
		{
			config.setEncryptionMode(cl.getOptionValue("encryption-mode"));
		}
		config.setS3Bucket(cl.getOptionValue("bucket"));
		if (cl.hasOption("file"))
		{
			config.setS3File(cl.getOptionValue("file"));
		}

		if (cl.hasOption("threads"))
		{
			config.setIOThreads(Integer.parseInt(cl.getOptionValue("threads")));
		}

		if (cl.hasOption("blocksize"))
		{
			String s = cl.getOptionValue("blocksize");
			s=s.toUpperCase();
			int multi=1;

			int end=0;
			while((end < s.length()) && (s.charAt(end) >= '0') && (s.charAt(end) <= '9'))
			{
				end++;
			}
			int size = Integer.parseInt(s.substring(0,end));
			

			if (end < s.length())
			{
				String m = s.substring(end);
				if (m.equals("K")) multi=1024;
				else if (m.equals("M")) multi=1048576;
				else if (m.equals("G")) multi=1024*1024*1024;
				else if (m.equals("KB")) multi=1024;
				else if (m.equals("MB")) multi=1048576;
				else if (m.equals("GB")) multi=1024*1024*1024;
				else
				{
					System.out.println("Unknown suffix on block size.  Only K,M and G understood.");
					System.exit(-1);
				}

			}
			size *= multi;
			config.setBlockSize(size);
		}

		Logger.getLogger("").setLevel(Level.WARNING);

		S3StreamingDownload.log.setLevel(Level.INFO);
		S3StreamingUpload.log.setLevel(Level.INFO);


		config.setS3Client(new AmazonS3Client(creds));

		if (cl.hasOption('c'))
		{
			S3CleanupMultipart.cleanup(config);
			return;
		}
		if (cl.hasOption('d'))
		{
			config.setOutputStream(System.out);
			S3StreamingDownload.download(config);
			return;
		}
		if (cl.hasOption('u'))
		{
			config.setInputStream(System.in);
			S3StreamingUpload.upload(config);
			return;
		}



	}

	private static void require(CommandLine cl, char opt)
	{
		if (!cl.hasOption(opt))
		{
			System.err.println("Missing required option: " + opt);
			System.exit(-1);
		}
	}


	public static Options getOptions()
	{
		Options o = new Options();
			
		o.addOption(new Option("h","help",false,"Display help"));
		o.addOption(new Option("z","encrypted",false,"Use encryption/decryption"));
		o.addOption(new Option("k","keyfile",true,"Location of encryption secret key"));
		o.addOption(new Option(null,"encryption-mode",true,"Encryption mode to use (default "+S3StreamConfig.DEFAULT_ENCRYPTION_MODE+")"));
		o.addOption(new Option("s","blocksize",true,"Block size (default "+S3StreamConfig.DEFAULT_BLOCK_MB+"MB)"));
		o.addOption(new Option("b","bucket",true,"S3 Bucket"));
		o.addOption(new Option("i","keyid",true,"AWS Key ID"));
		o.addOption(new Option("e","secret",true,"AWS Secret Key"));
		o.addOption(new Option("f","file",true,"S3 file name"));
		o.addOption(new Option("t","threads",true,"Number of IO threads to use (default "+S3StreamConfig.DEFAULT_IO_THREADS+")"));
		o.addOption(new Option("u","upload",false,"Upload from standard input"));
		o.addOption(new Option("d","download",false,"Download to standard output"));
		o.addOption(new Option("c","cleanup",false,"Interactively cleanup existing multipart uploads"));
		o.addOption(new Option("r","credfile",true,"Location of AWS credential file"));

		return o;


	}


}
