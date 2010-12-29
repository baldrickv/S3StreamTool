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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import java.util.logging.Logger;

class S3StreamingDownloadThread extends Thread
{
	private static Logger log = Logger.getLogger(S3StreamingDownload.class.getName());

	private S3StreamConfig config;
	private Semaphore read_allow;
	private BlockWeaver<DataBlock> weaver;
	private LinkedBlockingQueue<DownloadRequest> queue;

	public S3StreamingDownloadThread(
		S3StreamConfig config, 
		Semaphore read_allow, 
		BlockWeaver<DataBlock> weaver, 
		LinkedBlockingQueue<DownloadRequest> queue)
	{
		this.config = config;
		this.read_allow = read_allow;
		this.weaver = weaver;
		this.queue = queue;

		this.setName("S3StreamDownloadThread");
		this.setDaemon(false);

	}

	public void run()
	{
		while(true)
		{
			
			try
			{
				read_allow.acquire();

				DownloadRequest req = queue.take();

				byte[] data;
				
				data = S3StreamingDownload.get(config.getS3Client(), config.getS3Bucket(), config.getS3File(), req.start, req.end);


				DataBlock db = new DataBlock(req.block_no, data);
				weaver.addBlock(req.block_no, db);



			}
			catch(InterruptedException e)
			{
				
				return;
			}

		}


	}

}
