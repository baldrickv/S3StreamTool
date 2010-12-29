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

import com.amazonaws.services.s3.model.PartETag;


class S3StreamingUploadThread extends Thread
{
    private S3StreamConfig config;
    private Semaphore read_allow;
    private BlockWeaver<PartETag> weaver;
    private LinkedBlockingQueue<DataBlock> queue;
	private String upload_id;

    public S3StreamingUploadThread(
		String upload_id,
        S3StreamConfig config,
        Semaphore read_allow,
        BlockWeaver<PartETag> weaver,
        LinkedBlockingQueue<DataBlock> queue)
    {
		this.upload_id = upload_id;
        this.config = config;
        this.read_allow = read_allow;
        this.weaver = weaver;
        this.queue = queue;

        this.setName("S3StreamUploadThread");
        this.setDaemon(false);

    }

    public void run()
    {
        while(true)
        {
            try
            {

                DataBlock db = queue.take();
				int block_no = db.getBlockNumber();

				PartETag part = S3StreamingUpload.put(
					config.getS3Client(),
					config.getS3Bucket(), 
					config.getS3File(), 
					upload_id, 
					block_no, 
					db.getData());

                weaver.addBlock(block_no, part );
                
				read_allow.release();

            }
            catch(InterruptedException e)
            {
                return;
            }

        }


    }
		


}
