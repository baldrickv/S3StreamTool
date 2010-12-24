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


import java.util.Random;

import java.nio.ByteBuffer;

import java.util.HashMap;

import java.text.DecimalFormat;
import java.io.ByteArrayOutputStream;

import org.junit.Assert;
import org.junit.Test;


import baldrickv.s3streamingtool.dedup.StreamingDedup;
import baldrickv.s3streamingtool.dedup.BlockInfo;
import baldrickv.s3streamingtool.Hash;

public class DedupTest
{

	Random rnd;

	public DedupTest()
		throws Exception
	{
		rnd = new Random();
		setupBlockStream();

	}

	byte[][] block_list;

	int next_block;
	int next_idx;

	int block_count = 200;

	public void setupBlockStream()
	{
		block_list=new byte[block_count][];

		next_block = -1;
		next_idx = 0;


		for(int i=0; i<block_count; i++)
		{
			int min = 128;
			int max = 8192;
			block_list[i]= new byte[min + rnd.nextInt(max)];
			rnd.nextBytes( block_list[i]);

		}


	}

	public byte getNextByte()
	{
		if (next_block == -1)
		{
			next_block = rnd.nextInt(block_count);
			next_idx = 0;
		}

		byte b = block_list[next_block][next_idx];

		next_idx++;
		if (next_idx == block_list[next_block].length)
		{
			next_block = -1;
		}

		return b;

	}

	
	@Test
	public void testDedupStream()
		throws Exception
	{
		int block_size = 1024;

		MemoryBlockWriter blockMemory = new MemoryBlockWriter();


		long start = System.currentTimeMillis();


		for(int i=0; i<20; i++)
		{
			StreamingDedup sd = new StreamingDedup(block_size);
			sd.setBlockWriter(blockMemory);
			sd.addKnownBlocks(blockMemory.getBlockInfo());
		
			int read_idx = 0;
			int fin = 1024 * 1024 + rnd.nextInt(1024*1024);

			ByteArrayOutputStream input=new ByteArrayOutputStream(fin);

			while(read_idx < fin)
			{
				byte n = getNextByte();
				sd.write(n);
				input.write(n);
				read_idx++;
				if (read_idx % block_size == 0)
				{
					System.out.print(".");
				}
			}

			sd.complete();
			long blocks = fin / block_size;

			System.out.println();
			System.out.println("Blocks: " + blocks);
			System.out.println("Unique blocks: " + sd.getManifestUniqueBlockCount());
			System.out.println("Manifest len: " + sd.getManifestSize());
			System.out.println("Known sigs: " + sd.getKnownBlockSigs());


			DecimalFormat df = new DecimalFormat("0.00");


			long end = System.currentTimeMillis();

			double seconds = (double)(end - start) / 1000.0;
			double rate = fin / 1024.0 / 1024.0 / seconds;

			System.out.println("Rate: " + df.format(rate) + " MB/s");

			byte[] input_bytes = input.toByteArray();

			ByteArrayOutputStream output = new ByteArrayOutputStream();

			for(BlockInfo bi : sd.getManifest())
			{
				output.write(blockMemory.readBlock(bi.getId()));
			}
			byte[] output_bytes = output.toByteArray();

			Assert.assertEquals(Hash.hash(input_bytes),Hash.hash(output_bytes));

		}

		


	}

}
