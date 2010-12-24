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


package baldrickv.s3streamingtool.dedup;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Collection;

import  baldrickv.s3streamingtool.Hash;

public class StreamingDedup
{
	private LinkedList<BlockInfo> manifest;

	private HashMap<Long,TreeMap<String, BlockInfo> > knownBlocks;

	private BlockWriter writer;

	private int blockSize;
	private Snake snake;
	private long writtenToSnake;

	private byte[] pendingWrite;
	private int pendingIdx;
	private boolean terminated;

	public StreamingDedup(int blockSize)
	{
		this.blockSize = blockSize;
		snake = new Snake(blockSize, 8);

		manifest = new LinkedList<BlockInfo>();
		knownBlocks = new HashMap<Long, TreeMap<String, BlockInfo> >();

		pendingWrite = new byte[blockSize];
		pendingIdx = 0;
		terminated = false;
	}

	public int getManifestUniqueBlockCount()
	{
		TreeSet<String> s = new TreeSet<String>();

		for(BlockInfo bi : manifest)
		{
			s.add(bi.getId());
		}
		return s.size();

	}

	public int getManifestSize()
	{
		return manifest.size();
	}

	public int getKnownBlockSigs()
	{
		return knownBlocks.size();
	
	}

	public LinkedList<BlockInfo> getManifest()
	{
		return manifest;
	}

	public void addKnownBlocks(Collection<BlockInfo> lst)
	{
		for(BlockInfo info : lst)
		{
			addKnownBlock(info);
		}
	}

	private void addKnownBlock(BlockInfo info)
	{
		long sig = info.getSig();
		String hash = info.getHash();

		TreeMap<String, BlockInfo> sigGroup = null;

		sigGroup = knownBlocks.get(sig);
		if (sigGroup == null)
		{
			sigGroup = new TreeMap<String, BlockInfo>();
			knownBlocks.put(sig, sigGroup);
		}

		sigGroup.put(hash, info);

	}
	public void setBlockWriter(BlockWriter writer)
	{
		this.writer = writer;
	}

	public void write(byte b)
	{
		byte old = snake.add(b);
		writtenToSnake++;

		if (writtenToSnake > blockSize)
		{
			pendingWrite[pendingIdx] = old;
			pendingIdx++;

			if (pendingIdx == blockSize)
			{
				writePending();
			}	
		}

		if (terminated) return;

		if (writtenToSnake >= blockSize)
		{
			long sig = snake.getSig();
			TreeMap<String, BlockInfo> sigGroup = knownBlocks.get(sig);

			if (sigGroup != null)
			{
				System.out.print("S");
				String hash = snake.getHash();
				BlockInfo bi = sigGroup.get(hash);
				if (bi != null)
				{
					System.out.print("H");
					writeReusedBlock(bi);
				}

			}
		}
	}

	public void write(byte[] buff)
	{
		for(byte b : buff)
		{
			write(b);
		}
	}
	public void complete()
	{
		int toFlush = 0;

		//If there is anything in the snake, we need to flush it out

		if (writtenToSnake > 0) toFlush = blockSize;

		terminated=true;
		for(int i=0; i<toFlush; i++)
		{
			byte b = 0;
			write(b);
		}
		writePending();

	}

	private void writeReusedBlock(BlockInfo bi)
	{
		writePending();

		manifest.add(bi);
		snake = new Snake(blockSize, 8);

		writtenToSnake = 0;
	}

	private void writePending()
	{
		if (pendingIdx == 0) return;

		String hash = Hash.hash(pendingWrite, 0, pendingIdx);

		Snake s = new Snake(blockSize, 8);

		for(int i=0; i<pendingIdx; i++)
		{
			s.add(pendingWrite[i]);
		}
		long sig = s.getSig();

		BlockInfo bi = new BlockInfo(sig, hash);
		writer.writeBlock(bi, pendingWrite, 0, pendingIdx);

		manifest.add(bi);

		addKnownBlock(bi);


		pendingIdx = 0;
	}

	

	

}
