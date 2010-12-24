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

import java.util.HashMap;
import java.util.LinkedList;

import baldrickv.s3streamingtool.dedup.BlockWriter;
import baldrickv.s3streamingtool.dedup.BlockReader;


import baldrickv.s3streamingtool.dedup.BlockInfo;

public class MemoryBlockWriter implements BlockWriter, BlockReader
{
	long totalSize;
	HashMap<String, Block> blocks;
	LinkedList<BlockInfo> knownBlocks;

	public MemoryBlockWriter()
	{
		totalSize=0;
		blocks = new HashMap<String, Block>();
		knownBlocks = new LinkedList<BlockInfo>();

	}

	public void writeBlock(BlockInfo info, byte[] block, int offset, int size)
	{
		//System.out.println("Write called for block " + info.getId() + " size " + size);

		totalSize += size;
		Block b = new Block(block, offset, size);
		blocks.put(info.getId(), b);

		knownBlocks.add(info);

	}

	public class Block
	{
		public Block(byte[] b, int offset, int  size)
		{
			buff = new byte[size];
			for(int i=0; i<size; i++)
			{
				buff[i] = b[offset + i];
			}
		}

		byte[] buff;
	}

	public byte[] readBlock(String id)
	{
		return blocks.get(id).buff;
	}

	public LinkedList<BlockInfo> getBlockInfo()
	{
		return knownBlocks;
	}

}

