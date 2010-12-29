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

import java.util.TreeMap;

/**
 * If multiple readers are putting in blocks, possibly out of order this collects them all
 * back in the right order
 */
class BlockWeaver<T>
{
	private TreeMap<Integer, T> blocks;
	private int nextBlock;

	public BlockWeaver()
	{
		this(0);
	}

	public BlockWeaver(int first_block)
	{
		blocks = new TreeMap<Integer, T>();
		nextBlock = first_block;
	}

	/**
	 * This is how a reader hands off a block to be processed
	 */
	public synchronized void addBlock(int block_no, T block)
	{
		blocks.put(block_no, block);
		this.notifyAll();
	}

	/**
	 * This allows the processor to get the next block in order, possibly waiting
	 * if needed
	 */
	public synchronized T getNextBlock()
		throws InterruptedException
	{
		while(true)
		{
			if (blocks.containsKey(nextBlock))
			{
				T db = blocks.get(nextBlock);

				blocks.remove(nextBlock);

				nextBlock++;
				return db;
			}
			this.wait();

		}
	}
	

}


