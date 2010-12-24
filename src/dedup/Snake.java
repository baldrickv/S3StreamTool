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
import java.nio.ByteBuffer;

import  baldrickv.s3streamingtool.Hash;

public class Snake
{
	ByteList list;
	ByteList actual_bytes;
	int block_size;
	int sig_size;

	public Snake(int block_size, int sig_size)
	{
		assert(block_size % sig_size == 0);

		list = new ByteList(sig_size);
		actual_bytes = new ByteList(block_size);

		this.block_size = block_size;
		this.sig_size = sig_size;

	}

	public byte add(byte b)
	{

		byte old = actual_bytes.replace(b);

		byte x = list.get();

		x = (byte)(x ^ b);
		x = (byte)(x ^ old);

		list.replace(x);

		return old;

	}

	public void add(byte[] buff)
	{
		for(byte b : buff)
		{
			add(b);
		}
	}

	public long getSig()
	{
		return list.getSig();
	}

	public String getHash()
	{
		return Hash.hash(actual_bytes.getBytes());

	}

	public class ByteList
	{
		private byte[] list;
		private int size;
		private int loc;

		private byte[] copy;
		private ByteBuffer bb;

		public ByteList(int size)
		{
			list = new byte[size];

			copy = new byte[size];
			bb = ByteBuffer.wrap(copy);


			this.size = size;
			this.loc = 0;
		}

		public byte replace(byte b)
		{
			byte old = list[loc];

			list[loc] = b;
			loc = (loc + 1) % size;

			return old;
		}

		public byte get()
		{
			return list[loc];
		}


		public long getSig()
		{
			updateCopy();
			bb.rewind();
			return bb.getLong();
		}

		public byte[] getBytes()
		{
			updateCopy();
			return copy;

		}

		private void updateCopy()
		{

			for(int i=0; i<size; i++)
			{
				int idx = (i + loc) % size;
				copy[i] = list[idx];
			}
		}

	}

}
