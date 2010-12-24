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

import baldrickv.s3streamingtool.dedup.Snake;

public class SnakeTest 
{
	private Random rnd;

	public SnakeTest()
	{
		rnd = new Random();
	}

	@Test
	public void testSimpleIdentity()
	{
		Snake a = new Snake(65536, 64);
		Snake b = new Snake(65536, 64);

		for(int i=0; i<128000; i++)
		{
			byte buff[]=new byte[1];
			rnd.nextBytes(buff);

			a.add(buff[0]);
			b.add(buff[0]);

			Assert.assertEquals(a.getSig(), b.getSig());
			
		}

	}

	@Test
	public void testDupAfterRandom()
	{
		for(int i=0; i<10; i++)
		{
		Snake a = new Snake(65536, 8);
		Snake b = new Snake(65536, 8);

		byte[] rnd_a = new byte[rnd.nextInt(65536)];
		byte[] rnd_b = new byte[rnd.nextInt(1048576)];
		byte[] dup = new byte[65536];

		rnd.nextBytes(rnd_a);
		rnd.nextBytes(rnd_b);
		rnd.nextBytes(dup);

		a.add(rnd_a);
		b.add(rnd_b);

		Assert.assertFalse(a.getSig() == b.getSig());

		a.add(dup);
		b.add(dup);

		Assert.assertEquals(a.getSig(), b.getSig());
		}
		
	}


	@Test
	public void testXor()
	{
		byte[] buff = new byte[1024];

		rnd.nextBytes(buff);

		byte x = 0;

		for(int i=0; i<1024; i++)
		{
			x = (byte)(x ^ buff[i]);
		}

		int n = x;
		for(int i=0; i<1024; i++)
		{

			x = (byte)(x ^ buff[i]);
		}

		Assert.assertEquals(x, 0);

	}

	

}
