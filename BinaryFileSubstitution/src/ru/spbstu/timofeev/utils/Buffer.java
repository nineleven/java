package ru.spbstu.timofeev.utils;

import ru.spbstu.pipeline.RC;

import java.nio.ByteBuffer;

public class Buffer {
    private byte[] data;
    private int bufferTop;

    public Buffer(int initialCapacity) {
        data = new byte[initialCapacity];
        bufferTop = 0;
    }

    public byte[] take() {

        byte[] copy = new byte[bufferTop];

        System.arraycopy(data, 0, copy, 0, bufferTop);

        bufferTop = 0;

        return copy;
    }

    public short[] takeShort() {
        ByteBuffer bb = ByteBuffer.wrap(data);
        short[] result = new short[bufferTop / 2];
        for (int i = 0; i < bufferTop / 2; ++i) {
            result[i] = bb.getShort(2*i);
        }

        bufferTop = 0;

        return result;
    }

    public RC put(byte[] newData) {
        if (newData == null) {
            return RC.CODE_INVALID_ARGUMENT;
        }
        return put(newData, 0, newData.length);
    }

    public RC put(byte[] newData, int offset, int length) {
        if (newData == null) {
            return RC.CODE_INVALID_ARGUMENT;
        }

        if (bufferTop + length > data.length) {
            extend(bufferTop + length);
        }

        System.arraycopy(newData, offset, data, 0, length);
        bufferTop = length;

        return RC.CODE_SUCCESS;
    }

    public boolean isEmpty() {
        return bufferTop == 0;
    }

    public int capacity() {
        return data.length;
    }

    private void extend(int requiredCapacity) {
        int newCapacity = requiredCapacity > 2 * data.length ? requiredCapacity : 2 * data.length;
        byte[] newData = new byte[newCapacity];
        System.arraycopy(data, 0, newData, 0, data.length);
        data = newData;
    }
}
