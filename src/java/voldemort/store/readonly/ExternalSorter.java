/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.readonly;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.serialization.Serializer;
import voldemort.utils.ByteUtils;
import voldemort.utils.DefaultIterable;

import com.google.common.collect.AbstractIterator;

/**
 * Do an external sort on data coming from an input iterator and produce a new
 * iterator over the values in sorted order
 * 
 * @author jay
 * 
 * @param <V> The type of value being sorted
 */
public class ExternalSorter<V> {

    public static final Logger logger = Logger.getLogger(ExternalSorter.class);

    private final Serializer<V> serializer;
    private final Comparator<V> comparator;
    private final int internalSortSize;
    private final File tempDir;
    private final int bufferSize;
    private final int numThreads;

    /**
     * Create an external sorter using the given serializer and internal sort
     * size.
     * 
     * Use natural ordering, system temp dir, and reasonable buffer size
     * 
     * @param serializer The serializer used to write data to disk
     * @param internalSortSize The number of objects in the internal sort buffer
     */
    @SuppressWarnings("unchecked")
    public ExternalSorter(Serializer<V> serializer, int internalSortSize, int numThreads) {
        this(serializer, new Comparator<V>() {

            public int compare(V o1, V o2) {
                Comparable c1 = (Comparable) o1;
                Comparable c2 = (Comparable) o2;
                return c1.compareTo(c2);
            }
        }, internalSortSize, System.getProperty("java.io.tmpdir"), 10 * 1024 * 1024, numThreads);
    }

    /**
     * Create an external sorter using the given serializer and internal sort
     * size.
     * 
     * Use natural ordering, system temp dir, and reasonable buffer size
     * 
     * @param serializer The serializer used to write data to disk
     * @param comparator The comparator used to order the objects
     * @param internalSortSize The number of objects to keep in the internal
     *        memory
     */
    public ExternalSorter(Serializer<V> serializer,
                          Comparator<V> comparator,
                          int internalSortSize,
                          int numThreads) {
        this(serializer,
             comparator,
             internalSortSize,
             System.getProperty("java.io.tmpdir"),
             10 * 1024 * 1024,
             numThreads);
    }

    /**
     * Create an external sorter using the given serializer and internal sort
     * size.
     * 
     * Use natural ordering, system temp dir, and reasonable buffer size
     * 
     * @param serializer The serializer used to write data to disk
     * @param comparator The comparator used to order the objects
     * @param internalSortSize The number of objects to keep in the internal
     *        memory
     * @param tempDir The temporary directory to which to write temporary data
     * @param bufferSize The IO buffer size
     */
    public ExternalSorter(Serializer<V> serializer,
                          Comparator<V> comparator,
                          int internalSortSize,
                          String tempDir,
                          int bufferSize,
                          int numThreads) {
        this.serializer = serializer;
        this.comparator = comparator;
        this.internalSortSize = internalSortSize;
        this.tempDir = new File(tempDir);
        this.bufferSize = bufferSize;
        this.numThreads = numThreads;
    }

    /**
     * Produce an iterator over the input values in sorted order. Sorting will
     * occur in the fixed space configured in the constructor, data will be
     * dumped to disk as necessary.
     * 
     * @param input An iterator over the input values
     * @return An iterator over the values
     */
    public Iterable<V> sorted(Iterator<V> input) {
        ExecutorService executor = new ThreadPoolExecutor(this.numThreads,
                                                          this.numThreads,
                                                          1000L,
                                                          TimeUnit.MILLISECONDS,
                                                          new SynchronousQueue<Runnable>(),
                                                          new CallerRunsPolicy());
        final AtomicInteger count = new AtomicInteger(0);
        final List<File> tempFiles = Collections.synchronizedList(new ArrayList<File>());
        while(input.hasNext()) {
            final int chunkId = count.getAndIncrement();
            final long chunkStart = System.currentTimeMillis();
            logger.info("Chunk " + chunkId + ": filling sort buffer for chunk...");
            @SuppressWarnings("unchecked")
            final V[] buffer = (V[]) new Object[internalSortSize];
            int chunkSizeIter = 0;
            for(; chunkSizeIter < internalSortSize && input.hasNext(); chunkSizeIter++)
                buffer[chunkSizeIter] = input.next();
            final int chunkSize = chunkSizeIter;
            logger.info("Chunk " + chunkId + ": sort buffer filled...adding to sort queue.");

            // sort and write out asynchronously
            executor.execute(new Runnable() {

                public void run() {
                    logger.info("Chunk " + chunkId + ": sorting buffer.");
                    long start = System.currentTimeMillis();
                    Arrays.sort(buffer, 0, chunkSize, comparator);
                    long ellapsed = System.currentTimeMillis() - start;
                    logger.info("Chunk " + chunkId + ": sort completed in " + ellapsed
                                + " ms, writing to temp file.");

                    // write out values to a temp file
                    try {
                        File tempFile = File.createTempFile("chunk-", ".dat", tempDir);
                        tempFile.deleteOnExit();
                        tempFiles.add(tempFile);
                        DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile),
                                                                                                bufferSize));
                        for(int i = 0; i < chunkSize; i++)
                            writeValue(output, buffer[i]);
                        output.close();
                    } catch(IOException e) {
                        throw new VoldemortException(e);
                    }
                    long chunkEllapsed = System.currentTimeMillis() - chunkStart;
                    logger.info("Chunk " + chunkId + ": completed processing of chunk in "
                                + chunkEllapsed + " ms.");
                }
            });
        }

        // wait for all sorting to complete
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            // create iterator over sorted values
            return new DefaultIterable<V>(new ExternalSorterIterator(tempFiles, bufferSize
                                                                                / tempFiles.size()));
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeValue(DataOutputStream stream, V value) {
        byte[] bytes = serializer.toBytes(value);
        try {
            stream.writeInt(bytes.length);
            stream.write(bytes);
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    private V readValue(DataInputStream stream) throws EOFException {
        try {
            int size = stream.readInt();
            byte[] bytes = new byte[size];
            ByteUtils.read(stream, bytes);
            return serializer.toObject(bytes);
        } catch(EOFException e) {
            throw e;
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    private final class ExternalSorterIterator extends AbstractIterator<V> implements Iterator<V> {

        private final List<FileAndStream> inputs;
        private final PriorityQueue<Item> minHeap;

        public ExternalSorterIterator(List<File> files, int readBufferSize) {
            this.inputs = new ArrayList<FileAndStream>(files.size());
            for(File f: files) {
                try {
                    DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(f),
                                                                                              readBufferSize));
                    this.inputs.add(new FileAndStream(f, inputStream));
                } catch(IOException e) {
                    throw new VoldemortException(e);
                }
            }
            this.minHeap = new PriorityQueue<Item>(inputs.size());
            for(int i = 0; i < inputs.size(); i++) {
                FileAndStream fas = inputs.get(i);
                try {
                    V v = readValue(fas.getInputStream());
                    minHeap.add(new Item(i, v));
                } catch(EOFException e) {
                    fas.closeAndDelete();
                }
            }
        }

        @Override
        protected V computeNext() {
            if(minHeap.peek() == null)
                return endOfData();

            Item curr = minHeap.poll();
            // read replacement item
            FileAndStream fas = inputs.get(curr.getIndex());
            try {
                V v = readValue(fas.getInputStream());
                if(v != null)
                    minHeap.add(new Item(curr.getIndex(), v));
            } catch(EOFException e) {
                fas.closeAndDelete();
            }

            return curr.getValue();
        }

    }

    private final class Item implements Comparable<Item> {

        private final int index;
        private final V v;

        public Item(int index, V value) {
            this.index = index;
            this.v = value;
        }

        public int getIndex() {
            return this.index;
        }

        public V getValue() {
            return this.v;
        }

        public int compareTo(Item item) {
            return comparator.compare(v, item.getValue());
        }
    }

    private static class FileAndStream {

        private final DataInputStream inputStream;
        private final File file;

        private FileAndStream(File file, DataInputStream inputStream) {
            super();
            this.inputStream = inputStream;
            this.file = file;
        }

        public DataInputStream getInputStream() {
            return inputStream;
        }

        public File getFile() {
            return file;
        }

        public void closeAndDelete() {
            try {
                this.inputStream.close();
            } catch(IOException e) {
                throw new VoldemortException("Failed to close input stream.", e);
            } finally {
                this.file.delete();
            }
        }

    }
}
