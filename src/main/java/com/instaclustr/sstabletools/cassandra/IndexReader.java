package com.instaclustr.sstabletools.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.instaclustr.sstabletools.AbstractSSTableReader;
import com.instaclustr.sstabletools.PartitionStatistics;
import com.instaclustr.sstabletools.SSTableStatistics;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * SSTable Index.db reader.
 */
public class IndexReader extends AbstractSSTableReader {
    /**
     * Index.db reader.
     */
    private RandomAccessReader reader;

    /**
     * BTI Keyreader.
     */
    private KeyReader keyReader;

    /**
     * SSTable version.
     */
    private Version version;

    /**
     * The sstable partitioner.
     */
    private IPartitioner partitioner;

    /**
     * The next partition key.
     */
    private ByteBuffer nextKey;

    /**
     * The position in Data.db of the following partition key.
     */
    private long nextPosition;

    /**
     * Flag to determine that the last index entry has been read.
     */
    private boolean completed = false;

    /**
     * Construct a reader for Index.db sstable file.
     *
     * @param tableStats  SSTable statistics.
     * @param reader      Reader to Index.db file.
     * @param version     Version of SSTable
     * @param partitioner The sstable partitioner.
     */
    public IndexReader(SSTableStatistics tableStats, RandomAccessReader reader, Version version, IPartitioner partitioner) {
        this.tableStats = tableStats;
        this.reader = reader;
        this.version = version;
        this.nextKey = null;
        this.partitioner = partitioner;
    }


    /**
     * Construct a reader for Index.db sstable file.
     *
     * @param tableStats  SSTable statistics.
     * @param keyReader   Reader to Partition.db file.
     * @param version     Version of SSTable
     * @param partitioner The sstable partitioner.
     */
    public IndexReader(SSTableStatistics tableStats, KeyReader keyReader, Version version, IPartitioner partitioner) {
        this.tableStats = tableStats;
        this.keyReader = keyReader;
        this.version = version;
        this.nextKey = null;
        this.partitioner = partitioner;
    }

    /**
     * Skip data field on index entry.
     *
     * @throws IOException
     */
    private void skipData() throws IOException {
        if (keyReader != null) {
            return;
            //test
        } else {
            int size = version.version.compareTo("ma") >= 0 ? (int) reader.readUnsignedVInt() : reader.readInt();
            if (size > 0) {
                reader.skipBytesFully(size);
            }
        }
    }

    @Override
    public boolean next() {
        if (completed) {
            return false;
        }
        try {
            if (nextKey == null) {
                nextKey = getNextKey();
                nextPosition = getNextPosition();
                skipData();
            }
            System.out.printf("Key: %s%n", nextKey);
            partitionStats = new PartitionStatistics(partitioner.decorateKey(nextKey));
            long position = nextPosition;
            if ((reader != null && !reader.isEOF()) || (keyReader != null && !keyReader.isExhausted() && keyReader.advance())) {
                nextKey = getNextKey();
                nextPosition = getNextPosition();
                skipData();
                partitionStats.size = nextPosition - position;
            } else {
                partitionStats.size = this.tableStats.size - position;
                if (reader != null) {
                    reader.close();
                } else {
                    keyReader.close();
                }
                completed = true;
            }
            this.tableStats.partitionCount++;
            this.tableStats.maxPartitionSize = Math.max(partitionStats.size, this.tableStats.maxPartitionSize);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            if (!completed) {
                try {
                    reader.close();
                } catch (Throwable t) {
                }
            }
            completed = true;
            return false;
        }
    }

    private ByteBuffer getNextKey() throws IOException {
        if (keyReader != null) {
            return keyReader.key();
        } else {
            return ByteBufferUtil.readWithShortLength(reader);
        }
    }

    private long getNextPosition() throws IOException {
        if (keyReader != null) {
            return keyReader.dataPosition();
        } else {
            return version.version.compareTo("ma") > 0 ? reader.readUnsignedVInt() : reader.readLong();
        }
    }
}



