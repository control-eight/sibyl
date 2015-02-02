package com.my.sibyl.itemsets.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author abykovsky
 * @since 2/1/15
 */
public class Association implements Serializable, Writable {

    private long itemSetCount;

    private String associationId;

    private long associationCount;

    public Association() {
    }

    public Association(long itemSetCount, String associationId, long associationCount) {
        this.itemSetCount = itemSetCount;
        this.associationId = associationId;
        this.associationCount = associationCount;
    }

    public long getItemSetCount() {
        return itemSetCount;
    }

    public void setItemSetCount(long itemSetCount) {
        this.itemSetCount = itemSetCount;
    }

    public String getAssociationId() {
        return associationId;
    }

    public void setAssociationId(String associationId) {
        this.associationId = associationId;
    }

    public long getAssociationCount() {
        return associationCount;
    }

    public void setAssociationCount(long associationCount) {
        this.associationCount = associationCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(itemSetCount);
        out.writeUTF(associationId);
        out.writeLong(associationCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        itemSetCount = in.readLong();
        associationId = in.readUTF();
        associationCount = in.readLong();
    }
}
