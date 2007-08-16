package org.ajmm.obsearch;

/**
 * A container used to return an OB and its insertion or deletion timestamp.
 * @param <O>
 *            The type of object of the result.
 */
public class TimeStampResult < O extends OB > {

    /**
     * O object.
     */
    private O object;

    /**
     * The timestamp when the insert or deletion occurred.
     */
    private long timestamp;

    /**
     * If the operation is an insert or a delete (false).
     */
    private boolean insert;

    /**
     * Creates a time stamp result where the object and its associated timestamp
     * are kept. We also have a flag that indicates if the object was inserted
     * or deleted.
     * @param object
     *            Object to be stored
     * @param timestamp
     *            Time where the operation ocurred
     * @param insert
     *            If the operation is an insert or a delete
     */
    public TimeStampResult(final O object, final long timestamp,
            final boolean insert) {
        super();
        this.object = object;
        this.timestamp = timestamp;
        this.insert = insert;
    }

    /**
     * If the current operation is an insert.
     * @return True if the operation is an insert or false otherwise.
     */
    public final boolean isInsert() {
        return insert;
    }

    /**
     * Sets the type of operation.
     * @param insert (
     *            true = insert, false = delete)
     */
    public final void setInsert(boolean insert) {
        this.insert = insert;
    }

    /**
     * Returns the object of this operation.
     * @return the object of the result.
     */
    public final O getObject() {
        return object;
    }

    /**
     * Sets the object of the operation.
     * @param object
     *            The object that will be used in this result.
     */
    public final void setObject(final O object) {
        this.object = object;
    }

    /**
     * @return The timestamp of the result.
     */
    public final long getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp.
     * @param timestamp
     *            The new timestamp.
     */
    public final void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

}
