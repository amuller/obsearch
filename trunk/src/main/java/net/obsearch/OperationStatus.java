package net.obsearch;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C) 2007 Kyushu Institute of Technology

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * This class is used to return the result of operations performed by OBSearch.
 * The result also includes an id field that, depending on the operation returns
 * the last inserted or deleted object.
 * @author Arnoldo Jose Muller Molina
 * @since 0
 */

public class OperationStatus {

    /**
     * Status of the Operation
     */
    private Status status;

    /**
     * Object id for relevant
     */
    private long id;
    
    /**
     * Number of times this 
     */
    private long frequency;
    
    /**
     * String message
     */
    private String msg = null;

    public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	/**
     * Creates a default status with status Status.Error
     * and an id of -1.
     */
    public OperationStatus() {
        this(Status.ERROR, -1);
    }

    /**
     * Initializes the object with the given
     * status and an id of -1
     * @param status The status that will be used.
     */
    public OperationStatus(Status status){
        this(status, -1);
    }
    /**
     * Creates a new status object.
     * @param status The status the object will hold
     * @param id The id of the affected object.
     */
    public OperationStatus(Status status, int id) {
        this.status = status;
        this.id = id;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Sets the id returned in the enumeration.
     * @param id
     *                The new id.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Returns the id of the affected item of the operation -1 if the id does
     * not apply.
     * @return The id.
     */
    public long getId() {
        return id;
    }
}
