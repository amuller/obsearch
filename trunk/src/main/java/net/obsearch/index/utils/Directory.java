package net.obsearch.index.utils;

import java.io.File;
import java.io.IOException;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * Utility class to manipulate directories.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public final class Directory {

    /**
     * Utility classes should not have private constructors.
     */
    private Directory() {

    }

    /**
     * Delete the given directory and all its contents.
     * @param dbFolder The directory to delete.
     * @throws IOException If the operation is not succesful.
     */
    public static void deleteDirectory(File dbFolder) throws IOException {
        if (!dbFolder.exists()) {
            return;
        }
        File[] files = dbFolder.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                deleteDirectory(f);
            } else {
            	if(f.toString().endsWith(".java")){
            		throw new IOException("Cannot delete .java files!!!");
            	}
                if (!f.delete()) {
                    throw new IOException("Could not delete: " + f);
                }
            }
        }
        if (!dbFolder.delete()) {
            throw new IOException("Could not delete: " + dbFolder);
        }
        if (dbFolder.exists()) {
            throw new IOException("Could not delete: " + dbFolder);
        }
    }

}
