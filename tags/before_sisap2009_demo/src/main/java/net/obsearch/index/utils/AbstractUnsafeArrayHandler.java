package net.obsearch.index.utils;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

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
* Class that allows the direct access of data from arrays.
* It uses the class UnSafe that is not part of the JDK
* @author Arnoldo Jose Muller Molina
* @since 0.8
*/
public class AbstractUnsafeArrayHandler {
    
    public static Unsafe unsafe = null;
    
    public static long offset;
    
    public AbstractUnsafeArrayHandler(){
        if(unsafe == null){
            unsafe = getUnsafe();
            byte[] data = {1,2};
            offset = unsafe.arrayBaseOffset(data.getClass());
        }
        
    }
    
    public static Unsafe getUnsafe()  {
        Unsafe unsafe = null;

        try {
            Class uc = Unsafe.class;
            Field[] fields = uc.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                if (fields[i].getName().equals("theUnsafe")) {
                    fields[i].setAccessible(true);
                    unsafe = (Unsafe) fields[i].get(uc);
                    break;
                }
            }
        } catch (Exception ignore) {
        }
        return unsafe;
    }
}       
