package org.ajmm.obsearch.exception;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.   
 */
/**
 * 
 * From Wikipedia: Catastrophic failure is a sudden and total failure of some
 * system from which recovery is impossible. The affected system not only
 * experiences destruction beyond any reasonable possibility of repair, but also
 * frequently causes injury, death, or significant damage to other, often
 * unrelated systems.
 * 
 * We throw this when something really bad happens in our assumptions and it is inexpensive
 * to check for the condition. Where it is expensive, we use asserts.
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class CatastrophicError extends OBException{

}
