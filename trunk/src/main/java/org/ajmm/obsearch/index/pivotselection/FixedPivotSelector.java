package org.ajmm.obsearch.index.pivotselection;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.PivotSelector;

import com.sleepycat.je.DatabaseException;

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
 * Selects the pivots whose ids are in the array {@link #pivotArray}. This
 * pivot selector is only for testing purposes.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

/*
 * Good pivots (selected with k-means++ for metric spaces): 218128, 68061,
 * 55855, 306623, 122047, 119256, 253761, 240019, 163818, 74455, 234790, 327067,
 * 2145, 263544, 236616, 322278, 233874, 176069, 122893, 88566, 304501, 259976,
 * 16076, 84566, 187768, 68341, 20524, 150070, 67600, 77936 Another list:
 * 244374, 240864, 171916, 246450, 221529, 127367, 122029, 231572, 33754,
 * 197687, 77927, 102661, 241107, 293502, 86359, 95125, 332983, 178275, 15565,
 * 44697, 49975, 161903, 196601, 232716, 48277, 280739, 188855, 237080, 256569,
 * 79382
 */
public class FixedPivotSelector implements PivotSelector {

    /**
     * Internal db ids that will be extracted from the index.
     */
    /**
     * int[] pivotArray = { 143410, 3400, 308101, 132807, 146392, 322786, 37130,
     * 284923, 241765, 234087, 209606, 46464, 5242, 321523, 317796, 69782,
     * 176869, 27139, 188754, 73739, 109576, 229099, 153514, 163651, 110404,
     * 97472, 41835, 41897, 224014, 7069, 218288, 297527, 258698, 70361, 142473,
     * 52390, 338779, 133903, 295473, 136266, 100321, 290021, 70388, 17683,
     * 46664, 135066, 259983, 75949, 87947, 280334, 118411, 271720, 299177,
     * 278809, 213894, 204054, 37368, 218265, 98567, 13866, 165293, 222216,
     * 136266, 316674, 336036, 179049, 211876, 80309, 65684, 44838, 90099,
     * 231611, 156736, 159865, 47891, 218668, 10592, 218178, 47988 };
     */

    // 1-5000
    /*
     * int[] pivotArray =
     * {271348,60346,339751,86318,223229,218259,311295,229618,322767,20259,55936,319091,48269,292300,64264,214083,29909,16828,108060,281189,84142,118634,164138,250381,332203,349431,86896,286289,73965,315501};
     */

    /**
     * 1-500
     */

    /*
     * int[] pivotArray = { 283141, 62099, 112244, 82999, 129494, 337630, 74811,
     * 268776, 23810, 301526, 268269, 306110, 216812, 144222, 262215, 148028,
     * 84488, 173521, 239196, 305293, 302188, 290039, 219081, 342693, 211796,
     * 122652, 197586, 16744, 251605, 264425 };
     */

    // kmeans++ selected. (we were using this one before!)
    // int[] pivotArray = {163061, 110606, 56344, 238657, 344941, 104868, 52473,
    // 306248, 93852, 34913, 38839, 104441, 32621, 267878, 225436, 51165,
    // 195261, 305531, 116084, 232130, 241638, 73961, 255471, 281809, 261511,
    // 238568, 204115, 5642, 193213, 274868};

    // navarro with better settings
    // validate selected pivots. MTD 16 pivots
    // int [] pivotArray = {173221, 81425, 258192, 229333, 256964, 187123,
    // 211523, 143917, 28954, 262061, 86748, 218554, 113214, 260088, 337932,
    // 11769};
    // 30 pivots for TED!!! :)
    // int [] pivotArray = {57300, 196436, 10653, 136029, 203654, 189753,
    // 152060, 133273, 236109, 144716, 30129, 192050, 14036, 28810, 202543,
    // 59154, 123211, 203986, 5279, 118146, 75130, 135862, 200942, 92282, 77436,
    // 110657, 116055, 37261, 94338, 235642};
    // 32 pivots for TED
     int [] pivotArray = {68837, 223235, 57634, 34056, 157833, 82856, 129010,
     95903, 155337, 53284, 25306, 50538, 84025, 13301, 190506, 210276, 206931,
     27289, 6508, 176723, 84614, 192319, 63271, 238035, 34453, 94959, 131639,
     110443, 113794, 133047, 224166, 132884};
    
    // 20 pivots for LEV
    // int [] pivotArray = {467023, 264003, 474731, 363298, 280463, 659272,
    // 796706, 112242, 175608, 545550, 520555, 578161, 83328, 850889, 163530,
    // 438778, 389451, 167736, 454957, 725722};
    // 30 pivots for LEV
    // int [] pivotArray = {496827, 443276, 203657, 596825, 502345, 540033,
    // 829573, 324825, 202637, 731792, 938727, 157919, 237726, 755572, 468412,
    // 987674, 783351, 606873, 917533, 633898, 358242, 514174, 75537, 460217,
    // 196829, 85855, 453600, 475013, 410323, 660634};
    // 60 pivots for LEV
   /* int[] pivotArray = { 52667, 992478, 365508, 197290, 65310, 863770, 362621,
            462888, 304343, 47687, 757056, 290156, 11830, 63383, 142948,
            194941, 424118, 210919, 141763, 326137, 781708, 261897, 603128,
            440190, 651064, 73546, 42176, 599696, 439065, 952319, 402610,
            136171, 690189, 775173, 410971, 818961, 23118, 34816, 621629,
            202819, 112299, 400640, 770643, 320075, 834911, 817890, 197346,
            373249, 54766, 2549, 322845, 546090, 692238, 45828, 592475, 356001,
            669948, 140445, 626217, 910335 };*/

    /*
     * int[] pivotArray = { 147954, 42284, 12826, 85877, 252376, 31433, 132080,
     * 217007, 302558, 88019, 293258, 209220, 170868, 197718, 148508, 129379,
     * 266697, 130270, 281179, 272382, 84781, 151848, 17199, 167629, 161381,
     * 64784, 166667, 80616, 276120, 289191 };
     */

    /**
     * Selects the pivots whose ids are in the array {@link #pivotArray}. This
     * pivot selector is only for testing purposes.
     * @param index
     *                The index that will be processed.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @see org.ajmm.obsearch.index.PivotSelector#generatePivots(short)
     */
    public final void generatePivots(AbstractPivotIndex index)
            throws OBException, IllegalAccessException, InstantiationException,
            DatabaseException {
        short pivots = index.getPivotsCount();
        int maxIdAvailable = index.getMaxId();
        assert pivots <= maxIdAvailable;
        int[] res = new int[pivots];
        int i = 0;
        while (i < res.length) {
            assert pivotArray[i] <= maxIdAvailable;
            res[i] = pivotArray[i];
            i++;
        }
        index.storePivots(res);
    }
}
