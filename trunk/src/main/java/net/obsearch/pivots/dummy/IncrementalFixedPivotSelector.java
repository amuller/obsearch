package net.obsearch.pivots.dummy;

import net.obsearch.Index;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;
import cern.colt.list.LongArrayList;
/*
		OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
    Copyright (C) 2008 Arnoldo Jose Muller Molina

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
	*  IncrementalFixedPivotSelector 
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */

public class IncrementalFixedPivotSelector implements IncrementalPivotSelector {

    /**
     * Pre-computed pivot definitions
     */	
	public static long[] def = { 290, 960, 35, 263, 360, 75, 619, 519, 335, 681, 332, 497, 306, 169, 281, 890, 387, 850, 971, 804};

    public static long[] levScalab = { 94323, 63655, 37558, 34472, 54433, 88958,
            97696, 88615, 93960, 93739, 97697, 20414, 34162, 21023, 66357,
            65775, 74077, 74498, 69404, 75965, 31607, 55316, 91240, 326, 10456,
            41388, 64728, 39269, 63582, 54810 };

    // 30 pivots for ted
    public static long[] ted = { 57300, 196436, 10653, 136029, 203654, 189753,
            152060, 133273, 236109, 144716, 30129, 192050, 14036, 28810,
            202543, 59154, 123211, 203986, 5279, 118146, 75130, 135862, 200942,
            92282, 77436, 110657, 116055, 37261, 94338, 235642 };

    // 30 pivots + .95 (10 levels, only 9 used).
    public static long[][] tedLevel = {

            { 15934, 107349, 18536, 159414, 187321, 80913, 109586, 55891, 9032,
                    157372, 63619, 4363, 43993, 244702, 189550, 166295, 128207,
                    147989, 97480, 238126, 107564, 184695, 100880, 6080,
                    129561, 66674, 234816, 110551, 79308, 32987 },
            { 138006, 233523, 176720, 11694, 87234, 105080, 208082, 146706,
                    97906, 21545, 67960, 177729, 235606, 108664, 63968, 205992,
                    234488, 214120, 74450, 123203, 96723, 112753, 9651, 136600,
                    50675, 194891, 96263, 61289 },
            { 108877, 141682, 4195, 165175, 119242, 36922, 7158, 213156,
                    241849, 158081, 198049, 123730, 143313, 9963, 205958,
                    23842, 113848, 121265, 62235, 140034, 236632, 209080,
                    232804, 131268, 81772, 99495 },
            { 205859, 76130, 193358, 129233, 209350, 70459, 179759, 53284,
                    200934, 227243, 6605, 97360, 173855, 237054, 158210, 10636,
                    178349, 216620, 216814, 100031, 131604, 109242, 213669,
                    84933 },
            { 137734, 111366, 239355, 49991, 174182, 96945, 48328, 35088,
                    137863, 46953, 64300, 181846, 39215, 232845, 121960,
                    228332, 139662, 95548, 968, 85698, 147004, 118684 },
            { 210342, 41235, 75519, 105228, 87526, 2170, 108443, 34877, 112732,
                    39078, 26858, 120868, 152787, 14105, 144830, 178479, 68468,
                    194694, 129460, 18198 },
            { 220715, 236557, 152792, 31517, 5346, 25152, 154219, 187172,
                    34347, 199820, 53241, 95499, 48729, 163319, 65337, 138582,
                    191089, 12606, 79032 },
            { 170699, 121277, 26031, 27020, 43075, 110093, 231245, 176657,
                    67649, 145891, 138798, 48040, 133480, 47006, 242678,
                    193006, 197849, 229375 },
            { 235654, 169269, 107349, 174847, 108031, 65633, 61768, 27618,
                    108590, 104125, 110130, 110219, 188196, 132412, 3734,
                    50649, 202913 },

    };

    // lev pivots (18 pivots for lev)

    // public static int [] lev = { 992515, 828447, 708572, 879139, 679767,
    // 356148, 23230, 263566, 744140, 507502, 855677, 749056, 908433, 864534,
    // 754452, 301782, 552141, 103224};
    public static long[] lev = { 28968, 628464, 433184, 921379, 656225, 681704,
            209306, 319394, 472165, 891033, 277620, 38427, 496272, 509825,
            925880, 935531, 927760, 245884 };

    public static long[][] levLevel = {
            { 992316, 405315, 721966, 850092, 163932, 47113, 624547, 625026,
                    857504, 473406, 785269, 858005, 55829, 363803, 591228,
                    814583, 299929, 683399 },
            { 395006, 800848, 992359, 165724, 238112, 523984, 191257, 229295,
                    229266, 757516, 73494, 541124, 301782, 555629, 624227,
                    553970, 546464 },
            { 355815, 517393, 429050, 344504, 282655, 490669, 466620, 539612,
                    866049, 506134, 993156, 983808, 781464, 422685, 871406,
                    605017 },
            { 138781, 937714, 843316, 285526, 106044, 948402, 2098, 905359,
                    685163, 857504, 624288, 321874, 876559, 887469, 18843 },
            { 428597, 584831, 463252, 791267, 910050, 793711, 641866, 910218,
                    40579, 870290, 955569, 198249, 74473, 525787 },
            { 429041, 482882, 802292, 607524, 788646, 595296, 984845, 29348,
                    864562, 271879, 427230, 681523, 350428 },
            { 993205, 82115, 237862, 188255, 20414, 604416, 538071, 776840,
                    2001, 362136, 750745, 514440 },
            { 992537, 833676, 301971, 867517, 290161, 506692, 727581, 423088,
                    509094, 528868, 232835 },
            { 123279, 530530, 800734, 9878, 186602, 992663, 382075, 42792,
                    584374, 228068 } };

    // 16 pivots for mtd
     public static long [] mtd = {173221, 81425, 258192, 229333, 256964,
     187123, 211523, 143917, 28954, 262061, 86748, 218554, 113214, 260088,
     337932, 11769};
    
		/*public static int[] mtd = { 143538, 191537, 27666, 207637, 234407, 278801,
            14835, 142853, 198987, 150621, 194982, 265446, 14548, 18332, 67002,
            133379, 55931, 290771, 306695, 19787, 317705, 269611, 241363,
            235312, 49570, 147869, 347944, 80416, 88808, 305347, 292672, 32854 };
		*/
    public static long[][] mtdLevel = {
            { 275060, 130016, 199779, 346945, 99362, 225116, 257606, 346212,
                    174634, 47137, 51535, 98775, 133386, 325413, 282666, 24063 },
            { 98714, 305085, 188131, 348248, 171624, 120761, 215617, 322408,
                    287498, 75336, 107898, 66643, 312034, 282658, 173327 },
            { 269101, 17275, 337074, 221119, 30915, 206906, 75336, 95375,
                    62891, 104250, 239320, 116709, 28954, 45808 },
            { 112833, 211151, 121839, 285318, 117790, 105893, 59704, 191409,
                    123579, 40893, 172139, 308417, 148459 },
            { 225621, 274709, 315087, 102871, 223744, 305914, 246259, 18361,
                    187720, 313273, 115605, 296311 },
            { 28598, 226500, 179101, 224413, 113452, 160126, 272162, 114709,
                    6947, 76065, 231371 },
            { 288361, 169153, 199593, 180289, 62561, 137528, 205684, 279728,
                    110517, 102344 },
            { 99242, 40641, 42519, 291856, 244712, 179101, 299274, 203226,
                    204242 },
            { 283781, 257700, 152463, 236670, 4420, 125697, 55347, 246946 }, };

    long[][] data;

    int ind = 0; // next level
    
    public IncrementalFixedPivotSelector() {
    	this(def);
    }

    public IncrementalFixedPivotSelector(long[] ids) {
        data = new long[1][];
        data[0] = ids;
    }

    public IncrementalFixedPivotSelector(long[][] ids) {
        data = ids;
    }

    @Override
    public PivotResult generatePivots(int pivotCount, Index index)
            throws OBException, IllegalAccessException, InstantiationException,
            OBStorageException, PivotsUnavailableException {
        return generatePivots(pivotCount, null, null);
    }

    @Override
    public PivotResult generatePivots(int pivotCount, LongArrayList elements,
            Index index) throws OBException, IllegalAccessException,
            InstantiationException, OBStorageException,
            PivotsUnavailableException {
        long[] res = new long[pivotCount];
        int i = 0;
        while (i < res.length) {
            res[i] = data[ind][i];
            i++;
        }
        ind++;
        return new PivotResult(res);
    }

}
