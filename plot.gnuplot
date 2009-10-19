set datafile separator '\t'
#set autoscale y

#set xrange [0 to 40]
#set yrange [0 to 40]
set xlabel "sketch distance"
set ylabel "Mean and std. deviation"
set title "Variation of sketch and distance when sketch is $x$"
set size square
#set boxwidth 0.2
set terminal png
set output 'graph.png'
set key below right
set grid



plot 'my0.csv' using 0 title "hamming distance" with lines, 'my0.csv' using 1:2:3 title "avg/std dev." with yerrorbars,  'my0.csv' using 1:2:4:5 notitle  with yerrorbars

