set datafile separator '\t'
#set autoscale y

#set xrange [0 to 40]
#set yrange [0 to 40]
set xlabel "x distance"
set ylabel "y Mean and std. deviation"
set title "Variation of y when x"
set size square
#set boxwidth 0.2
set terminal png
set output 'graph.png'
set key below right
set grid



plot 'leftRight' using 1 title "x distance" with lines, 'leftRight' using 1:2:3 title "y avg/std dev." with yerrorbars,  'leftRight' using 1:2:4:5 notitle  with yerrorbars

