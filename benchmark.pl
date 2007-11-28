#!/usr/bin/perl -w

$executions = 3;
# options: fixedPivotSelector tentaclePivotSelector kMeansPPPivotSelector 
 execExperiments(2,30,"fixedPivotSelector");
# execExperiments(3,30,"fixedPivotSelector");
 execExperiments(4,30,"fixedPivotSelector");
# execExperiments(5,30,"fixedPivotSelector");
# execExperiments(6,30,"fixedPivotSelector");
 execExperiments(7,30,"fixedPivotSelector");
# execExperiments(8,30,"fixedPivotSelector");
 execExperiments(9,30,"fixedPivotSelector");
# execExperiments(10,30,"fixedPivotSelector");
# execExperiments(11,30,"fixedPivotSelector");
 execExperiments(12,30,"fixedPivotSelector");
# execExperiments(13,30,"fixedPivotSelector");
 execExperiments(14,30,"fixedPivotSelector");

 execExperiments(6,30,"tentaclePivotSelector");
# execExperiments(7,30,"tentaclePivotSelector");
# execExperiments(8,30,"tentaclePivotSelector");
 execExperiments(9,30,"tentaclePivotSelector");
# execExperiments(10,30,"tentaclePivotSelector");
# execExperiments(11,30,"tentaclePivotSelector");
execExperiments(12,30,"tentaclePivotSelector");
#execExperiments(14,30,"tentaclePivotSelector");

# execExperiments(6,30,"kMeansPPPivotSelector");
# execExperiments(7,30,"kMeansPPPivotSelector");
# execExperiments(8,30,"kMeansPPPivotSelector");
execExperiments(9,30,"kMeansPPPivotSelector");
#execExperiments(10,30,"kMeansPPPivotSelector");
execExperiments(12,30,"kMeansPPPivotSelector");
execExperiments(14,30,"kMeansPPPivotSelector");

# non-fixed:

#execExperiments(12,35,"tentaclePivotSelector");
#execExperiments(12,40,"tentaclePivotSelector");
#execExperiments(12,45,"tentaclePivotSelector");
#execExperiments(12,50,"tentaclePivotSelector");
#execExperiments(12,55,"tentaclePivotSelector");
#execExperiments(12,60,"tentaclePivotSelector");
# execExperiments(12,70,"tentaclePivotSelector");
# execExperiments(12,80,"tentaclePivotSelector");
# execExperiments(12,90,"tentaclePivotSelector");
# execExperiments(12,100,"tentaclePivotSelector");
# execExperiments(15,100,"tentaclePivotSelector");
# execExperiments(20,100,"tentaclePivotSelector");

# execExperiments(11,35,"kMeansPPPivotSelector");
# execExperiments(11,40,"kMeansPPPivotSelector");
# execExperiments(11,45,"kMeansPPPivotSelector");
# execExperiments(11,50,"kMeansPPPivotSelector");
# execExperiments(11,55,"kMeansPPPivotSelector");
# execExperiments(11,60,"kMeansPPPivotSelector");
# execExperiments(11,70,"kMeansPPPivotSelector");
# execExperiments(11,80,"kMeansPPPivotSelector");
# execExperiments(11,90,"kMeansPPPivotSelector");
# execExperiments(11,100,"kMeansPPPivotSelector");


# execExperiments(15,100,"kMeansPPPivotSelector");
# execExperiments(20,100,"kMeansPPPivotSelector");

# execExperiments(12,10,"tentaclePivotSelector");
# execExperiments(12,15,"tentaclePivotSelector");
# execExperiments(12,20,"tentaclePivotSelector");
# execExperiments(12,25,"tentaclePivotSelector");


# execExperiments(12,10,"kMeansPPPivotSelector");
# execExperiments(12,15,"kMeansPPPivotSelector");
# execExperiments(12,20,"kMeansPPPivotSelector");
# execExperiments(12,25,"kMeansPPPivotSelector");


sub execExperiments{

		my($od, $pivotSize, $pivotSelectionCriteria) = @_;

		print "$od-$pivotSize-$pivotSelectionCriteria(";
		my $totalC = createDatabase($od, $pivotSize, $pivotSelectionCriteria);
		print "$totalC)\n";
		execExperimentsAux(1,1,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(1,3,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(3,3,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(5,5,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(10,10,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(10,3,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(3,20,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(20,20,$od, $pivotSize, $pivotSelectionCriteria);
execExperimentsAux(3,30,$od, $pivotSize, $pivotSelectionCriteria);
		
}

sub execExperimentsAux{

			my($k, $r, $od, $pivotSize, $pivotSelectionCriteria) = @_;
			my $cx = 0;
			my $total = 0;
			while($cx < $executions){
				$total += execExperiment($k,$r, $od, $pivotSize, $pivotSelectionCriteria);
				$cx++;
		}
		
		print  $total / $executions . ", k=$k/r=$r\n";
}

sub createDatabase{
		my($od, $pivotSize, $pivotSelectionCriteria) = @_;	
		$total = shell("ant -buildfile example.xml -Dod=$od -DpivotSize=$pivotSize -DpivotSelectionCriteria=$pivotSelectionCriteria create > $od-$pivotSize-$pivotSelectionCriteria-create");		
		return $total;
}

# executes the search with the given parameters and returns
# the time it took to execute in msec
sub execExperiment{
		my($k, $r, $od, $pivotSize, $pivotSelectionCriteria) = @_;
    my $file = 	"$k-$r-$od-$pivotSize-$pivotSelectionCriteria-search";
		$total = shell("ant -buildfile example.xml -Dk=$k -Dr=$r -Dod=$od -DpivotSize=$pivotSize -DpivotSelectionCriteria=$pivotSelectionCriteria search > $file");		
		# get the time
    my $str = `grep "Running time in seconds:" $file`;
		($time) = ($str =~ /Running time in seconds[:] (\d+)/);
		return $time;
}


sub shell {
    my($cmd) = shift;
		$start = time();
    my $status = system($cmd);
    die "Command failed: $cmd\n" unless $status == 0;
		$total = time() - $start;
		return $total;
}
