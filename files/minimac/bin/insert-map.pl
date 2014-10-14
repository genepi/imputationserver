#!/usr/bin/env perl

# Replaces the genetic position in the given SNP file (first argument) using
# the given genetic map (second argument).  The genetic map is expected to
# have three columns: a physical position, a column that is ignored, and a
# genetic position in centi-Morgans.  This is the file format for HapMap
# recombination rate files and these can be downloaded from
# http://hapmap.ncbi.nlm.nih.gov/downloads/recombination/
#
# NOTE: it is *critical* that the physical positions of the two files are
# from the same genome build.  For example, both could have physical positions
# from build 36 or both could have physical positions from build 37, but
# do not mix physical positions from different builds!

# The updated SNP file is printed to STDOUT

use strict;
use warnings;

## Note: the reason this works for both Eigenstrat/Ancestrymap SNP files and
## for PLINK map/bim files is that the genetic position and physical
## positions are in the same column number in the two file types


if (@ARGV != 2) {
  print "Usage:\n";
  print "$0 [SNP_file] [genetic_map]\n";
  print "     SNP_file can either be Eigenstrat/Ancestrymap format SNP file or\n";
  print "     a PLINK formatted .map or .bim file.\n";
  exit;
}


my $snp_file = $ARGV[0];
my $map_file = $ARGV[1];

my @positions;
my @genetic_dist;

open MAP, "$map_file" or die "Couldn't open $map_file: $!\n";

my $last_pos = -1;

<MAP>; # read the header line

while (my $line = <MAP>) {
  $line =~ s/^\s+//;
  my @fields = split /\s+/, $line;
  if (@fields != 3) {
    my $num_cols = @fields;
    die "Read line with $num_cols columns but should always have 3 columns\n";
  }

  if ($fields[0] <= $last_pos) {
    die "Error: physical positions not in ascending order in map file at position $fields[0] ($fields[1])\n";
  }
  $last_pos = $fields[0];
  push @positions, $fields[0];
  push @genetic_dist, ( $fields[2] / 100 );
}

close MAP;


open SNP, "$snp_file" or die "Couldn't open $snp_file: $!\n";

my $cur_index = 0;
$last_pos = -1;

while ($_ = <SNP>) {
  s/^\s+//;
  my @fields = split /\s+/;
  my $ph_pos = $fields[3];

  if ($ph_pos <= $last_pos) {
    die "Error: physical positions not in ascending order in SNP file at SNP $fields[0] ($fields[1])\n";
  }
  $last_pos = $ph_pos;

  if ($cur_index > 0 && $positions[ $cur_index - 1 ] > $ph_pos) {
    $cur_index = 0;
  }

#  print "cur_index = $cur_index; p = $positions[$cur_index], ph_pos = $ph_pos\n";

  my $genet_pos;
  if ($ph_pos == 0) {
    $genet_pos = 0.0;
  }
  else {
    for( ; $cur_index < scalar @positions && $positions[ $cur_index ] < $ph_pos;
								 $cur_index++){}

    if ($cur_index >= scalar @positions) {
      print STDERR "Warning: position $ph_pos after the last position in the map; map and physical pos set to 0\n";
      $genet_pos = 0.0;
      $ph_pos = $fields[3] = 0;
    }
    elsif ($positions[ $cur_index ] == $ph_pos) {
      $genet_pos = $genetic_dist[ $cur_index ];
    }
    else {
      # linear interpolation:
      if ($cur_index == 0) {
	print STDERR "Warning: position $ph_pos before the first position in the map; map and physical pos set to 0\n";
	$genet_pos = 0.0;
	$ph_pos = $fields[3] = 0;
      }
      else {
	my $rel_distance = ($ph_pos - $positions[ $cur_index - 1 ]) /
		        ($positions[$cur_index] - $positions[ $cur_index - 1 ]);
        if ($rel_distance > 1.0 || $rel_distance < 0.0) {
	  print "Huh?";
	}
        die "Huh?" if ($rel_distance > 1.0 || $rel_distance < 0.0);
	$genet_pos = $genetic_dist[ $cur_index - 1] +
		   $rel_distance *
		   ($genetic_dist[$cur_index] - $genetic_dist[ $cur_index - 1]);
      }
    }
  }

  print "$fields[0]\t$fields[1]\t$genet_pos\t$fields[3]";
  if (@fields > 4) {
    print "\t$fields[4] $fields[5]";
  }
  if (@fields > 6) {
    die "More than 6 fields in SNP file?\n";
  }
  print "\n";
}

close SNP;
