#!/usr/bin/perl

if ($#ARGV == 0) {
    $name = $ARGV[0];
} else {
    $name = `/bin/hostname`;
    chomp($name);
}

print "Calling gethostbyname(\"$name\")...\n\n";

($name, $aliases, $addrtype, $length, @addrs) = gethostbyname($name);

print "Name         : $name\n";
print "Aliases      : $aliases\n";
print "Address type : $addrtype\n";
print "Length       : $length\n";
print "\n";

print "Addresses:\n";
foreach $addr (@addrs) {
    ($a, $b, $c, $d) = unpack('C4', $addr);
    print " - $a.$b.$c.$d\n";
}
