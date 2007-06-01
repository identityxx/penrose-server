#!/usr/bin/perl

if ($#ARGV == -1) {
    $hostname = `/bin/hostname`;
    chomp($hostname);
} else {
    $hostname = $ARGV[0];
}

print "Calling gethostbyname(\"$hostname\")...\n\n";

($name,$aliases,$addrtype,$length,@addrs) = gethostbyname($hostname);

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
