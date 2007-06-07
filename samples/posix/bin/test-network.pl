#!/usr/bin/perl

if ($#ARGV == 0) {
    $name = $ARGV[0];
} else {
    $name = "default";
}

print "Calling getnetbyname(\"$name\")...\n\n";

($name, $aliases, $addrtype, $net) = getnetbyname($name);

print "Name         : $name\n";
print "Aliases      : $aliases\n";
print "Address type : $addrtype\n";
print "Net          : $net\n";
