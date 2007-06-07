#!/usr/bin/perl

if ($#ARGV == 0) {
    $name = $ARGV[0];
} else {
    $name = "nfs";
}

print "Calling getrpcbyname(\"$name\")...\n\n";

($name, $aliases, $rpc) = getrpcbyname($name);

print "Name     : $name\n";
print "Aliases  : $aliases\n";
print "Rpc      : $rpc\n";
