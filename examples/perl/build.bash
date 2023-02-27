/usr/bin/thrift --gen perl -o ./src ../../nifi-simple-thrift-converter-processors/src/main/resources/flowfile_nifi.thrift
#export PERL5LIB=./gen_perl
cd  src
perl -d  PerlClient.pl

