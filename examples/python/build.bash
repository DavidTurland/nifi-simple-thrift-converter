# sudo apt install python3-thrift


/usr/bin/thrift --gen py ../../nifi-simple-thrift-converter-processors/src/main/resources/flowfile_nifi.thrift
python3  ./src/PythonClient.py3

