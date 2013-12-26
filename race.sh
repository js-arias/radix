# !/bin/sh

rm -fr db/
rm -f nohup.out 
nohup go test -race -test.timeout=20h

