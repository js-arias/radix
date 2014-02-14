# !/bin/sh

rm -fr db/
rm -f nohup.out
go test -test.run TestCas

