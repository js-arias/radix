# !/bin/sh

rm -fr db/
rm -f nohup.out
nohup go test -test.run TestOnDiskDelete
