# !/bin/sh

rm -fr db/
go test -test.timeout=1h
