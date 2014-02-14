# !/bin/sh

rm -fr db/
go test -cover -test.timeout=2h

