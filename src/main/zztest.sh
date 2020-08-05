rm ./stdans/*

go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg-*.txt
mv mr-out-0 ./stdans/ans

echo "make std ans done."

go build -buildmode=plugin ../mrapps/wc.go
go run mrmaster.go pg-*.txt &
sleep 0.2s
go run mrworker.go wc.so

cat mr-out-* | sort > ./stdans/my
diff ./stdans/ans ./stdans/my
echo "check done."