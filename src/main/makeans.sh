go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg-*.txt
mv mr-out-0 ./stdans/ans