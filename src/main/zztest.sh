go build -buildmode=plugin ../mrapps/wc.go
go run mrmaster.go pg-*.txt &
sleep 1s
go run mrworker.go wc.so

