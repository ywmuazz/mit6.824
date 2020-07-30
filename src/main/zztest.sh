go build -buildmode=plugin ../mrapps/wc.go
go run mrmaster.go pg-being_ernest.txt &
sleep 0.2s
go run mrworker.go wc.so

