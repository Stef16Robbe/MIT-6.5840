# MIT 6.5840

http://nil.csail.mit.edu/6.5840/2024/labs/lab-mr.html

## Running

Coordinator: `go build -buildmode=plugin ../mrapps/wc.go && sh -c 'rm -f mr-out*' && echo "ready" && go run mrcoordinator.go pg-*.txt`
Worker(s): `go run mrworker.go wc.so`

