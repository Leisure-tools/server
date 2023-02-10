module github.com/leisure-tools/server

go 1.19

require (
	github.com/leisure-tools/document v0.0.9
	github.com/leisure-tools/history v0.0.0
	github.com/sergi/go-diff v1.3.1
)

require github.com/leisure-tools/lazyfingertree v0.0.10 // indirect

replace github.com/leisure-tools/history v0.0.0 => /home/deck/Vaults/local/work/leisure-tools/history

replace github.com/leisure-tools/document v0.0.9 => /home/deck/Vaults/local/work/leisure-tools/document

replace github.com/leisure-tools/lazyfingertree v0.0.10 => /home/deck/Vaults/local/work/leisure-tools/lazyfingertree
