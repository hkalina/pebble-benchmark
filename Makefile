benchmark: main.go
	go build -o benchmark .

test:
	./benchmark ./data
