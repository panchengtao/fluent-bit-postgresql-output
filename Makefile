all:
	go build -buildmode=c-shared -o out_postgresql.so .

#fast:
#	go build out_postgresql.go

clean:
	rm -rf *.so *.h *~