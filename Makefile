
.PHONY: benchpress
benchpress:
	go build -o benchpress .

.PHONY: clean
clean:
	rm benchpress
