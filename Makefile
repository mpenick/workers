all:
	clang++ -O2 -o workers -std=c++11 workers.cpp hdr_histogram.cpp -luv

debug:
	clang++ -g -o workers -std=c++11 workers.cpp hdr_histogram.cpp -luv

clean:
	rm -rf workers workers.dSYM
