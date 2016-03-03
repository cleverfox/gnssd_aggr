INCLUDE=-pa deps/*/ebin \
	-pa ebin

all:
	./rebar compile
run:
	erl $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' \
	-boot start_sasl -name gnss_aggr -config gnss_aggr.config -s lager start -s gnss_aggr start
deploy:
	erl -detached -noshell -noinput $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' \
	-boot start_sasl -name gnss_aggr -config gnss_aggr.config -s lager start -s gnss_aggr start
attach:
	erl -name con`jot -r 1 0 100` -hidden -remsh gnss_aggr@`hostname`
stop:
	echo 'halt().' | erl -name con`jot -r 1 0 100` -remsh gnss_aggr@`hostname`
