INCLUDE=-pa deps/*/ebin \
	-pa ebin

all:
	./rebar compile
run:
	rlwrap --always-readline erl $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' \
	-boot start_sasl -name gnss_aggr -config gnss_aggr.config -s lager start -s gnss_aggr start -s sync go
deploy:
	erl -detached -noshell -noinput $(INCLUDE) \
	-kernel inetrc '"./erl_inetrc"' \
	-boot start_sasl -name gnss_aggr -config gnss_aggr.config -s lager start -s gnss_aggr start -s sync go
attach:
	rlwrap --always-readline erl -name con`jot -r 1 0 100` -hidden -remsh gnss_aggr@`hostname`
stop:
	echo 'halt().' | erl -name con`jot -r 1 0 100` -remsh gnss_aggr@`hostname`
