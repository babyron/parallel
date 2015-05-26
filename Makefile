subsystem:
	cd ./master_slave && make || exit 1;

clean::
	cd ./master_slave && make clean || exit 1;
