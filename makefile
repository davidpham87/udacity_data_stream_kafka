reset:
	podman pod stop -a && podman pod rm -a && podman rm -a

init:
	podman-compose up

producers: conda-env
	cd producers
	python3 simulation.py

faust: conda-env
	cd consumers
	faust -A faust_stream worker -l info

ksql: conda-env
	cd consumers
	python3 ksql.py

server: conda-env
	cd consumers
	python3 server.py

conda-env:
	conda activate venv
