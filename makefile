reset:
	podman pod stop -a && podman pod rm -a && podman rm -a

init:
	podman-compose up

producers:
	cd producers && python3 simulation.py

faust:
	cd consumers && faust -A faust_stream worker -l info

ksql:
	cd consumers && python3 ksql.py


server:
	cd consumers && python3 server.py
