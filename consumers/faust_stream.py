"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092",
                store="memory://")
topic = app.topic("org.cta.stations", value_type=Station)
out_topic = app.topic("org.cta.stations.table", partitions=1)
table = app.Table(
    "cta.stations.table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic)


@app.agent(topic)
async def process_stations(stations):
    async for station in stations:
        lines = [k for k in ("red", "green", "blue") if getattr(station, k)]
        line = ":undefined"
        if lines:
            line = lines[0]
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line)

if __name__ == "__main__":
    app.main()
