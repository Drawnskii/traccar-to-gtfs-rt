import re
import json
import logging
import pandas as pd

from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent  # Va al root del proyecto
GTFS_PATH = BASE_DIR / "gtfs"

class SingletonMeta(type):
    _instaces = {}

    def __call__(cls, *args: Any, **kwds: Any) -> Any:
        if cls not in cls._instaces:
            instace = super().__call__(*args, **kwds)
            cls._instaces[cls] = instace

        return cls._instaces[cls]
    

class DataContext(metaclass=SingletonMeta):
    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}

    def load_data(self, positions, devices, events):
        if "devices" in devices:
            for device in devices["devices"]:
                device_attributes = device.get("attributes", {})
                current_geofence = device_attributes.get("currentGeofence")
                if current_geofence is not None:
                    # device["route_id"] = current_geofence
                    device["route_id"] = re.findall(r"\d+", device["name"])[0]
                    self.data[device["id"]] = device

        if "events" in events:
            for event in events:
                device_id = event["deviceId"]
                if device_id in self.data:
                    self.data[events["deviceId"]]["events"] = event

        if "positions" in positions:
            for position in positions["positions"]:
                device_id = position["deviceId"]
                if device_id in self.data: 
                    self.data[position["deviceId"]]["position"] = position

        with open("data.json", "w") as f:
            json.dump(self.data, f, indent=4)


class GtfsDataContext(metaclass=SingletonMeta):
    def __init__(self):
        self.trips = pd.read_csv(GTFS_PATH/ "trips.txt")
        self.stops = pd.read_csv(GTFS_PATH / "stops.txt")
        self.calendar = pd.read_csv(GTFS_PATH / "calendar.txt")
        self.stop_times = pd.read_csv(GTFS_PATH / "stop_times.txt")

context: DataContext = DataContext()
gtfs_context: GtfsDataContext = GtfsDataContext()