import re
import json
import logging
import pandas as pd

from datetime import datetime
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
        self.routes_ids = {
            1: "55",
            2: "50",
            3: "62",
            4: "53",
            5: "54",
            6: "60",
            7: "51",
            8: "52",
            9: "56",
            10: "57",
            11: "58",
            12: "59",
            59: "61",
            60: "64",
            61: "63",
        }

    def load_data(self, positions, devices, events):
        if "devices" in devices:
            for device in devices["devices"]:
                device_attributes = device.get("attributes", {})
                current_geofence = device_attributes.get("currentGeofence")
                if current_geofence is not None:
                    if current_geofence in self.routes_ids:
                        device["route_id"] = self.routes_ids[current_geofence]
                        self.data[device["id"]] = device

        if "events" in events:
            for event in events:
                device_id = event["deviceId"]
                if device_id in self.data:
                    self.data[device_id]["event"] = event

        if "positions" in positions:
            for position in positions["positions"]:
                device_id = position["deviceId"]
                if device_id in self.data: 
                    self.data[device_id]["position"] = position

        self.test_events()

        with open("data.json", "w") as f:
            json.dump(self.data, f, indent=4)

    def test_events(self) -> None:
        alternate = True

        for data in self.data.values():
            device_id = data["id"]

            now = datetime.now()

            base_event = {
                "id": device_id,
                "deviceId": device_id,
                "eventTime": now.strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
                "positionId": data.get("positionId", 0),
                "geofenceId": 0,
                "maintenanceId": 0,
                "busStopId": 0
            }

            if alternate:
                event = {
                    **base_event,
                    "type": "alarm",
                    "attributes": {
                        "alarm": "geofenceExited"
                    }
                }
            else:
                event = {
                    **base_event,
                    "type": "geofenceExited",
                    "attributes": {}
                }

            data["event"] = event
            
            alternate = not alternate


class GtfsDataContext(metaclass=SingletonMeta):
    def __init__(self):
        self.trips = pd.read_csv(GTFS_PATH/ "trips.txt")
        self.stops = pd.read_csv(GTFS_PATH / "stops.txt")
        self.calendar = pd.read_csv(GTFS_PATH / "calendar.txt")
        self.stop_times = pd.read_csv(GTFS_PATH / "stop_times.txt")

context: DataContext = DataContext()
gtfs_context: GtfsDataContext = GtfsDataContext()