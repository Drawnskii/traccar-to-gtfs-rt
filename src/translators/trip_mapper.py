import os
import pytz
import pandas as pd

from dotenv import load_dotenv
from datetime import datetime
from src.context import gtfs_context

load_dotenv()

class TripMapper:
    @staticmethod
    def map(route_id, device_time):
        dt = datetime.fromisoformat(device_time.replace("Z", "+00:00"))
        
        timezone = pytz.timezone(os.getenv("timezone"))
        dt = dt.astimezone(timezone)

        date = dt.strftime("%Y%m%d")
        time = dt.strftime("%H:%M:%S")
        weekday = dt.strftime("%A").lower()

        calendar = gtfs_context.calendar
        calendar = calendar[(calendar[weekday] == 1)]
        calendar = calendar[(calendar["start_date"] <= int(date)) & (calendar["end_date"] >= int(date))]

        active_services = calendar["service_id"].unique()

        trips = gtfs_context.trips
        today_trips = trips[(trips["route_id"] == int(route_id)) & trips["service_id"].isin(active_services)]

        merged = pd.merge(today_trips, gtfs_context.stop_times, on='trip_id')
        first_stops = merged.sort_values(["trip_id", "stop_sequence"]).groupby("trip_id").first().reset_index()

        if first_stops.empty:
            return None

        first_stops["time_diff"] = first_stops["departure_time"].apply(
            lambda t: abs((datetime.strptime(t, "%H:%M:%S") - datetime.strptime(time, "%H:%M:%S")).total_seconds())
        )

        best_match = first_stops.sort_values("time_diff").iloc[0]
        trip_id = best_match["trip_id"]

        # Get all stops for this trip
        trip_stops = merged[merged["trip_id"] == trip_id].sort_values("stop_sequence")

        stops_df = pd.merge(trip_stops, gtfs_context.stops, on="stop_id")

        stops = []
        
        for _, row in stops_df.iterrows():
            stops.append({
                "stop_sequence": int(row["stop_sequence"]),
                "stop_id": row["stop_id"],
                "stop_lat": row["stop_lat"],
                "stop_lon": row["stop_lon"],
                "arrival_time": row["arrival_time"]
            })

        return trip_id, stops
