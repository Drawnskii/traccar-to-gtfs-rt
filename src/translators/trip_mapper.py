import pandas as pd
from datetime import datetime
from src.context import gtfs_context

class TripMapper:
    @staticmethod
    def map(route_id, device_time):
        dt = datetime.fromisoformat(device_time.replace("Z", "+00:00"))

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
            return None, None

        first_stops["time_diff"] = first_stops["departure_time"].apply(
            lambda t: abs((datetime.strptime(t, "%H:%M:%S") - datetime.strptime(time, "%H:%M:%S")).total_seconds())
        )

        best_match = first_stops.sort_values("time_diff").iloc[0]
        trip_id = best_match["trip_id"]
        stop_id = best_match["stop_id"]
        stop_sequence = best_match["stop_sequence"]
        stop_arrival_time = best_match["arrival_time"]
        stop_departure_time = best_match["departure_time"]

        stops = gtfs_context.stops
        stop_position = stops[stops["stop_id"] == stop_id]

        stop_lat = stop_position["stop_lat"]
        stop_lon = stop_position["stop_lon"]

        return trip_id, stop_id, stop_lat, stop_lon, stop_arrival_time, stop_departure_time, stop_sequence
