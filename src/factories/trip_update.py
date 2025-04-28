import pytz
from geopy.distance import geodesic
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2 as gtfsrt

class TripUpdate:
    @staticmethod
    def create(*args, **kwargs):
        entity_id = str(kwargs.get("entity_id"))
        trip_id = str(kwargs.get("trip_id"))
        vehicle_id = kwargs.get("vehicle_id")
        latitude = kwargs.get("latitude")
        longitude = kwargs.get("longitude")
        speed_kmh = kwargs.get("speed_kmh", 30)

        stops = kwargs.get("stops", [])  # Expecting a list of stops

        trip_descriptor = gtfsrt.TripDescriptor(
            trip_id=trip_id
        )

        vehicle_descriptor = gtfsrt.VehicleDescriptor(id=vehicle_id)

        stop_time_updates = []

        for stop in stops:
            stop_sequence = stop["stop_sequence"]
            stop_lat = stop["stop_lat"]
            stop_lon = stop["stop_lon"]
            arrival_time = stop["arrival_time"]

            delay_info = TripUpdate._is_delayed(
                device_lat=latitude,
                device_lon=longitude,
                stop_lat=stop_lat,
                stop_lon=stop_lon,
                stop_arrival_time=arrival_time,
                speed_kmh=speed_kmh
            )

            stop_time_update = gtfsrt.TripUpdate.StopTimeUpdate(
                stop_sequence=stop_sequence
            )

            stop_time_update.arrival.delay = delay_info

            stop_time_updates.append(stop_time_update)

        trip_update = gtfsrt.TripUpdate(
            trip=trip_descriptor,
            vehicle=vehicle_descriptor,
            stop_time_update=stop_time_updates,
            timestamp=int(datetime.now().timestamp())
        )

        return gtfsrt.FeedEntity(
            id=entity_id,
            trip_update=trip_update
        )

    @staticmethod
    def _is_delayed(device_lat, device_lon, stop_lat, stop_lon, stop_arrival_time, speed_kmh=30):
        now = datetime.now(pytz.timezone("America/Guayaquil"))

        try:
            device_lat = float(device_lat)
            device_lon = float(device_lon)
            stop_lat = float(stop_lat)
            stop_lon = float(stop_lon)
        except Exception as e:
            raise ValueError(f"Error al convertir coordenadas a float: {e}")

        try:
            scheduled_arrival = datetime.strptime(stop_arrival_time, "%H:%M:%S").replace(
                year=now.year, month=now.month, day=now.day, tzinfo=now.tzinfo
            )
        except Exception as e:
            raise ValueError(f"Error al parsear tiempo de llegada: {e}")

        current_pos = (device_lat, device_lon)
        stop_pos = (stop_lat, stop_lon)

        try:
            distance_km = geodesic(current_pos, stop_pos).km
        except Exception as e:
            raise ValueError(f"Error al calcular la distancia geodésica: {e}")

        eta_seconds = (distance_km / speed_kmh) * 3600 if speed_kmh > 0 else 0
        estimated_arrival_time = now + timedelta(seconds=eta_seconds)

        arrival_delay = int((estimated_arrival_time - scheduled_arrival).total_seconds())

        return arrival_delay
