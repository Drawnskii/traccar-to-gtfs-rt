import pytz

from geopy.distance import geodesic
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2 as gtfsrt

class TripUpdate:
    @staticmethod
    def create(*args, **kwargs):
        entity_id = str(kwargs.get("entity_id"))
        trip_id = str(kwargs.get("trip_id"))
        route_id = str(kwargs.get("route_id"))
        stop_id = str(kwargs.get("stop_id"))
        stop_sequence = kwargs.get("stop_sequence", 1)
        vehicle_id = kwargs.get("vehicle_id")

        latitude = kwargs.get("latitude")
        longitude = kwargs.get("longitude")
        stop_lat = kwargs.get("stop_lat")
        stop_lon = kwargs.get("stop_lon")
        speed_kmh = kwargs.get("speed_kmh", 30)

        arrival_time = kwargs.get("arrival_time")
        departure_time = kwargs.get("departure_time")

        delay_info = TripUpdate._is_delayed(
            device_lat=latitude,
            device_lon=longitude,
            stop_lat=stop_lat,
            stop_lon=stop_lon,
            stop_arrival_time=arrival_time,
            stop_departure_time=departure_time,
            speed_kmh=speed_kmh
        )

        trip_descriptor = gtfsrt.TripDescriptor(
            trip_id=trip_id,
            route_id=route_id,
            schedule_relationship=gtfsrt.TripDescriptor.SCHEDULED
        )

        vehicle_descriptor = gtfsrt.VehicleDescriptor(
            id=vehicle_id
        )

        stop_time_update = gtfsrt.TripUpdate.StopTimeUpdate(
            stop_id=stop_id,
            stop_sequence=stop_sequence
        )

        stop_time_update.arrival.delay = delay_info["arrival_delay"]
        stop_time_update.arrival.time = int(
            datetime.now().timestamp() + delay_info["arrival_delay"]
        )
        stop_time_update.arrival.uncertainty = delay_info["uncertainty_seconds"]

        stop_time_update.departure.delay = delay_info["departure_delay"]
        stop_time_update.departure.time = int(
            datetime.now().timestamp() + delay_info["departure_delay"]
        )
        stop_time_update.departure.uncertainty = delay_info["uncertainty_seconds"]

        trip_update = gtfsrt.TripUpdate(
            trip=trip_descriptor,
            vehicle=vehicle_descriptor,
            stop_time_update=[stop_time_update]
        )

        return gtfsrt.FeedEntity(
            id=entity_id,
            trip_update=trip_update
        )
    
    @staticmethod
    def _is_delayed(device_lat, device_lon, stop_lat, stop_lon, stop_arrival_time, stop_departure_time, speed_kmh=30):
        from geopy.distance import geodesic
        from datetime import datetime, timedelta
        import pytz

        now = datetime.now(pytz.timezone("America/Guayaquil"))

        try:
            device_lat = float(device_lat)
            device_lon = float(device_lon)
            stop_lat = float(stop_lat.iloc[0])
            stop_lon = float(stop_lon.iloc[0])
        except Exception as e:
            raise ValueError(f"Error al convertir coordenadas a float: {e}")

        try:
            scheduled_arrival = datetime.strptime(stop_arrival_time, "%H:%M:%S").replace(
                year=now.year, month=now.month, day=now.day, tzinfo=now.tzinfo
            )
            scheduled_departure = datetime.strptime(stop_departure_time, "%H:%M:%S").replace(
                year=now.year, month=now.month, day=now.day, tzinfo=now.tzinfo
            )
        except Exception as e:
            raise ValueError(f"Error al parsear tiempos de llegada/salida: {e}")

        current_pos = (device_lat, device_lon)
        stop_pos = (stop_lat, stop_lon)

        try:
            distance_km = geodesic(current_pos, stop_pos).km
        except Exception as e:
            raise ValueError(f"Error al calcular la distancia geodésica: {e}")

        if speed_kmh > 0:
            eta_seconds = (distance_km / speed_kmh) * 3600
            estimated_arrival_time = now + timedelta(seconds=eta_seconds)
        else:
            estimated_arrival_time = now

        estimated_departure_time = estimated_arrival_time + timedelta(seconds=30)

        arrival_delay = int((estimated_arrival_time - scheduled_arrival).total_seconds())
        departure_delay = int((estimated_departure_time - scheduled_departure).total_seconds())

        uncertainty = min(max(int(distance_km * 60), 30), 300)

        return {
            "arrival_delay": arrival_delay,
            "departure_delay": departure_delay,
            "estimated_arrival_time": estimated_arrival_time.strftime("%H:%M:%S"),
            "estimated_departure_time": estimated_departure_time.strftime("%H:%M:%S"),
            "distance_to_stop_km": round(distance_km, 3),
            "uncertainty_seconds": uncertainty,
            "current_timestamp": now.strftime("%H:%M:%S"),
        }
