from datetime import datetime
from google.transit import gtfs_realtime_pb2
from google.transit import gtfs_realtime_pb2 as gtfsrt

class VehiclePosition:
    @staticmethod
    def create(*args, **kwargs):
        entity_id = str(kwargs.get("entity_id"))

        # TripDescriptor: identifies the trip associated with this vehicle
        route_id = str(kwargs.get("route_id"))
        trip_id = str(kwargs.get("trip_id"))
        bearing = kwargs.get("bearing")

        trip_descriptor = gtfsrt.TripDescriptor(
            route_id=route_id,
            trip_id=trip_id,
            schedule_relationship=gtfsrt.TripDescriptor.SCHEDULED
        )

        # VehicleDescriptor: uniquely identifies the vehicle
        vehicle_id = kwargs.get("vehicle_id")
        vehicle_descriptor = gtfsrt.VehicleDescriptor(id=vehicle_id)

        # Position: represents the current position and bearing of the vehicle
        latitude = kwargs.get("latitude")
        longitude = kwargs.get("longitude")

        position = gtfsrt.Position(
            longitude=longitude,
            latitude=latitude, 
            bearing=bearing
        )

        # VehiclePosition: encapsulates the real-time vehicle data
        vehicle_position = gtfsrt.VehiclePosition(
            position=position,
            trip=trip_descriptor,
            vehicle=vehicle_descriptor,
             timestamp=int(datetime.now().timestamp())
        )

        return gtfsrt.FeedEntity(
            id=entity_id,
            vehicle=vehicle_position
        )
