import logging

from src.context import context
from .trip_mapper import TripMapper
from src.factories.feed_message import FeedMessage
from src.factories.trip_update import TripUpdate

logger = logging.getLogger(__name__)

class TripUpdates:
    _known_ids = set()

    @staticmethod
    def make():
        updated = 0
        created = 0

        trip_updates = []

        for data in context.data.values():
            if "position" in data:
                entity_id = data["id"]
                
                position = data["position"]
                route_id = data["route_id"]
                device_time = position["deviceTime"]

                trip_data = TripMapper.map(route_id, device_time)

                if trip_data is None:
                    continue

                trip_id, stops = trip_data

                speed_kmh = position.get("speed", 25) * 3.6  # m/s a km/h

                params = {
                    "entity_id": entity_id,
                    "trip_id": trip_id,
                    "stops": stops,
                    "vehicle_id": data["name"],
                    "latitude": position["latitude"],
                    "longitude": position["longitude"],
                    "speed_kmh": speed_kmh,
                }

                if entity_id in TripUpdates._known_ids:
                    updated += 1
                else:
                    created += 1
                    TripUpdates._known_ids.add(entity_id)

                trip_update = TripUpdate.create(**params)
                trip_updates.append(trip_update)

        logger.info(f"TripUpdates feed created, Total: {len(trip_updates)}, New: {created}, Updated: {updated}")
        
        return FeedMessage.create(entities=trip_updates)
