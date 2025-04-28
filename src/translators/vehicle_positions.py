import logging

from src.context import context
from .trip_mapper import TripMapper
from src.factories.feed_message import FeedMessage
from src.factories.vehicle_position import VehiclePosition

logger = logging.getLogger(__name__)

class VehiclePositions:
    _known_ids = set()

    @staticmethod
    def make():
        updated = 0
        created = 0

        vehicle_positions = []
        for data in context.data.values():
            if "position" in data:
                entity_id = data["id"]

                position = data.get("position", {})

                route_id = data["route_id"]
                device_time = position["deviceTime"]

                trip_data = TripMapper.map(route_id, device_time)

                if trip_data is None:
                    continue

                trip_id, stops = trip_data

                params = {
                    "entity_id": entity_id,
                    "route_id": route_id,
                    "trip_id": trip_id,
                    "stop_id": stops[0]["stop_id"],
                    "vehicle_id": data["name"],
                    "bearing": position["course"],
                    "latitude": position["latitude"],
                    "longitude": position["longitude"],
                    "current_stop_sequence": stops[0]["stop_sequence"]
                }

                if entity_id in VehiclePositions._known_ids:
                    updated += 1
                else:
                    created += 1
                    VehiclePositions._known_ids.add(entity_id)

                vehicle_position = VehiclePosition.create(**params)
                vehicle_positions.append(vehicle_position)
        
        logger.info(f"VehiclePositions feed created, Total: {len(vehicle_positions)}, New: {created}, Updated: {updated}")

        return FeedMessage.create(entities=vehicle_positions)

