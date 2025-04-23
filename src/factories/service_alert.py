from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2 as gtfsrt

class ServiceAlert:
    @staticmethod
    def create(*args, **kwargs):
        entity_id = str(kwargs.get("entity_id"))
        trip_id = str(kwargs.get("trip_id"))
        route_id = str(kwargs.get("route_id"))

        # Parse event time string and get timestamp
        event_time_str = kwargs.get("event_time")
        start_time = int(datetime.fromisoformat(event_time_str.replace("Z", "+00:00")).timestamp())
        end_time = int(start_time + timedelta(hours=1).total_seconds())

        header_text = kwargs.get("header_text", "Service Disruption")
        description_text = kwargs.get("description_text", "Detailed description of the service disruption")
        language = kwargs.get("language", "en")

        cause = getattr(gtfsrt.Alert.Cause, kwargs.get("cause", "UNKNOWN_CAUSE"))
        effect = getattr(gtfsrt.Alert.Effect, kwargs.get("effect", "UNKNOWN_EFFECT"))

        # Alert - Time active
        active_period = gtfsrt.TimeRange(start=start_time, end=end_time)

        # Alert - Entities this alert informs
        informed_entity = gtfsrt.EntitySelector(
            trip=gtfsrt.TripDescriptor(trip_id=trip_id, route_id=route_id, schedule_relationship=gtfsrt.TripDescriptor.SCHEDULED)
        )

        # Alert - Header and description translations
        header_translation = gtfsrt.TranslatedString.Translation(
            text=header_text, language=language
        )
        description_translation = gtfsrt.TranslatedString.Translation(
            text=description_text, language=language
        )

        alert = gtfsrt.Alert(
            informed_entity=[informed_entity],
            active_period=[active_period],
            header_text=gtfsrt.TranslatedString(translation=[header_translation]),
            description_text=gtfsrt.TranslatedString(translation=[description_translation]),
            cause=cause,
            effect=effect
        )

        return gtfsrt.FeedEntity(
            id=entity_id,
            alert=alert
        )
