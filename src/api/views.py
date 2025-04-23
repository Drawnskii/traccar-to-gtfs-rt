from fastapi import FastAPI, Response
from google.protobuf.json_format import MessageToDict

from src.translators.trip_updates import TripUpdates
from src.translators.service_alerts import ServiceAlerts
from src.translators.vehicle_positions import VehiclePositions

app = FastAPI(title="Traccar to GTFS-RT")

@app.get('/')
async def root():
    return {"message": "Traccar to GTFS-RT is running!"}

@app.get("/gtfs-rt/vehicle-positions", response_class=Response)
async def get_vehicle_positions_pb():
    feed = VehiclePositions.make()

    if feed is None:
        return Response(content="No vehicle positions available!", media_type="text/plain", status_code=404)
    
    return Response(
        content=feed.SerializeToString(),
        media_type="application/x-protobuf",
        status_code=200
    )

@app.get("/vehicle-positions")
async def get_vehicle_positions_json():
    feed = VehiclePositions.make()

    if feed is None:
        return {"error": "No vehicle positions available!"}
    
    return MessageToDict(feed)

@app.get("/gtfs-rt/trip-updates", response_class=Response)
async def get_trip_updates_pb():
    feed = TripUpdates.make()

    if feed is None:
        return Response(content="No trip updates available!", media_type="text/plain", status_code=404)
    
    return Response(
        content=feed.SerializeToString(),
        media_type="application/x-protobuf",
        status_code=200
    )

@app.get("/trip-updates")
async def get_trip_updates_json():
    feed = TripUpdates.make()

    if feed is None:
        return {"error": "No trip updates available!"}
    
    return MessageToDict(feed)

@app.get("/gtfs-rt/service-alerts", response_class=Response)
async def get_service_alerts_pb():
    feed = ServiceAlerts.make()

    if feed is None:
        return Response(content="No service alerts available!", media_type="text/plain", status_code=404)
    
    return Response(
        content=feed.SerializeToString(),
        media_type="application/x-protobuf",
        status_code=200
    )

@app.get("/service-alerts")
async def get_service_alerts_json():
    feed = ServiceAlerts.make()

    if feed is None:
        return {"error": "No service alerts available!"}
    
    return MessageToDict(feed)