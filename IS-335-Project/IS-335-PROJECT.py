import datetime
from logging import exception, debug
import json
from flask import Flask, request, jsonify
import psycopg2
application = Flask(__name__)

def DbConnection():
    connenction = psycopg2.connect(host="localhost", dbname="IS335", user="postgres", password="0000", port="1024")
    curr = connenction.cursor()
    return curr

class Ride:

    def __init__(self,Riderid,pickup_location,drop_off_location,vehicletype):
        self.rideid = self.CreateRideID() #we should use a function to check for max if none then 1
        self.driverid = None, # should be updated when a driver accept a ride
        self.Riderid = Riderid, # parameter of the user who want to request a ride
        self.status = 'requested', # always start as requested
        self.pickup_location = pickup_location, #parameter for where the user wants the pick-up location
        self.drop_off_location = drop_off_location, #parameter for where the user wants to drop-off
        self.start_time = None, #should start when the driver accept
        self.end_time = None, # whenever the ride end
        self.RequestTime = datetime.date.today(),# the time we created the request
        self.vehicletype = vehicletype, # type of viechle user want
        self.distance_traveled = self.distanceToTravel(), #calculated as the ride go OR pre-defined (calculated from pick-up and drop-off locations if so should be updated to reflect the real distance traveled)
        self.route_taken = None, #calculated as the ride go
        self.ride_duration = self.AprroximateTime(), #calculated as the ride go
        self.totalprice = self.getprice() # calculated based on many criteria (must be updated after the ride to reflect the real price)
        self.surge_area_surgeid = self.CheckSurge()[0] #check if the drop-off is inside a surge area

        self.CreateRide()
        self.NotifyNearbyDrivers(self.rideid)


    def CreateRideID(self):
        curr = DbConnection()


        curr.execute("SELECT nextval('ride_id');")
        RideId = curr.fetchone()[0]
        curr.close()
        return RideId

    def CreateRide(self):
        curr= DbConnection()

        curr.execute("""
                            INSERT INTO ride (
                                rideid, driverid, riderid, status, pickup_location, drop_off_location, 
                                start_time, end_time, requesttime, vehicletype, distance_traveled, 
                                route_taken, ride_duration, totalprice, surge_area_surgeid
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            );
                            """, (
                    self.rideid,
                    self.driverid,
                    self.Riderid,
                    self.status,
                    self.pickup_location,
                    self.drop_off_location,
                    self.start_time,
                    self.end_time,
                    self.RequestTime,
                    self.vehicletype,
                    self.distance_traveled,
                    self.route_taken,
                    self.ride_duration,
                    self.totalprice,
                    self.surge_area_surgeid
                )
                             )
        curr.connection.commit()




    def CheckSurge(self):
       curr = DbConnection()
       wkb_location = bytes.fromhex(self.pickup_location[0])
       curr.execute("""SELECT surgeid, rate, name FROM surge_area WHERE ST_Within(ST_GeomFromWKB(%s), location);
          """, (wkb_location,))

       PickUpIsSurge = curr.fetchone()
       if PickUpIsSurge:
           return PickUpIsSurge[0],PickUpIsSurge[1]
       else:
           return None,1


    def getprice(self):
        vRate=1
        if float(self.distance_traveled[0]) <= 0:
            return 0

        if self.vehicletype == "economy":
            vRate = 1
        elif self.vehicletype == "premium":
            vRate = 1.10
        elif self.vehicletype == "luxury": #should be family but we forgot
            vRate = 1.25

        sRate= float(self.CheckSurge()[1])
        AprroximateTime= self.AprroximateTime()
        total= (float(AprroximateTime*2.5)) *  sRate  *  vRate
        return total


    def AprroximateTime(self):
        return ((self.distance_traveled[0])/40)*60



    def distanceToTravel(self):
        curr = DbConnection()
        pickup_wkb = bytes.fromhex(self.pickup_location[0])
        dropoff_wkb = bytes.fromhex(self.drop_off_location[0])

        # Calculate distance using same transformation as driver matching
        curr.execute("""
                        SELECT ST_Distance(
                            ST_Transform(ST_GeomFromWKB(%s, 4326), 3857),
                            ST_Transform(ST_GeomFromWKB(%s, 4326), 3857)
                        ) / 1000 AS distance_km
                    """, (pickup_wkb, dropoff_wkb))

        result = curr.fetchone()
        curr.close()
        return float(result[0]) if result else 0.0


    def NotifyNearbyDrivers(self,rideid):
        curr = DbConnection()
        curr.execute("SELECT pickup_location FROM ride WHERE rideid=%s ",(rideid,))
        x = curr.fetchone()
        wkb_location = bytes.fromhex(x[0])
        curr.execute( """WITH NearestDrivers AS (
                SELECT d.DriverID, 
                       ST_Distance(
                           ST_Transform(d.location, 3857),
                           ST_Transform(ST_GeomFromWKB(%s, 4326), 3857)
                       ) / 1000 AS distance
                FROM Driver d
                WHERE d.status = 'available'  AND d.vehicle_type =%s

                ORDER BY distance
                LIMIT 5
            )
            SELECT DriverID, distance FROM NearestDrivers;
        """, (wkb_location,self.vehicletype[0],))

        nearby_drivers = curr.fetchall()
        for driver in nearby_drivers:
            driver_id, distance = driver
            print(f"Driver ID: {driver_id}, Distance: {distance} km")
            curr.execute("""INSERT INTO nearby_drivers (ride_id, driver_id, distance, status) VALUES (%s, %s, %s, %s)""",
                         (self.rideid, driver_id, distance, 'pending'))
        curr.connection.commit()
        return nearby_drivers




class Driver:
    def __init__(self, DriverId):     ##i think we need id only (removed name license status location)):
        self.DriverId=DriverId


    def AcceptDrive(self, ride_id):
        curr = DbConnection()

        try:

            # Lock the ride row for the entire transaction
            curr.execute('''
                SELECT * FROM ride 
                WHERE rideid = %s AND status = 'requested'
                FOR UPDATE NOWAIT  
            ''', (ride_id,))
            ride = curr.fetchone()

            if ride:
                # Assign ride to driver
                curr.execute('''
                    UPDATE ride 
                    SET driverid = %s, status = 'in-progress' 
                    WHERE rideid = %s
                ''', (self.DriverId, ride_id))

                # Update driver status to busy
                curr.execute('''
                    UPDATE driver 
                    SET status = 'busy' 
                    WHERE driverid = %s
                ''', (self.DriverId,))

                # Delete ride from nearby drivers
                curr.execute('DELETE FROM nearby_drivers WHERE ride_id = %s', (ride_id,))

                curr.connection.commit()
                return "Ride accepted"
            else:
                curr.connection.rollback()
                return "Ride not available"
        except Exception as e:
            curr.connection.rollback()
            return f"Error: {e}"
        finally:
            curr.close()


    def CheckRideRequests(self):
        curr = DbConnection()
        curr.execute("""
            SELECT ride_id, distance, pickup_location, drop_off_location 
            FROM nearby_drivers 
            LEFT JOIN ride ON nearby_drivers.ride_id = ride.rideid 
            WHERE driver_id = %s;
        """, (self.DriverId,))

        ride_requests = curr.fetchall()
        print(ride_requests)
        return ride_requests

@application.route('/requist_rides', methods=['POST'])
def request_ride_json():
    RequistBody=request.json
    Rider_id=RequistBody.get('rider_id')
    Pickup_Location=RequistBody.get('pickup_location')
    DropOff_Location=RequistBody.get('dropoff_location')
    Vehicle_Type=RequistBody.get('vehicle_type')
    if not  Rider_id :
        return jsonify({"error": "Missing required fields: rider_id "}, 400)
    if not Pickup_Location:
        return jsonify({"error": "Missing required fields: pickup_location "}, 400)
    if not DropOff_Location:
        return jsonify({"error": "Missing required fields: dropOff_location "}, 400)
    if not Vehicle_Type:
        return jsonify({"error": "Missing required fields: vehicle_type "}, 400)


    ride= Ride(Rider_id,Pickup_Location,DropOff_Location,Vehicle_Type)
    return jsonify({"ride_id":ride.rideid
                        , "status":ride.status
                         ,'estimated_price':ride.totalprice}),201

@application.route('/accept_rides', methods=['PUT'])
def accept_ride_json():
    RequistBody=request.json
    Ride_Id=RequistBody.get('ride_id')
    driver_id=RequistBody.get('driver_id')
    if not Ride_Id:
        return jsonify({"error": "Missing required fields: ride_id "}, 400)

    if not driver_id:
        return jsonify({"error": "Missing required fields: driver_id "}, 400)

    driver=Driver(driver_id)
    Response=driver.AcceptDrive(Ride_Id)
    if "Ride accepted" in Response:
        return jsonify({"ride_id":Ride_Id,
                        "status":"in-progress",
                         "driver_id":driver_id}),200
    else:
        return jsonify({"Error": Response}), 400
if __name__=='__main__':
   application.run(debug(True))
