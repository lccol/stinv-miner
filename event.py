from datetime import datetime

class event:
    eventId = ''
    type = ''
    refinedType = ''
    startTime = '' #UTC 
    endTime = '' #UTC
    locationLat = 0
    locationLng = 0
    distance = 0 #mi
    airportCode = ''
    number = 0
    street = ''
    side = ''
    city = ''
    county = ''
    state = ''
    zipCode = ''
    childs = set()
    parents = set()
    
    def __init__(self, eventId, type, refinedType, startTime, endTime, locationLat, locationLng, distance, 
                 airportCode, number, street, side, city, county, state, zipCode, childs=set(), parents=set()):
        self. eventId = eventId
        self.type = type
        self.refinedType = refinedType
        self.startTime = datetime.strptime(startTime.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
        self.endTime = datetime.strptime(endTime.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
        self.locationLat = locationLat
        self.locationLng = locationLng
        self.distance = distance
        self.airportCode = airportCode
        self.number = number
        self.street = street
        self.side = side
        self.city = city
        self.county = county
        self.state = state
        self.zipCode= zipCode
        self.childs = childs
        self.parents = parents