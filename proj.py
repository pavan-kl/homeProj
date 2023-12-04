import time
import datetime
import threading
import random
import json
from kafka import KafkaProducer, KafkaConsumer, Consumer, KafkaError
from concurrent.futures import ThreadPoolExecutor

###### >>>>>>>>>>>> Configuration Begins #####
## Define all configurations in this section. 
## the logic is made extensible such that you can add new vehicles in the future 

# Kafka configurations
bootstrap_servers = 'localhost:9092'
incomingTrafficTopic = 'incomingTraffic'
exitTrafficTopic = 'exitingTraffic'
num_partitions = 3  # 3 partitions for incoming traffic

# configure time window in mins
timeMinsWindow = 5

# min limits to be specified. we can add new vehicle here in the future, eg: minivan:20
# the program will derive other parameters based on this declaration
vehicleLimits = { 'bike': 15, 'car': 60, 'truck': 25 }

# exit time limits of each of the vehicle in minutes
exitTimes = { 'bike': [30, 59], 'car': [20, 39], 'truck' : [30, 59]}

#### Configuration Ends ###### <<<<<<<<<<<<<<<<<<<<<<<<

currWindowCount = 0
totalIncomingVehicleCount = 0
# Queues for each type of vehicle
vehicleQueue = {}
# this keeps track of the incoming vehicle type count eg: bike, car, truck, van etc
incomingVehicleCount = {}
# this keeps track if the vehicle type is allowed to be processed in the time window
processVehicles = {}
# this keeps track of the max vehicles(capacity) to be processed for a vehicle type 
# in the time window. this is the max of (minLimits, percentage of Vehicles)
processMaxVehicles = {}
# derived from the vehicleLimits, see below in the main code
vehicleTypes = []
currLimits = {}
vehicleNo = 0

def produceMessages(topic):
    # Create Kafka producer 
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    # buffer some intial data to the topic, 
    for i in range(100):        
        produceVehicleMessage(topic)
    
    try:
        ## Run Forever
        while True:
            produceVehicleMessage(topic)
            # create random incoming distribution based on sleep time
            time.sleep(random.uniform(1, 10))
    finally:
        producer.close()

def produceVehicleMessage(producer, topic):
    # create random distribution of incoming vehicle types
    vehicle_type = random.choice(vehicleTypes)
    # incr total incoming vehicles
    totalIncomingVehicleCount += 1
    # incr total incoming of a vehicle type, we will reset it on use later
    incomingVehicleCount[vehicle_type] += 1
    vehicle_data = {
        'type': vehicle_type,
        'number': f'KA05-{++vehicleNo}',
        'ingressTime': int(time.time())
    }
    # Produce message to the specified partition based on the vehicle type
    producer.produce(
        topic=topic,
        key=vehicle_data['number'],  
        value=f'"type": {vehicle_data["type"]}, "number": {vehicle_data["number"]}, 
                "ingressTime":{vehicle["ingressTime"]}',
        partition=getPartitionForVehicleType(vehicle_data['type']))
    

def getPartitionForVehicleType(vehicle_type):
    partition = -1
    for vehicle in vehicleTypes:
        if vehicle_type == vehicle :
            return ++partition
    raise Exception('unknown vehicle specified: {vehicle_type}')    
 

def shouldProcessPartition(type_count, msg):
    
    # initially & on every window reset, the current Limits is empty
    if not currLimits:
        currWindowCount = totalIncomingVehicleCount 
        totalIncomingVehicleCount = 0

        atleastOneVehicleTypeFound = False        

        # enable processing flag of vehicle type if it meets the min count criteria
        # & set the processing capacity for each of the vehicle type based on the max
        # value of the percentage and the min limit for that vehicle type
        for vehicle in vehicleTypes:
            if (incomingVehicleCount[vehicle] >= vehicleLimits[vehicle]) :
                atleastOneVehicleTypeFound = True
                processVehicles[vehicle] = True
                processMaxVehicles[vehicle] = max(vehicleLimits[vehicle]/100 * currWindowCount, vehicleLimits[vehicle])
                incomingVehicleCount[vehicle] = 0

        # we were unable to find a single vehicle type which did not meet the
        # min Limits criteria, so defer processing
        if not atleastOneVehicleTypeFound : 
            return False 
        
        for vehicle in (vehicleTypes):
            currLimits[vehicle] = 0
        
        # we have atleast one or more vehicle types which can be processed
        # Get the current time
        currentTime = datetime.now()

        # set time minutes window
        futureTime = currentTime + datetime.timedelta(minutes=timeMinsWindow)
        

    # rate limit based on the vehicle type
    if currentTime <= futureTime:
        # check if processing is enabled for this vehicle type
        # and that the current limit does not exceed the max limit for that vehicle type
        # in the time window
        if (processVehicles[vehicle] == True and 
            currLimits[msg['type']] < processMaxVehicles[msg['type']]) :
            currLimits[msg['type']] = currLimits[msg['type']] + 1
            return True # process 
        else :
            return False # throttle 
        
    else : # current time is greater than time window
        currLimits.clear
        processVehicles.clear
        processMaxVehicles.clear
        
    return False # defer processing

# keeps printing the stats of the vehicles which have crossed the gate of toll plaza
# every minute
def reporter():

    # run forever
    while True:
        print(f"{datetime.now()},")
        for vehicle in vehicleTypes:
            if vehicle in vehicleQueue:
                percent = (vehicleQueue[vehicle].size()/currWindowCount)*100             
                print(f"[{vehicle}, {vehicleQueue[vehicle].size()}, {percent}%]")
            else:
                print(f"[{vehicle}, 0, 0]")
        time.sleep(60)

# ingresses the vehicle into the toll plaza
def ingressVehicle(vehicle):
    exitTime = exitTimes[vehicle['type']]
    #randomize the egress times for the vehicles, by choosing between the given times
    vehicle['egressTime'] = (datetime.now() + 
                            datetime.timedelta(minutes=random(exitTime[0], exitTime[1])))
    vehicleQueue[vehicle['type']].append(vehicle)   

# consume Messages based on the rate limit set
def consumeMessages(consumer):
    consumer = KafkaConsumer(incomingTrafficTopic, bootstrap_servers=bootstrap_servers, group_id='toll_plaza')
    
    try:
        while True:
            msg = consumer.poll(1.0) 

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Get the partition of the message
            partition = msg.partition()

            # should the partition be processed for the vehicle type
            if not shouldProcessPartition(partition, msg):
                print(f"Deferring consumption for partition {partition}")
                time.sleep(1)
                continue

            # Process the message 
            vehicle_data = json.loads(msg.value().decode('utf-8'))
            # ingress vehicle to the toll plaza stretch
            ingressVehicle(vehicle_data)            

    finally:
        consumer.close()

# egress vehicle from the toll plaza and add it to the exit traffic
def egressVehicle(producer, vehicle):
    producer.produce(
        topic=exitTrafficTopic,
        key=vehicle['number'],  # vehicle number as the key 
        value=f'"type": {vehicle["type"]}, "number": {vehicle["number"]},
                "ingressTime":{vehicle["ingressTime"]}, "egressTime":{vehicle["egressTime"]}')
    
# egresses vehicle from exitTrafficTopic, i.e after coming out of the toll plaza
# runs forever to exit the vehicles
def egressConsumer():
    consumer = KafkaConsumer (exitTrafficTopic, group_id ='group1',bootstrap_servers =
                                bootstrap_servers)

    try: 
        while True:
            for msg in consumer:
                vehicle = json.loads(msg.value().decode('utf-8'))
                print(f"vehicle with number {vehicle['number']} entered at time {vehicle['ingressTime']} 
                                & exited at time {vehicle['egressTime']}")
            time.sleep(1)

    finally:
        consumer.close()

# this exits traffic from the toll plaza stretch and onto the exitTrafficTopic
# we already have vehicles which are on the toll plaza stretch in the vehicleQueue 
def exitTraffic() :
    
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    try: 
        while True:
            # egress all vehicles who have crossed their egress times 
            for vehicleType in vehicleTypes :
                if vehicleType in vehicleQueue:
                    vehicleList = vehicleQueue[vehicleType]
                    for vehicle in vehicleList :
                        if (datetime.now() > vehicle['egressTime']) :
                            vehicleList.remove(vehicle)
                            egressVehicle(producer, vehicle)
            time.sleep(2)
    finally:
        producer.close()

if __name__ == "__main__":

    # derive other constructs based on the vehicle limits specified
    for vehicle in vehicleLimits:
        vehicleTypes.append(vehicle)
        incomingVehicleCount[vehicle]=0
        vehicleQueue[vehicle] = []       

    # produce vehicle traffic on the toll plaza stretch
    producerThread = threading.Thread(target=produceMessages)
    producerThread.start()    

    # ingress vehicle traffic on the toll plaza stretch
    consumerThread = threading.Thread(target=consumeMessages)
    consumerThread.start()

    # log reporter for the toll plaza stretch for every min
    reporterThread = threading.Thread(target=reporter)
    reporterThread.start()

    # egress vehicles from the toll plaza stretch based on randomized exit times specified
    # and move them on to the exit traffic topic
    exitThread = threading.Thread(target=exitTraffic)
    exitThread.start()

    # exit traffic altogether & report the ingress/egress time of each vehicle
    egressConsumerThread = threading.Thread(target=egressConsumer)
    egressConsumerThread.start()

    producerThread.join()
    consumerThread.join()
    reporterThread.join()
    exitThread.join()
    egressConsumerThread.join()
