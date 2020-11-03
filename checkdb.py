import psycopg2, sys
import random, string, re

list_thread = []
list_connSQL = []

with open('password.txt') as f:
	lines = [line.rstrip() for line in f]
username = lines[0]
pg_password = lines[1]

def thread1(threadIndex, passengerId, flightId):
	cursor = list_connSQL[threadIndex].cursor()
	print(f'Thread {threadIndex+1} starting\npassengerId={passengerId}\nflightId={flightId}')
	time.sleep(threadIndex)
	result = cursor.fetchone()
	print(f'Thread {threadIndex+1} result:{result[0]}')

def main(argv):
	conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
	cursor = conn.cursor()

	cmd = argv[0]
	filename = cmd.split(';')[0]
	isTransaction = cmd.split(';')[1]
	thread = cmd.split(';')[2]
	#print(f'input={filename.split("=")[1]}\ntransaction={isTransaction.split("=")[1]}\nthreads={thread.split("=")[1]}')

	file = open(filename.split('=')[1], 'r')
	Lines = file.readlines()
	sizeref = 6
	sizetick = 13
	countsuccess = 0
	countfailed = 0
	countunsuccess = 0
	successful = 'no'


	for i, line in enumerate(Lines):
		#get line value
		line = line.strip()
		if i < 1:
			continue
		passengerId = line.split(',')[0]
		flightId = line.split(',')[1]
		#print(f'passengerId={passengerId}\nflightId={flightId}')
	countbkref = 0

	#THREAD
	for i, name in enumerate(thread):
		list_thread.append(threading.Thread(target=thread1,args=(i,passengerId,flightId)))
		list_connSQL.append(psycopg2.connect(database="COSC3380", user=username, password=pg_password))
	for thread in list_thread:
		thread.start()
	#TRANSACTION
	for j in passengerId:
		for k in flightId:
			#cursor.execute("START TRANSACTION;")
			#generate a randomly unique book_ref
			bkref = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(sizeref))
			#generate a random ticket_no
			ticketno = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(sizetick))
			#print(bkref)
			#countbkref += 1
	#Lock thread, then check if flight_id is in the table
	#Check to see if that flight_id exists, = 0 -> does not exist
			cursor.execute("SELECT COUNT(*) FROM flights WHERE flight_id = {}".format(flightId))
			flightidexists = int(cursor.fetchall()[0][0])
			#print(flightidexists)
	#Unlock thread
	#Check to see if passenger_id is NULL or if flight_id does not exist then reservation failed
			if flightidexists != 0 or passengerId != 'NULL':
				#lock thread
				cursor.execute("SELECT COUNT(seats_available) FROM flights WHERE flight_id = {} AND seats_available > 0".format(flightId))
				availseats = int(cursor.fetchall()[0][0])
				#print(availseats)
				if availseats == 0:
					cursor.execute("INSERT INTO bookings(book_ref, book_date, total_amount) VALUES ('{}', now(), 12700)".format(bkref))
					successful = 'no'
					countunsuccess += 1
					print("Unsuccessful transactions: " , countunsuccess)
				else:
					cursor.execute("START TRANSACTION;"
								"INSERT INTO bookings(book_ref, book_date, total_amount) VALUES ('{}', now(), 12700);"
								"INSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name, email, phone) VALUES ('{}', '{}', '{}', 'ABC' , NULL, NULL);"
								"INSERT INTO ticket_flights(ticket_no, flight_id, fare_conditions, amount) VALUES ('{}', '{}', 'Economy', 12700);"
								"UPDATE flights SET seats_booked = seats_booked + 1, seats_available = seats_available - 1 WHERE flight_id = '{}';"
								"COMMIT;".format(bkref,ticketno,bkref,passengerId,ticketno,flightId,flightId))
					successful = 'yes'
					countsuccess += 1
					print("Successful transactions: ", countsuccess)
			else:
				countfailed += 1
				print("Failed transactions: ", countfailed)


	#WITHOUT TRANSACTION
	for j in passengerId:
		for k in flightId:
			cursor.execute("SELECT COUNT(*) FROM flights WHERE flight_id = {}".format(flightId))
			flightidexists = int(cursor.fetchall()[0][0])
			#Check to see if passenger_id is NULL or if flight_id does not exist then reservation failed
			if flightidexists != 0 or passengerId != 'NULL':
				cursor.execute("SELECT COUNT(seats_available) FROM flights WHERE flight_id = {} AND seats_available > 0".format(flightId))
				availseats = int(cursor.fetchall()[0][0])
				bkref = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(sizeref))
				ticketno = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(sizetick))
				if availseats == 0:
					cursor.execute("INSERT INTO bookings(book_ref, book_date, total_amount) VALUES ('{}', now(), 12700)".format(bkref))
					successful = 'no'
					countunsuccess += 1
					print("Unsuccessful transactions: " , countunsuccess)
				else:
					cursor.execute("INSERT INTO bookings(book_ref, book_date, total_amount) VALUES ('{}', now(), 12700)".format(bkref))
					cursor.execute("COMMIT;")
					cursor.execute("INSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name, email, phone) VALUES ('{}', '{}', {}, 'DEF', NULL, NULL)".format(ticketno,bkref,passengerId))
					cursor.execute("COMMIT;")
					cursor.execute("UPDATE flights SET seats_booked = seats_booked + 1, seats_available = seats_available - 1 WHERE flight_id = '{}';")
					cursor.execute("COMMIT;")
					cursor.execute("SELECT COUNT(*) FROM flights WHERE flights_id = {} AND seats_available > 0".format(flightId))
					seatsavail2 = int(cursor.fetchall()[0][0])
					if seatsavail2 == 0:
						successful = 'no'
						countunsuccess += 1
						print("Unsuccessful transactions: ", countunsuccess)
					else:
						cursor.execute("INSERT INTO ticket_flights SELECT ('{}', {}, 'Economy', 12700 FROM flights WHERE flights_id = {} AND seats_available > 0".format(ticketno,flightId,flightId))
						cursor.execute("COMMIT;")
						successful = 'yes'
						countsuccess += 1
						print("Successful transactions: ", countsuccess)
					#cursor.execute("INSERT INTO ticket_flights SELECT ('{}', {}, 'Economy', 12700 FROM flights WHERE flights_id = {} AND seats_available = 0".format(ticketno,flightId,flightId))
					
					### will return 0 1 -> true -> insert success
					
			

			#cursor.execute("SELECT * FROM bookings;")
			#cursor.execute("SELECT COUNT(*) FROM flights WHERE seats_available > 0 AND 
			#SELECT COUNT(*) FROM flights JOIN ticket_flights ON flight.flight_id = ticket_flights.flight_id
			#JOIN ticket ON ticket.ticket_no = ticket_flights.ticket_no
			#WHERE flights.seats_available > 0

			#while True:
			#	row = cursor.fetchone()
			#	if row == None:
			#		break
			#	print(row)
			#cursor.execute("COMMIT;")
	#print("Count book_ref: ", countbkref)

	#OUTPUT
	#Number of successful transactions
	print("# of successful transactions: ", countsuccess)
	#Number of unsuccessful transactions
	print("# of unsuccessful transactions: ", countfailed)
	#Number of records updated for table bookings
	print("# of records updated for bookings: ")
	#Number of records updated for table ticket
	print("# of records updated for ticket: ")
	#Number of records updated for table ticket_flights
	print("# of records updated for ticket_flights: ")
	#Number of records updated for table flights
	print("# of records updated for flights: ")
		
if __name__ == "__main__":
	main(sys.argv[1:])
