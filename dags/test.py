from asyncore import write
import csv
import pandas as pd

citizens_travel_data = pd.read_csv('../data/processed_travel_data.csv');
userAndTravelcount = {}
for i in range(len(citizens_travel_data['aadhar_no'])):
    if citizens_travel_data['aadhar_no'][i] not in  userAndTravelcount.keys():
         userAndTravelcount[citizens_travel_data['aadhar_no'][i]] = 1
    else:
        count =  userAndTravelcount.get(citizens_travel_data['aadhar_no'][i]) + 1
        userAndTravelcount[citizens_travel_data['aadhar_no'][i]] = count

topUserAndTravelCount = {}

for user, travelcount in userAndTravelcount.items():
    if travelcount > 1:
        topUserAndTravelCount[user] = travelcount
    
print(topUserAndTravelCount)

#New Task
# create a csv file called test.csv and
# store topUserAndTravelCount as outfile
with open("test.csv", "w") as outfile:
    # pass the csv file to csv.writer.
    writer = csv.writer(outfile)
    report_headers = ['aadhar_no', 'count']
    writer.writerow(report_headers)
	# convert the dictionary keys to a list
    for topUser, travelCount in topUserAndTravelCount.items():
        traveller_data = [topUser, travelCount]
        writer.writerow(traveller_data)

#send email
# Python code to illustrate Sending mail from
# your Gmail account
import smtplib

# creates SMTP session
s = smtplib.SMTP('smtp.office365.com', 587)
# start TLS for security
s.starttls()
# Authentication
s.login("pkrislive@outlook.com", "XXXXXXXXXXX")
# message to be sent
message = "Message_you_need_to_send"
# sending the mail
s.sendmail("pkrislive@outlook.com", "pradeep.rout@Impetus.com", message)
# terminating the session
s.quit()
