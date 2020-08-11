from time import sleep
from json import dumps
from sklearn.datasets import make_blobs
from datetime import datetime
import csv
import time
import socket


def create_data(n_samples, n_features, centers, std):
    features, target = make_blobs(n_samples = n_samples,
                                  # two feature variables,
                                  n_features = n_features,
                                  # four clusters,
                                  centers = centers,
                                  # with .65 cluster standard deviation,
                                  cluster_std = std,
                                  # shuffled,
                                  shuffle = True)
    return features, target

# Create random centers
random_centers, _ = create_data(8,3,8,3)
with open('random_centers.csv','w') as f:
    fieldnames = ['x','y','z']
    writer = csv.DictWriter(f,fieldnames)
    writer.writeheader()
    for i in range(len(random_centers)):
        dct= {}
        for j in range(len(random_centers[i])):
            dct[fieldnames[j]]=random_centers[i][j]
        writer.writerow(dct)

# Create Centers for production
centers, cluster_num = create_data(8,3,8,3)
with open('centers.csv','w') as f:
    fieldnames = ['x','y','z']
    writer = csv.DictWriter(f,fieldnames)
    writer.writeheader()
    for i in range(len(centers)):
        dct= {}
        for j in range(len(centers[i])):
            dct[fieldnames[j]]=centers[i][j]
#         dct[fieldnames[len(centers[i])]] = cluster_num[i]
        writer.writerow(dct)


# with open('data.csv','w') as f:
#     fieldnames = ['x','y','z','target']
#     writer = csv.DictWriter(f,fieldnames)
#     writer.writeheader()
#     for i in range(len(features)):
#         dct= {}
#         for j in range(len(features[i])):
#             dct[fieldnames[j]]=features[i][j]
#         dct[fieldnames[len(features[i])]] = target[i]
#         writer.writerow(dct)


s = socket.socket() #Create a socket object
# host = '192.168.122.54'
host = 'localhost'
port = 9999 # Reserve a port for your service
s.bind((host,port)) #Bind to the port

s.listen() #Wait for the client connection
while True:
    c,addr = s.accept() #Establish a connection with the client
    print("Got connection from"+ str(addr))
    # features, target = create_data(100, 3, centers, 0.65)
    # for i in range(len(features)):
    #     message = str(datetime.now())+','
    #     # message+= ' '.join(features[i])
    #     for j in features[i]:
    #         message += ' '+ str(j)
    #     message.strip()
    #     print(f'Send: {message!r}')
    #     c.send(message.encode())

    t1 = time.time()
    while time.time()<t1+1:
        message = str(datetime.now())
        print(f'Send: {message!r}')
        c.send(message.encode())
    c.close()
