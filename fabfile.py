# Import Fabric's API module
from fabric.api import sudo
from fabric.operations import reboot
from fabric2 import Connection, Config
from invoke import Responder
from fabric2.transfer import Transfer
import os
from time import sleep

with open('./conf/master', 'r') as f:
    array = f.readline().split()
    master_host = array[0]
    master_port = array[1]
    user = array[2]
    host = array[3]

config = Config(overrides={'user': user})
conn = Connection(host=host, config=config)
config_master = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
master = Connection(host=master_host, config=config_master, gateway=conn)

slave_connections = []
config_slaves = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
with open('./conf/slaves', 'r') as f:
    array = f.readline().split()
    slave_connections.append(Connection(host=array[0], config=config_slaves, gateway=conn))

sudopass = Responder(pattern=r'\[sudo\] password:',
                     response='1\n',
                     )

def start_spark_cluster():
    master.run('source /etc/profile && $SPARK_HOME/sbin/start-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/start-slaves.sh')


def stop_spark_cluster():
    master.run('source /etc/profile && $SPARK_HOME/sbin/stop-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/stop-all.sh')

def restart_all_vms():
    for connection in slave_connections:
        try:
            connection.sudo('shutdown -r now')
        except:
            continue
    try:
        master.sudo('shutdown -r now')
    except:
        pass



def start_kafka():
    master.run('tmux new -d -s kafka')
    master.run('tmux new-window')
    master.run('tmux new-window')
    master.run('tmux send -t kafka:0 /home/ronald/kafka_2.12-2.5.0/bin/zookeeper-server-start.sh\ '
           '/home/ronald/kafka_2.12-2.5.0/config/zookeeper.properties ENTER')
    sleep(5)
    master.run('tmux send -t kafka:1 /home/ronald/kafka_2.12-2.5.0/bin/kafka-server-start.sh\ '
           '/home/ronald/kafka_2.12-2.5.0/config/server.properties ENTER')
    sleep(10)
    master.run('tmux send -t kafka:2 python3\ /home/ronald/kafka_producer_example.py ENTER')

def stop_kafka():
    master.run('tmux kill-session -t kafka')

def streaming_kmeans():
    # Create Package
    os.system('sbt package')
    # Transfer package
    transfer = Transfer(master)
    transfer.put('./target/scala-2.12/spark_example_2.12-0.1.jar')
    # Transfer datagenerator
    transfer.put('./kafka_producer_example.py')
    # start spark cluster
    start_spark_cluster()
    # start kafka
    start_kafka()
    master.run('rm -rf kmeansModel')
    master.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0 '
        '--class example.stream.StreamingKMeansModelExample '
        '--master spark://' + str(master_host) + ':7077 --executor-memory 2g '
        '~/spark_example_2.12-0.1.jar '
        'localhost:9092 '
        'consumer-group '
        'test'
    )
    run_checker()

def run_checker():
    # transfer checker
    transfer = Transfer(master)
    transfer.put('./checker.py')
    master.run(
        'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
        '~/checker.py'
    )

def stop():
    stop_kafka()
    stop_spark_cluster()
